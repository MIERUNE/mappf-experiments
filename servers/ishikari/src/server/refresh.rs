//! Cluster-internal advisory refresh receiver.

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use mmpf_cluster::{HintAdmission, StyleRefreshHint};

use super::{AppState, style};

pub(crate) async fn style_refresh_handler(
    State(state): State<AppState>,
    Json(hint): Json<StyleRefreshHint>,
) -> Response {
    if hint.validate().is_err() {
        state.metrics.record_style_refresh_hint("rejected");
        return (StatusCode::BAD_REQUEST, "invalid refresh hint\n").into_response();
    }
    // A `hint_id` names one publisher mutation, so a repeat is a retry. Answer as
    // the first delivery did without invalidating again: each invalidation
    // discards concurrent in-flight provider work, so an unbounded retry sequence
    // could otherwise keep a hot style permanently re-fetching.
    if state.membership.admit_style_refresh(&hint) == HintAdmission::Duplicate {
        // Counted, not merely logged: the response is an indistinguishable `202`,
        // so without this the retry amplification this guard exists to bound
        // would be invisible in production.
        state.metrics.record_style_refresh_hint("suppressed");
        tracing::debug!(
            hint_id = %hint.hint_id,
            style_id = %hint.style_id,
            "ignored duplicate advisory style refresh"
        );
        return (StatusCode::ACCEPTED, "refresh accepted\n").into_response();
    }
    if let Err((status, message)) = style::request_style_revalidation(&state, &hint.style_id) {
        state.metrics.record_style_refresh_hint("unknown_style");
        return (status, format!("{message}\n")).into_response();
    }
    if let Err(error) = state.membership.publish_style_refresh(&hint).await {
        tracing::warn!(%error, "failed to publish style refresh hint");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "refresh publish failed\n",
        )
            .into_response();
    }
    state.metrics.record_style_refresh_hint("accepted");
    tracing::info!(
        hint_id = %hint.hint_id,
        style_id = %hint.style_id,
        "accepted advisory style refresh"
    );
    (StatusCode::ACCEPTED, "refresh accepted\n").into_response()
}

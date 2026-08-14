//! Management-listener assembly.

use std::{error::Error as _, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context as _;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Request, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use http_body_util::LengthLimitError;
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use abashiri_core::{
    auth::{ManagementAction, ManagementAuthFailure, ObjectStoreManagementAuth},
    catalog::StyleCatalog,
    mutation::{
        AccountId, Actor, Execution, IdempotencyConflict, LocalResourceId, VersionEvidence,
        mutation_key_sha256,
    },
    style::{
        MAX_STYLE_BYTES, PublishStyleRequest, StyleObjectPath, StylePrecondition,
        StylePublishConflict, StylePublisher,
    },
};
use mmpf_cluster::StyleRefreshHint;
use mmpf_http::request_id::{self, RequestId};
use mmpf_http::serve::wait_for_shutdown_signal;

use crate::notifier::StyleRefreshNotifier;
use crate::operations::OperationalStatusClient;

const RECONCILIATION_INTERVAL: Duration = Duration::from_mins(5);

#[derive(Clone)]
struct AppState {
    auth: Option<ObjectStoreManagementAuth>,
    publishing: Option<StylePublishing>,
    operations: Option<OperationalStatusClient>,
}

#[derive(Clone)]
pub(crate) struct StylePublishing {
    catalog: Arc<StyleCatalog>,
    publisher: Arc<StylePublisher>,
    notifier: Option<StyleRefreshNotifier>,
}

impl StylePublishing {
    pub(crate) fn new(
        catalog: StyleCatalog,
        publisher: StylePublisher,
        notifier: Option<StyleRefreshNotifier>,
    ) -> anyhow::Result<Self> {
        if notifier.is_some() {
            for location in catalog.locations() {
                StyleRefreshHint::new(
                    "catalog-validation",
                    location.delivery_style_id().to_owned(),
                )
                .with_context(|| {
                    format!(
                        "catalog style path {} cannot form a refresh hint",
                        location.as_ref()
                    )
                })?;
            }
        }
        Ok(Self {
            catalog: Arc::new(catalog),
            publisher: Arc::new(publisher),
            notifier,
        })
    }
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    code: &'static str,
    message: &'static str,
    request_id: &'a str,
}

#[derive(Serialize)]
struct StylePublishResponse<'a> {
    account_id: &'a str,
    style_id: &'a str,
    outcome: &'static str,
    refresh: &'static str,
}

#[derive(Serialize)]
struct WhoAmI {
    actor: abashiri_core::mutation::Actor,
    accounts: Vec<String>,
    actions: Vec<&'static str>,
    registry_revision: u64,
}

pub(crate) async fn serve(
    http_addr: SocketAddr,
    auth: Option<ObjectStoreManagementAuth>,
    publishing: Option<StylePublishing>,
    operations: Option<OperationalStatusClient>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(http_addr)
        .await
        .with_context(|| format!("bind management listener to {http_addr}"))?;
    let local_addr = listener
        .local_addr()
        .context("read management listener address")?;
    tracing::info!(%local_addr, "Abashiri management listener started");

    let reconciler = publishing.clone().map(|publishing| {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILIATION_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                match publishing
                    .publisher
                    .reconcile_unfinished(&publishing.catalog)
                    .await
                {
                    Ok(report) if report.unfinished_intents() > 0 => {
                        info!(?report, "reconciled unfinished mutation intents");
                    }
                    Ok(report) => {
                        debug!(?report, "mutation reconciliation scan completed");
                    }
                    Err(error) => {
                        warn!(error = %format_args!("{error:#}"), "mutation reconciliation scan failed");
                    }
                }
            }
        })
    });
    let result = axum::serve(
        listener,
        router_with_operations(auth, publishing, operations),
    )
    .with_graceful_shutdown(wait_for_shutdown_signal())
    .await
    .context("serve management listener");
    if let Some(reconciler) = reconciler {
        reconciler.abort();
        let _ = reconciler.await;
    }
    result
}

#[cfg(test)]
fn router(auth: Option<ObjectStoreManagementAuth>, publishing: Option<StylePublishing>) -> Router {
    router_with_operations(auth, publishing, None)
}

fn router_with_operations(
    auth: Option<ObjectStoreManagementAuth>,
    publishing: Option<StylePublishing>,
    operations: Option<OperationalStatusClient>,
) -> Router {
    let publication_enabled = publishing.is_some();
    let operations_enabled = operations.is_some();
    let mut router = Router::new()
        .route("/livez", get(health))
        .route("/readyz", get(health))
        .route("/whoami", get(whoami))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found);
    if publication_enabled {
        router = router.route(
            "/accounts/{account_id}/styles/{style_id}",
            get(get_style).put(publish_style),
        );
    }
    if operations_enabled {
        router = router.route("/operations/status", get(operations_status));
    }
    router.with_state(AppState {
        auth,
        publishing,
        operations,
    })
}

async fn health() -> Response {
    no_store((StatusCode::OK, Json(Health { status: "ok" })).into_response())
}

async fn not_found(headers: HeaderMap) -> Response {
    let request_id = inbound_request_id(&headers);
    mutation_error(
        StatusCode::NOT_FOUND,
        "not_found",
        "Resource not found",
        &request_id,
    )
}

async fn method_not_allowed(headers: HeaderMap) -> Response {
    let request_id = inbound_request_id(&headers);
    mutation_error(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "Method not allowed",
        &request_id,
    )
}

async fn whoami(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = inbound_request_id(&headers);
    let Some(auth) = state.auth else {
        return mutation_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Resource not found",
            &request_id,
        );
    };
    match auth.authenticate(&headers).await {
        Ok(principal) => with_request_id(
            no_store(
                (
                    StatusCode::OK,
                    Json(WhoAmI {
                        actor: principal.actor().clone(),
                        accounts: principal
                            .accounts()
                            .iter()
                            .map(|account| account.as_str().to_string())
                            .collect(),
                        actions: principal
                            .actions()
                            .iter()
                            .copied()
                            .map(abashiri_core::auth::ManagementAction::as_str)
                            .collect(),
                        registry_revision: principal.registry_revision(),
                    }),
                )
                    .into_response(),
            ),
            &request_id,
        ),
        Err(failure) => auth_failure_response(failure, &request_id),
    }
}

async fn operations_status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let request_id = inbound_request_id(&headers);
    let Some(auth) = state.auth else {
        return mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_unavailable",
            "Management authentication is unavailable",
            &request_id,
        );
    };
    let principal = match auth.authenticate(&headers).await {
        Ok(principal) => principal,
        Err(failure) => return auth_failure_response(failure, &request_id),
    };
    if let Err(failure) = principal.authorize_action(ManagementAction::OperationsRead) {
        return auth_failure_response(failure, &request_id);
    }
    let Some(operations) = state.operations else {
        return mutation_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Resource not found",
            &request_id,
        );
    };
    with_request_id(
        no_store((StatusCode::OK, Json(operations.overview().await)).into_response()),
        &request_id,
    )
}

struct AuthorizedStyle {
    publishing: StylePublishing,
    actor: Actor,
    account: AccountId,
    style: LocalResourceId,
    location: StyleObjectPath,
}

async fn authorize_style(
    state: &AppState,
    account: String,
    style: String,
    headers: &HeaderMap,
    action: ManagementAction,
    request_id: &RequestId,
) -> Result<AuthorizedStyle, Response> {
    let auth = state.auth.as_ref().ok_or_else(|| {
        mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_unavailable",
            "Management authentication is unavailable",
            request_id,
        )
    })?;
    let publishing = state.publishing.clone().ok_or_else(|| {
        mutation_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "Resource not found",
            request_id,
        )
    })?;
    let principal = auth
        .authenticate(headers)
        .await
        .map_err(|failure| auth_failure_response(failure, request_id))?;
    let account = AccountId::try_new(account).map_err(|_| {
        mutation_error(
            StatusCode::BAD_REQUEST,
            "invalid_account_id",
            "Invalid account ID",
            request_id,
        )
    })?;
    let style = LocalResourceId::try_new(style).map_err(|_| {
        mutation_error(
            StatusCode::BAD_REQUEST,
            "invalid_style_id",
            "Invalid style ID",
            request_id,
        )
    })?;
    principal.authorize(&account, action).map_err(|_| {
        mutation_error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Management credential is not authorized",
            request_id,
        )
    })?;
    let location = publishing
        .catalog
        .resolve(&account, &style)
        .cloned()
        .ok_or_else(|| {
            mutation_error(
                StatusCode::NOT_FOUND,
                "style_not_found",
                "Style is not present in the management catalog",
                request_id,
            )
        })?;
    Ok(AuthorizedStyle {
        publishing,
        actor: principal.actor().clone(),
        account,
        style,
        location,
    })
}

async fn get_style(
    State(state): State<AppState>,
    Path((account, style)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let request_id = inbound_request_id(&headers);
    let authorized = match authorize_style(
        &state,
        account,
        style,
        &headers,
        ManagementAction::StyleRead,
        &request_id,
    )
    .await
    {
        Ok(authorized) => authorized,
        Err(response) => return response,
    };
    let document = match authorized
        .publishing
        .publisher
        .get(&authorized.location)
        .await
    {
        Ok(Some(document)) => document,
        Ok(None) => {
            return mutation_error(
                StatusCode::NOT_FOUND,
                "style_not_published",
                "Style has not been published",
                &request_id,
            );
        }
        Err(error) => return publication_error(error, &request_id),
    };
    let entity_tag = match version_entity_tag(document.version(), "published", &request_id) {
        Ok(entity_tag) => entity_tag,
        Err(response) => return response,
    };
    let mut response = no_store(document.body().clone().into_response());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&entity_tag).expect("encoded version ETag is a valid header"),
    );
    with_request_id(response, &request_id)
}

async fn publish_style(
    State(state): State<AppState>,
    Path((account, style)): Path<(String, String)>,
    request: Request<Body>,
) -> Response {
    let request_id = inbound_request_id(request.headers());
    let authorized = match authorize_style(
        &state,
        account,
        style,
        request.headers(),
        ManagementAction::StylePublish,
        &request_id,
    )
    .await
    {
        Ok(authorized) => authorized,
        Err(response) => return response,
    };
    if request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_none_or(|value| !value.trim().eq_ignore_ascii_case("application/json"))
    {
        return mutation_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "Style publication requires application/json",
            &request_id,
        );
    }
    let Some(idempotency_key) =
        single_header(request.headers(), "idempotency-key").map(str::to_owned)
    else {
        return mutation_error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "Exactly one Idempotency-Key header is required",
            &request_id,
        );
    };
    if RequestId::try_new(idempotency_key.clone()).is_err() {
        return mutation_error(
            StatusCode::BAD_REQUEST,
            "invalid_idempotency_key",
            "Idempotency-Key must be a bounded HTTP token",
            &request_id,
        );
    }
    let style_hint = authorized.publishing.notifier.as_ref().map(|_| {
        let hint_id = mutation_key_sha256(&idempotency_key)
            .expect("validated idempotency key has a stable mutation digest");
        StyleRefreshHint::new(hint_id, authorized.location.delivery_style_id().to_owned())
            .expect("notifier-enabled catalogs are validated at startup")
    });
    let precondition = match publication_precondition(request.headers()) {
        Ok(precondition) => precondition,
        Err((status, code, message)) => {
            return mutation_error(status, code, message, &request_id);
        }
    };
    let is_create = matches!(precondition, StylePrecondition::MustNotExist);
    let body = match to_bytes(request.into_body(), MAX_STYLE_BYTES).await {
        Ok(body) => body,
        Err(error)
            if error
                .source()
                .is_some_and(<dyn std::error::Error + 'static>::is::<LengthLimitError>) =>
        {
            return mutation_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "style_too_large",
                "Style document is too large",
                &request_id,
            );
        }
        Err(_) => {
            return mutation_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_body",
                "Could not read the style document",
                &request_id,
            );
        }
    };
    let publication = match PublishStyleRequest::try_new(
        idempotency_key,
        authorized.actor,
        authorized.account.clone(),
        authorized.style.clone(),
        authorized.location,
        precondition,
        body,
        request_id.clone(),
    ) {
        Ok(publication) => publication,
        Err(_) => {
            return mutation_error(
                StatusCode::BAD_REQUEST,
                "invalid_style",
                "Invalid MapLibre style document",
                &request_id,
            );
        }
    };
    let (version, outcome, status) =
        match authorized.publishing.publisher.publish(publication).await {
            Ok(Execution::Committed(published)) => (
                published.version().clone(),
                "committed",
                if is_create {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
            ),
            Ok(Execution::AlreadyCompleted(completed)) => (
                completed.state_version().clone(),
                "already_completed",
                StatusCode::OK,
            ),
            Err(error) => return publication_error(error, &request_id),
        };
    let entity_tag = match version_entity_tag(&version, "committed", &request_id) {
        Ok(entity_tag) => entity_tag,
        Err(response) => return response,
    };
    let refresh = match &authorized.publishing.notifier {
        Some(notifier) => {
            let style_hint = style_hint
                .as_ref()
                .expect("a notifier-enabled request constructs a refresh hint");
            let delivery = notifier.notify(style_hint).await;
            if delivery.complete() {
                "delivered"
            } else {
                warn!(
                    delivered = delivery.delivered,
                    total = delivery.total,
                    hint_id = %style_hint.hint_id,
                    style_id = %style_hint.style_id,
                    %request_id,
                    "style committed but refresh notification was only partially delivered"
                );
                "partial_failure"
            }
        }
        None => "not_configured",
    };
    let mut response = no_store(
        (
            status,
            Json(StylePublishResponse {
                account_id: authorized.account.as_str(),
                style_id: authorized.style.as_str(),
                outcome,
                refresh,
            }),
        )
            .into_response(),
    );
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&entity_tag).expect("encoded version ETag is a valid header"),
    );
    with_request_id(response, &request_id)
}

fn publication_precondition(
    headers: &HeaderMap,
) -> Result<StylePrecondition, (StatusCode, &'static str, &'static str)> {
    let if_match = single_header(headers, header::IF_MATCH.as_str());
    let if_none_match = single_header(headers, header::IF_NONE_MATCH.as_str());
    match (if_match, if_none_match) {
        (None, Some("*")) => Ok(StylePrecondition::MustNotExist),
        (Some(value), None) => VersionEvidence::from_entity_tag(value)
            .map(StylePrecondition::MustMatch)
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "invalid_precondition",
                    "If-Match must contain one Abashiri version ETag",
                )
            }),
        (None, None) => Err((
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            "Use If-None-Match: * to create or If-Match to replace",
        )),
        _ => Err((
            StatusCode::BAD_REQUEST,
            "invalid_precondition",
            "Specify exactly one supported publication precondition",
        )),
    }
}

fn single_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(value)
}

fn publication_error(error: anyhow::Error, request_id: &RequestId) -> Response {
    let conflict = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<StylePublishConflict>());
    if let Some(conflict) = conflict {
        let (status, code, message) = match conflict {
            StylePublishConflict::AlreadyExists => (
                StatusCode::PRECONDITION_FAILED,
                "style_exists",
                "Style already exists",
            ),
            StylePublishConflict::PreconditionFailed => (
                StatusCode::PRECONDITION_FAILED,
                "style_version_mismatch",
                "Style version does not match",
            ),
            StylePublishConflict::NotFound => (
                StatusCode::PRECONDITION_FAILED,
                "style_missing",
                "Style does not exist",
            ),
        };
        return mutation_error(status, code, message, request_id);
    }
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<IdempotencyConflict>().is_some())
    {
        return mutation_error(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "Idempotency key was reused for a different mutation",
            request_id,
        );
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| matches!(error, object_store::Error::AlreadyExists { .. }))
    }) {
        return mutation_error(
            StatusCode::PRECONDITION_FAILED,
            "style_exists",
            "Style already exists",
            request_id,
        );
    }
    if error.chain().any(|cause| {
        cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| matches!(error, object_store::Error::Precondition { .. }))
    }) {
        return mutation_error(
            StatusCode::PRECONDITION_FAILED,
            "style_version_mismatch",
            "Style version does not match",
            request_id,
        );
    }
    if error
        .chain()
        .any(|cause| cause.downcast_ref::<object_store::Error>().is_some())
    {
        error!(%error, %request_id, "style publication storage operation failed");
        return mutation_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "publication_unavailable",
            "Style publication is temporarily unavailable",
            request_id,
        );
    }
    error!(%error, %request_id, "style publication failed");
    mutation_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "Internal server error",
        request_id,
    )
}

fn mutation_bearer_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: &RequestId,
) -> Response {
    let mut response = mutation_error(status, code, message, request_id);
    if status == StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
}

fn mutation_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: &RequestId,
) -> Response {
    with_request_id(
        no_store(
            (
                status,
                Json(ErrorEnvelope {
                    error: ErrorDetail {
                        code,
                        message,
                        request_id: request_id.as_str(),
                    },
                }),
            )
                .into_response(),
        ),
        request_id,
    )
}

fn with_request_id(mut response: Response, request_id: &RequestId) -> Response {
    response.headers_mut().insert(
        request_id::HEADER,
        HeaderValue::from_str(request_id.as_str()).expect("validated request ID is a valid header"),
    );
    response
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn inbound_request_id(headers: &HeaderMap) -> RequestId {
    request_id::accept_or_generate(
        headers
            .get(request_id::HEADER)
            .and_then(|value| value.to_str().ok()),
    )
}

#[allow(clippy::result_large_err)]
// Like `authorize_style`, an HTTP `Response` is the established control-flow error type.
fn version_entity_tag(
    version: &VersionEvidence,
    noun: &'static str,
    request_id: &RequestId,
) -> Result<String, Response> {
    version.to_entity_tag().map_err(|error| {
        error!(%error, %request_id, "failed to encode {noun} style version");
        mutation_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Internal server error",
            request_id,
        )
    })
}

fn auth_failure_response(failure: ManagementAuthFailure, request_id: &RequestId) -> Response {
    match failure {
        ManagementAuthFailure::InvalidCredential => mutation_bearer_error(
            StatusCode::UNAUTHORIZED,
            "invalid_credential",
            "Invalid management credential",
            request_id,
        ),
        ManagementAuthFailure::Forbidden => mutation_bearer_error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "Management credential is not authorized",
            request_id,
        ),
        ManagementAuthFailure::Unavailable => mutation_bearer_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "authentication_unavailable",
            "Management authentication is unavailable",
            request_id,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use abashiri_core::auth::credential_sha256;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, header},
    };
    use bytes::Bytes;
    use object_store::{ObjectStoreExt as _, memory::InMemory, path::Path as ObjectPath};
    use serde_json::json;
    use tokio::{io::AsyncWriteExt as _, net::TcpListener};
    use tower::ServiceExt as _;
    use url::Url;

    use super::*;
    use crate::operations::OperationalStatusEndpoint;

    const TOKEN: &str = "server-management-token-with-32-bytes";

    async fn management_auth() -> ObjectStoreManagementAuth {
        management_auth_with_actions(vec!["style.read", "style.publish"]).await
    }

    async fn management_auth_with_actions(actions: Vec<&str>) -> ObjectStoreManagementAuth {
        let store = Arc::new(InMemory::new());
        let registry = json!({
            "schema_version": 1,
            "revision": 7,
            "credentials": [{
                "credential_sha256": credential_sha256(TOKEN).unwrap(),
                "enabled": true,
                "actor": {
                    "kind": "workload",
                    "issuer": "test",
                    "subject": "publisher"
                },
                "accounts": ["example"],
                "actions": actions
            }]
        });
        store
            .put(
                &ObjectPath::from("auth/current.json"),
                Bytes::from(serde_json::to_vec(&registry).unwrap()).into(),
            )
            .await
            .unwrap();
        let auth = ObjectStoreManagementAuth::from_object_store(
            store,
            ObjectPath::from("auth/current.json"),
        );
        auth.prime().await.unwrap();
        auth
    }

    async fn operational_client() -> (OperationalStatusClient, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = crate::test_http::read_http_request(&mut stream).await;
            let body = r#"{"schema_version":1,"service":"biei","observer_node_id":"biei-1","observed_at_unix_ms":1,"status":{"ready":true}}"#;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            String::from_utf8(request).unwrap()
        });
        let endpoint: OperationalStatusEndpoint = format!(
            "renderer=http://{address}{}",
            mmpf_http::operational::INTERNAL_STATUS_PATH
        )
        .parse()
        .unwrap();
        (
            OperationalStatusClient::new(vec![endpoint]).unwrap(),
            request,
        )
    }

    fn style_publisher() -> StylePublisher {
        StylePublisher::from_object_stores(
            Arc::new(InMemory::new()),
            ObjectPath::from("state"),
            Arc::new(InMemory::new()),
            ObjectPath::from("audit"),
        )
    }

    fn style_publishing() -> StylePublishing {
        let catalog = StyleCatalog::parse(
            br#"{
                "schema_version": 1,
                "styles": [{
                    "account_id": "example",
                    "style_id": "basic",
                    "object_path": "styles/delivery/basic/style.json"
                }]
            }"#,
        )
        .unwrap();
        StylePublishing::new(catalog, style_publisher(), None).unwrap()
    }

    async fn failing_notifier(
        request_count: usize,
    ) -> (StyleRefreshNotifier, tokio::task::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = tokio::spawn(async move {
            let mut requests = Vec::new();
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = crate::test_http::read_http_request(&mut stream).await;
                stream
                    .write_all(
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                requests.push(String::from_utf8(request).unwrap());
            }
            requests
        });
        let endpoint = Url::parse(&format!("http://{address}/_internal/refresh/style")).unwrap();
        (StyleRefreshNotifier::new(vec![endpoint]).unwrap(), requests)
    }

    fn style_request(
        body: impl Into<Body>,
        idempotency_key: &str,
        precondition_name: header::HeaderName,
        precondition_value: &str,
    ) -> Request<Body> {
        Request::put("/accounts/example/styles/basic")
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", idempotency_key)
            .header(precondition_name, precondition_value)
            .header(request_id::HEADER, "request-test-1")
            .body(body.into())
            .unwrap()
    }

    #[tokio::test]
    async fn health_is_json_and_not_cacheable() {
        for path in ["/livez", "/readyz"] {
            let response = router(None, None)
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store"
            );
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/json"
            );
            assert_eq!(
                to_bytes(response.into_body(), 1024).await.unwrap(),
                r#"{"status":"ok"}"#
            );
        }
    }

    #[tokio::test]
    async fn unknown_paths_do_not_look_implemented() {
        let response = router(None, None)
            .oneshot(
                Request::post("/future-management-route")
                    .header(request_id::HEADER, "request-not-found-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(
            response.headers()[request_id::HEADER],
            "request-not-found-1"
        );
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "not_found");
        assert_eq!(body["error"]["request_id"], "request-not-found-1");

        let response = router(None, None)
            .oneshot(
                Request::post("/whoami")
                    .header(request_id::HEADER, "request-method-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "method_not_allowed");
        assert_eq!(body["error"]["request_id"], "request-method-1");
    }

    #[tokio::test]
    async fn whoami_requires_object_store_management_credential() {
        let auth = management_auth().await;
        let response = router(Some(auth), None)
            .oneshot(Request::get("/whoami").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Bearer"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "invalid_credential");
        assert!(body["error"]["request_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn whoami_returns_only_bounded_principal_metadata() {
        let auth = management_auth().await;
        let response = router(Some(auth), None)
            .oneshot(
                Request::get("/whoami")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["actor"]["subject"], "publisher");
        assert_eq!(body["accounts"], json!(["example"]));
        assert_eq!(body["actions"], json!(["style.read", "style.publish"]));
        assert_eq!(body["registry_revision"], 7);
        assert!(!body.to_string().contains(TOKEN));
    }

    #[tokio::test]
    async fn operational_status_requires_its_global_action_and_hides_endpoint_urls() {
        let (operations, upstream_request) = operational_client().await;
        let app = router_with_operations(
            Some(management_auth_with_actions(vec!["operations.read"]).await),
            None,
            Some(operations),
        );
        let unauthorized = app
            .clone()
            .oneshot(
                Request::get("/operations/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::get("/operations/status")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(request_id::HEADER, "operations-request-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            response.headers()[request_id::HEADER],
            "operations-request-1"
        );
        let body = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["complete"], true);
        assert_eq!(body["sources"][0]["source_id"], "renderer");
        assert_eq!(body["sources"][0]["state"], "fresh");
        assert!(!body.to_string().contains("127.0.0.1"));
        assert!(
            upstream_request
                .await
                .unwrap()
                .starts_with("GET /_internal/operations/v1/status HTTP/1.1\r\n")
        );
    }

    #[tokio::test]
    async fn style_route_exists_only_when_publication_is_configured() {
        let auth = management_auth().await;
        let response = router(Some(auth), None)
            .oneshot(style_request(
                r#"{"version":8,"sources":{},"layers":[]}"#,
                "style-create",
                header::IF_NONE_MATCH,
                "*",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn creates_replays_and_conditionally_replaces_a_catalog_style() {
        let app = router(Some(management_auth().await), Some(style_publishing()));
        let create = app
            .clone()
            .oneshot(style_request(
                r#"{"version":8,"name":"first","sources":{},"layers":[]}"#,
                "style-create",
                header::IF_NONE_MATCH,
                "*",
            ))
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::CREATED);
        assert_eq!(create.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(create.headers()[request_id::HEADER], "request-test-1");
        let first_version = create.headers()[header::ETAG].to_str().unwrap().to_string();
        assert!(first_version.starts_with("\"abashiri-v1."));
        let create_body = to_bytes(create.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["refresh"],
            "not_configured"
        );

        let current = app
            .clone()
            .oneshot(
                Request::get("/accounts/example/styles/basic")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(current.status(), StatusCode::OK);
        assert_eq!(current.headers()[header::ETAG], first_version);
        assert_eq!(
            to_bytes(current.into_body(), 4096).await.unwrap(),
            r#"{"version":8,"name":"first","sources":{},"layers":[]}"#
        );

        let replay = app
            .clone()
            .oneshot(style_request(
                r#"{"version":8,"name":"first","sources":{},"layers":[]}"#,
                "style-create",
                header::IF_NONE_MATCH,
                "*",
            ))
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        assert_eq!(replay.headers()[header::ETAG], first_version);
        let replay_body = to_bytes(replay.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&replay_body).unwrap()["outcome"],
            "already_completed"
        );

        let duplicate_create = app
            .clone()
            .oneshot(style_request(
                r#"{"version":8,"name":"different","sources":{},"layers":[]}"#,
                "style-create-collision",
                header::IF_NONE_MATCH,
                "*",
            ))
            .await
            .unwrap();
        assert_eq!(duplicate_create.status(), StatusCode::PRECONDITION_FAILED);
        let duplicate_body = to_bytes(duplicate_create.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&duplicate_body).unwrap()["error"]["code"],
            "style_exists"
        );

        let replace = app
            .clone()
            .oneshot(style_request(
                r#"{"version":8,"name":"second","sources":{},"layers":[]}"#,
                "style-replace",
                header::IF_MATCH,
                &first_version,
            ))
            .await
            .unwrap();
        assert_eq!(replace.status(), StatusCode::OK);
        assert_ne!(replace.headers()[header::ETAG], first_version);

        let stale = app
            .oneshot(style_request(
                r#"{"version":8,"name":"stale","sources":{},"layers":[]}"#,
                "style-stale",
                header::IF_MATCH,
                &first_version,
            ))
            .await
            .unwrap();
        assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
        let stale_body = to_bytes(stale.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stale_body).unwrap()["error"]["code"],
            "style_version_mismatch"
        );
    }

    #[tokio::test]
    async fn notification_failure_preserves_success_and_replay_retries_it() {
        let (notifier, received) = failing_notifier(2).await;
        let mut publishing = style_publishing();
        publishing.notifier = Some(notifier);
        let app = router(Some(management_auth().await), Some(publishing));

        for (expected_status, expected_outcome) in [
            (StatusCode::CREATED, "committed"),
            (StatusCode::OK, "already_completed"),
        ] {
            let response = app
                .clone()
                .oneshot(style_request(
                    r#"{"version":8,"sources":{},"layers":[]}"#,
                    "style-notifier-retry",
                    header::IF_NONE_MATCH,
                    "*",
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            let body = to_bytes(response.into_body(), 4096).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["outcome"], expected_outcome);
            assert_eq!(body["refresh"], "partial_failure");
        }

        let requests = received.await.unwrap();
        assert_eq!(requests.len(), 2);
        let hints: Vec<serde_json::Value> = requests
            .iter()
            .map(|request| serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap())
            .collect();
        assert_eq!(hints[0], hints[1]);
        assert_eq!(hints[0]["style_id"], "delivery/basic");
        assert_eq!(
            hints[0]["hint_id"],
            mutation_key_sha256("style-notifier-retry").unwrap()
        );
    }

    #[test]
    fn publication_catalog_requires_the_canonical_delivery_style_key() {
        assert!(
            StyleCatalog::parse(
                br#"{
                "schema_version": 1,
                "styles": [{
                    "account_id": "example",
                    "style_id": "basic",
                    "object_path": "styles/delivery/bad name/style.json"
                }]
            }"#,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn publication_requires_auth_catalog_idempotency_and_precondition() {
        let app = router(Some(management_auth().await), Some(style_publishing()));
        let unauthorized = app
            .clone()
            .oneshot(
                Request::put("/accounts/example/styles/basic")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"version":8}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let unknown = app
            .clone()
            .oneshot(
                Request::put("/accounts/example/styles/unknown")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("idempotency-key", "unknown-style")
                    .header(header::IF_NONE_MATCH, "*")
                    .body(Body::from(r#"{"version":8}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        let no_precondition = app
            .oneshot(
                Request::put("/accounts/example/styles/basic")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("idempotency-key", "no-precondition")
                    .body(Body::from(r#"{"version":8}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(no_precondition.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[tokio::test]
    async fn oversized_style_body_returns_payload_too_large() {
        let response = router(Some(management_auth().await), Some(style_publishing()))
            .oneshot(style_request(
                vec![b'x'; MAX_STYLE_BYTES + 1024],
                "oversized-style",
                header::IF_NONE_MATCH,
                "*",
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "style_too_large");
    }
}

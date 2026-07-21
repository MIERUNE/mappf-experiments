//! Request-scoped correlation context owned by the HTTP server.

pub(crate) use mmpf_http::request_id::{HEADER, RequestId, accept_or_generate};

tokio::task_local! {
    pub(crate) static REQUEST_ID: RequestId;
}

pub(crate) fn current() -> Option<RequestId> {
    REQUEST_ID.try_with(Clone::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn current_follows_task_local_scope() {
        assert!(current().is_none());
        let id = RequestId::from_string("request-1");
        REQUEST_ID
            .scope(id.clone(), async {
                assert_eq!(current(), Some(id));
            })
            .await;
        assert!(current().is_none());
    }
}

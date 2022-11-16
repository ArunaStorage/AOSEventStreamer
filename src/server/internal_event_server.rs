use aruna_rust_api::api::internal::v1::{internal_event_emitter_service_server, EmitEventResponse};
use async_trait::async_trait;
use tonic::{Response, Status};

use log::error;

use crate::stream_handler::handler::EventHandler;

use super::server::INTERNAL_AUTHZ_TOKEN;

pub struct InternalServer {
    pub event_handler: Box<dyn EventHandler + Send + Sync>,
    pub internal_token: String,
}

#[async_trait]
impl internal_event_emitter_service_server::InternalEventEmitterService for InternalServer {
    async fn emit_event(
        &self,
        request: tonic::Request<aruna_rust_api::api::internal::v1::EmitEventRequest>,
    ) -> Result<tonic::Response<aruna_rust_api::api::internal::v1::EmitEventResponse>, tonic::Status>
    {
        let metadata = request.metadata();
        let token = match metadata.get(INTERNAL_AUTHZ_TOKEN) {
            Some(value) => match value.to_str() {
                Ok(value) => value.to_string(),
                Err(err) => {
                    error!("{}", err);
                    return Err(tonic::Status::invalid_argument("could not read token"));
                }
            },
            None => {
                return Err(tonic::Status::unauthenticated(
                    "authentication header required and was not found",
                ))
            }
        };
        let inner_request = request.into_inner();

        if token != self.internal_token {
            return Err(tonic::Status::new(
                tonic::Code::PermissionDenied,
                "bad token",
            ));
        };

        let resource_type = inner_request.event_resource().clone();
        let resource_id = inner_request.resource_id.clone();
        let event_type = inner_request.event_type().clone();

        for relation in inner_request.relations {
            match self
                .event_handler
                .register_event(resource_type, resource_id.clone(), event_type, &relation)
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    error!("{}", err);
                    return Err(Status::internal("could not emit event"));
                }
            };
        }

        return Ok(Response::new(EmitEventResponse {}));
    }
}

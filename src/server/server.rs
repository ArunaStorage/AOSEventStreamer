use aruna_rust_api::api::{
    internal::v1::{
        internal_authorize_service_client::InternalAuthorizeServiceClient,
        internal_event_emitter_service_server::InternalEventEmitterServiceServer,
        internal_event_service_client::InternalEventServiceClient,
    },
    notification::services::v1::update_notification_service_server::UpdateNotificationServiceServer,
    storage::services::v1::resource_info_service_client::ResourceInfoServiceClient,
};
use async_nats::ServerAddr;
use futures::future::try_join;
use log::error;
use tonic::transport::Server;

use crate::stream_handler::natsio::NatsIOEventHandler;

use super::{internal_event_server::InternalServer, public_event_server::PublicServer};

pub const TOKEN_METADATA_NAME: &str = "api-token";
pub const INTERNAL_AUTHZ_TOKEN: &str = "internal-token";

pub struct EventServer {}

impl EventServer {
    pub async fn start_server(
        internal_event_token: String,
        nats_hosts: &[ServerAddr],
        internal_event_service_client_host: String,
        authz_event_service_client_host: String,
        internal_event_emitter_service_server_host: String,
        resource_host: String,
        public_event_server_host: String,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
        let nats_client = async_nats::connect(nats_hosts).await?;
        let internal_event_service_client =
            match InternalEventServiceClient::connect(internal_event_service_client_host).await {
                Ok(value) => value,
                Err(err) => {
                    error!("{}", err);
                    return Err(Box::new(err));
                }
            };
        let internal_authz_service_client =
            match InternalAuthorizeServiceClient::connect(authz_event_service_client_host).await {
                Ok(value) => value,
                Err(err) => {
                    error!("{}", err);
                    return Err(Box::new(err));
                }
            };

        let resource_client = match ResourceInfoServiceClient::connect(resource_host).await {
            Ok(value) => value,
            Err(err) => {
                error!("{}", err);
                return Err(Box::new(err));
            }
        };

        let event_handler = Box::new(NatsIOEventHandler::new(nats_client).await?);

        let internal_event_server = InternalServer {
            event_handler: event_handler.clone(),
            internal_token: internal_event_token,
        };

        let public_event_server = PublicServer {
            internal_events_client: internal_event_service_client.clone(),
            internal_authz_client: internal_authz_service_client.clone(),
            event_handler: event_handler.clone(),
            resource_client: resource_client.clone(),
        };

        let internal_event_server_service = Server::builder()
            .add_service(InternalEventEmitterServiceServer::new(
                internal_event_server,
            ))
            .serve(internal_event_emitter_service_server_host.parse().unwrap());

        let public_event_server_service = Server::builder()
            .add_service(UpdateNotificationServiceServer::new(public_event_server))
            .serve(public_event_server_host.parse().unwrap());

        match try_join(internal_event_server_service, public_event_server_service).await {
            Ok(value) => value,
            Err(err) => {
                error!("{}", err);
                return Err(Box::new(err));
            }
        };
        return Ok(());
    }
}

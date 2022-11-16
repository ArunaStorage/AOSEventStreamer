use aruna_rust_api::api::internal::v1::internal_authorize_service_client::InternalAuthorizeServiceClient;
use aruna_rust_api::api::storage::models::v1::ResourceAction;

use aruna_rust_api::api::storage::services::v1::resource_info_service_client::ResourceInfoServiceClient;
use aruna_rust_api::api::storage::services::v1::GetResourceHierarchyRequest;
use futures::lock::Mutex;
use log::error;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use aruna_rust_api::api::internal::v1::internal_event_service_client::InternalEventServiceClient;
use aruna_rust_api::api::internal::v1::{
    AuthorizeRequest, CreateStreamGroupRequest, GetStreamGroupRequest,
};
use aruna_rust_api::api::notification::services::v1::read_stream_group_messages_request::StreamAction;
use aruna_rust_api::api::notification::services::v1::{
    update_notification_service_server, CreateEventStreamingGroupRequest,
    CreateEventStreamingGroupResponse, DeleteEventStreamingGroupRequest,
    DeleteEventStreamingGroupResponse, EventNotificationMessage, EventType,
    NotificationStreamResponse, ReadStreamGroupMessagesResponse,
};
use async_nats::jetstream;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use prost::Message;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::stream_handler::handler::EventHandler;

use super::server::TOKEN_METADATA_NAME;

// Server to handle the outgoing notifications for users
pub struct PublicServer {
    pub internal_events_client: InternalEventServiceClient<Channel>,
    pub internal_authz_client: InternalAuthorizeServiceClient<Channel>,
    pub resource_client: ResourceInfoServiceClient<Channel>,
    pub event_handler: Box<dyn EventHandler + Send + Sync>,
}

// The type definition for the outgoing response stream
type ResponseStream =
    Pin<Box<dyn Stream<Item = Result<ReadStreamGroupMessagesResponse, Status>> + Send>>;

#[async_trait]
impl update_notification_service_server::UpdateNotificationService for PublicServer {
    // Creates a new event streaming group depdending on the underlaying notification system
    // Currently only Nats.io is supported
    // The main challenge is the creation query subject
    async fn create_event_streaming_group(
        &self,
        request: tonic::Request<CreateEventStreamingGroupRequest>,
    ) -> Result<tonic::Response<CreateEventStreamingGroupResponse>, tonic::Status> {
        let metadata = request.metadata().clone();

        let token = match metadata.get(TOKEN_METADATA_NAME) {
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

        let mut authz_request = Request::new(AuthorizeRequest {
            resource: inner_request.resource,
            resource_action: ResourceAction::Read as i32,
            resource_id: inner_request.resource_id.clone(),
        });

        authz_request.metadata_mut().clone_from(&metadata.clone());

        let authorized = self
            .internal_authz_client
            .clone()
            .authorize(authz_request)
            .await
            .unwrap();

        if !authorized.into_inner().ok {
            return Err(tonic::Status::new(
                tonic::Code::PermissionDenied,
                "unsufficient permissions",
            ));
        };

        let mut hierarchy_req = Request::new(GetResourceHierarchyRequest {
            resource_id: inner_request.resource_id.clone(),
            resource_type: inner_request.resource,
        });

        hierarchy_req.metadata_mut().clone_from(&metadata.clone());
        let hiearchies = match self
            .resource_client
            .clone()
            .get_resource_hierarchy(hierarchy_req)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                error!("{}", err);
                return Err(err);
            }
        }
        .into_inner()
        .hierarchies;

        let stream_group = match self
            .internal_events_client
            .clone()
            .create_stream_group(CreateStreamGroupRequest {
                event_type: EventType::All as i32,
                resource_type: inner_request.resource.clone(),
                notify_on_sub_resource: inner_request.include_subresource.clone(),
                resource_id: inner_request.resource_id.clone(),
                token: token,
            })
            .await
        {
            Ok(value) => value.into_inner().stream_group.unwrap(),
            Err(err) => {
                error!("{}", err.message());
                return Err(tonic::Status::new(
                    tonic::Code::Internal,
                    "could not create stream group",
                ));
            }
        };

        let hierarchy = match hiearchies.get(0) {
            Some(value) => value,
            None => {
                return Err(Status::internal(
                    "no hierarchy found, cannot create query string",
                ))
            }
        };

        match self
            .event_handler
            .create_stream_group(
                stream_group.id.clone(),
                &hierarchy,
                inner_request.resource(),
                inner_request.resource_id,
                inner_request.include_subresource,
            )
            .await
        {
            Ok(_) => {}
            Err(err) => {
                error!("{}", err);
                return Err(tonic::Status::internal("could not create stream group"));
            }
        };

        return Ok(Response::new(CreateEventStreamingGroupResponse {
            stream_group_id: stream_group.id.clone(),
        }));
    }

    async fn delete_event_streaming_group(
        &self,
        _request: tonic::Request<DeleteEventStreamingGroupRequest>,
    ) -> Result<tonic::Response<DeleteEventStreamingGroupResponse>, tonic::Status> {
        todo!()
    }

    type ReadStreamGroupMessagesStream = ResponseStream;

    // A bidirectional stream
    // It sends events from the stream_group to the user
    // The user has to the id of send handled messages back to acknowledge them
    // Since we can't reconstruct individual messages easily this is done stateful per stream
    // Therefor each stream has a small internal HashMap that stores the chunk_ids of the send messages
    // Based on these the messages can be acknowledged against the underlaying streaming system.
    async fn read_stream_group_messages(
        &self,
        request: tonic::Request<
            tonic::Streaming<
                aruna_rust_api::api::notification::services::v1::ReadStreamGroupMessagesRequest,
            >,
        >,
    ) -> Result<tonic::Response<Self::ReadStreamGroupMessagesStream>, tonic::Status> {
        let metadata = request.metadata().clone();

        let token = match metadata.get(TOKEN_METADATA_NAME) {
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

        let mut stream = request.into_inner();
        let initial_msg = match stream.next().await {
            Some(value) => match value {
                Ok(value) => value,
                Err(err) => {
                    error!("{}", err);
                    return Err(tonic::Status::internal("error on stream handling"));
                }
            },
            None => {
                return Err(tonic::Status::invalid_argument(
                    "init message required to initiate streaming",
                ))
            }
        };

        let init = match initial_msg.stream_action {
            Some(value) => match value {
                StreamAction::Init(value) => value,
                StreamAction::Ack(_) => {
                    return Err(Status::invalid_argument(
                        "an init message needs to be send before any ack message",
                    ))
                }
            },
            None => {
                return Err(Status::invalid_argument(
                    "could not read stream action value",
                ));
            }
        };

        let stream_group = match self
            .internal_events_client
            .clone()
            .get_stream_group(GetStreamGroupRequest {
                stream_group_id: init.stream_group_id,
                token: token.clone(),
            })
            .await
        {
            Ok(value) => value.into_inner().stream_group.unwrap(),
            Err(err) => {
                error!("{}", err);
                return Err(tonic::Status::internal(
                    "internal error requesting stream group",
                ));
            }
        };

        let mut authz_request = Request::new(AuthorizeRequest {
            resource: stream_group.resource_type.clone(),
            resource_action: stream_group.event_type.clone(),
            resource_id: stream_group.resource_id.clone(),
        });

        authz_request.metadata_mut().clone_from(&metadata);

        match self
            .internal_authz_client
            .clone()
            .authorize(authz_request)
            .await
        {
            Ok(value) => {
                let authorized = value.into_inner().ok;
                if !authorized {
                    return Err(tonic::Status::permission_denied(
                        "not allowed to perform call",
                    ));
                }
            }
            Err(err) => {
                error!("{}", err);
                return Err(tonic::Status::internal(
                    "internal error when authorizing request",
                ));
            }
        }

        let stream_group_handler = match self
            .event_handler
            .create_event_stream_handler(stream_group.id)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                error!("{}", err);
                return Err(tonic::Status::internal(
                    "could not create stream group handler",
                ));
            }
        };

        // Hashmap to store the send chunks and acknowledge them later
        let ack_chunks: Arc<Mutex<HashMap<String, Arc<Vec<jetstream::Message>>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Global variable to track if a close request was send
        // Is used to synchronize between input and output streams
        let close = Arc::new(AtomicBool::new(false));

        let (err_sender, err_recv) = async_channel::bounded(10);

        let cloned_close = close.clone();
        let cloned_ack_chunks = ack_chunks.clone();
        // Spawns the handler that handles the incoming request from the client
        tokio::spawn(async move {
            while let Some(ack_request) = stream.next().await {
                let ack_request_unwrapped = match ack_request {
                    Ok(value) => value,
                    Err(err) => {
                        error!("{}", err);
                        err_sender
                            .send(Status::internal("error reading from input stream"))
                            .await
                            .unwrap();
                        break;
                    }
                };
                if ack_request_unwrapped.close {
                    cloned_close.store(true, Ordering::Relaxed);
                }

                let ack = match ack_request_unwrapped.stream_action.unwrap() {
                    StreamAction::Init(_) => {
                        // Init can only be called once and will otherwise yield an error
                        // The error needs to be propagated to the response channel
                        // This send should never fail to send, otherwise it should panic
                        err_sender
                            .send(Status::invalid_argument(
                                "init can only be used once in request",
                            ))
                            .await
                            .unwrap();
                        break;
                    }
                    StreamAction::Ack(value) => value,
                };

                // Ackndowledge the messages in a chunk based on their ids
                // Could be parallelized if performance becomes an issue
                let chunk_ids = ack.ack_chunk_id;
                for chunk_id in chunk_ids {
                    let mut ack_chunks = cloned_ack_chunks.lock().await;
                    let msg_chunks = ack_chunks.remove(&chunk_id).unwrap();
                    for msg in msg_chunks.iter() {
                        match msg.ack().await {
                            Ok(_) => {}
                            Err(err) => {
                                error!("{}", err);
                                err_sender
                                    .send(Status::internal(format!(
                                        "error when acknowledging ack chunk with id {}",
                                        chunk_id,
                                    )))
                                    .await
                                    .unwrap();
                            }
                        };
                    }
                }
            }
        });

        // async_stream::stream!
        // Output stream
        // This will read messages from an underlaying event stream service and return them to the client
        let output = async_stream::stream! {
            // Iterate until a close is requested
            while !close.load(Ordering::Relaxed) {
                // Check if any error occured in request handling
                match err_recv.try_recv() {
                    Ok(err) => {
                        yield Err(err);
                    }
                    //An error here indicates and empty stream and can be ignored
                    Err(_) => {}
                };

                // chunk id for message chunk send to the client
                // Used to ack the messages in that chunk
                let chunk_id = uuid::Uuid::new_v4();

                let msgs = match stream_group_handler.get_stream_group_msgs().await {
                    Ok(value) => value,
                    Err(err) => {
                        error!("{}", err);
                        yield Err(Status::internal("error reading from event system"));
                        continue;
                    }
                };

                let msgs = Arc::new(msgs);
                ack_chunks
                    .lock()
                    .await
                    .insert(chunk_id.to_string(), msgs.clone());
                let event_notfication_msgs: Vec<NotificationStreamResponse> = msgs
                    .iter()
                    .map(|x| {
                        let message_bytes = x.payload.clone();
                        let event_msg = EventNotificationMessage::decode(message_bytes).unwrap();
                        NotificationStreamResponse {
                            message: Some(event_msg),
                            sequence: 0,
                            timestamp: None,
                        }
                    })
                    .collect::<Vec<NotificationStreamResponse>>();

                let response = ReadStreamGroupMessagesResponse {
                    notification: event_notfication_msgs,
                    ack_chunk_id: chunk_id.to_string(),
                };
                yield Ok(response)
            }
        };

        Ok(Response::new(
            Box::pin(output) as Self::ReadStreamGroupMessagesStream
        ))
    }
}

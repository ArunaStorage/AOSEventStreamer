#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused)]

use aruna_rust_api::api::{
    internal::v1::{
        internal_authorize_service_client::InternalAuthorizeServiceClient,
        internal_authorize_service_server::InternalAuthorizeServiceServer,
        internal_event_emitter_service_server::InternalEventEmitterService,
        internal_event_service_client::InternalEventServiceClient,
        internal_event_service_server::InternalEventServiceServer, EmitEventRequest, Relation,
    },
    notification::services::v1::{
        read_stream_group_messages_request::StreamAction,
        update_notification_service_client::UpdateNotificationServiceClient,
        update_notification_service_server::UpdateNotificationServiceServer,
        CreateEventStreamingGroupRequest, EventType, NotficationStreamAck, NotificationStreamInit,
        ReadStreamGroupMessagesRequest,
    },
    storage::{
        models::v1::{Project, ResourceType},
        services::v1::{
            resource_info_service_client::ResourceInfoServiceClient,
            resource_info_service_server::ResourceInfoServiceServer,
        },
    },
};
use async_channel::{Receiver, Sender};
use log::error;

use std::io::Write;
use tokio_stream::StreamExt;

use core::time;
use crossbeam_utils::sync::WaitGroup;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Mutex, Once, RwLock},
    thread,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{metadata::MetadataMap, transport::Server, Request};

use crate::{
    server::{
        internal_event_server::InternalServer,
        public_event_server::PublicServer,
        server::{INTERNAL_AUTHZ_TOKEN, TOKEN_METADATA_NAME},
    },
    storage_test_server::storage_endpoint_mock::{
        AuthzEndpointMock, ResourceInfoMock, StorageEndpointMock,
    },
    stream_handler::{handler::EventHandler, natsio::NatsIOEventHandler},
};

static INIT: Once = Once::new();
static SERVICE_ENDPOINT_PORT: RwLock<u16> = RwLock::new(0);

fn initialize_e2e_server() {
    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{}:{} {} [{}] - {}",
                record.file().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter_level(log::LevelFilter::Error)
        .init();

    let wg = WaitGroup::new();
    INIT.call_once(|| start_server_background(wg.clone()));

    wg.wait();
}

fn start_server_background(wg: WaitGroup) {
    tokio::spawn(async move {
        let addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        *SERVICE_ENDPOINT_PORT.write().unwrap() = port;

        let stream_group_store = Mutex::new(HashMap::new());
        let authz_service = AuthzEndpointMock {};
        let event_service = StorageEndpointMock {
            stream_groups: stream_group_store,
        };
        let resource_service = ResourceInfoMock {};

        let builder = Server::builder()
            .add_service(InternalAuthorizeServiceServer::new(authz_service))
            .add_service(InternalEventServiceServer::new(event_service))
            .add_service(ResourceInfoServiceServer::new(resource_service))
            .serve_with_incoming(TcpListenerStream::new(listener));

        drop(wg);
        builder.await.unwrap();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_test_nats() {
    initialize_e2e_server();
    let nats_client = async_nats::connect("localhost:4222").await.unwrap();
    let event_handler = Box::new(NatsIOEventHandler::new(nats_client).await.unwrap());

    let internal_events_handler = InternalServer {
        event_handler: event_handler.clone(),
        internal_token: "test".to_string(),
    };

    let server_addr_port = SERVICE_ENDPOINT_PORT.read().unwrap().clone();

    let authz_client =
        InternalAuthorizeServiceClient::connect(format!("http://127.0.0.1:{}", server_addr_port))
            .await
            .unwrap();

    let resource_client =
        ResourceInfoServiceClient::connect(format!("http://127.0.0.1:{}", server_addr_port))
            .await
            .unwrap();

    let internal_event_client =
        InternalEventServiceClient::connect(format!("http://127.0.0.1:{}", server_addr_port))
            .await
            .unwrap();

    let public_events_handler = PublicServer {
        event_handler: event_handler,
        internal_authz_client: authz_client,
        resource_client: resource_client,
        internal_events_client: internal_event_client,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let notification_server_port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        Server::builder()
            .add_service(UpdateNotificationServiceServer::new(public_events_handler))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut emit_request = Request::new(EmitEventRequest {
        event_resource: ResourceType::Project as i32,
        resource_id: "project_id".to_string(),
        event_type: EventType::All as i32,
        relations: vec![Relation {
            ..Default::default()
        }],
    });

    emit_request
        .metadata_mut()
        .insert(INTERNAL_AUTHZ_TOKEN, "test".parse().unwrap());

    internal_events_handler
        .emit_event(emit_request)
        .await
        .unwrap();

    let public_event_client = UpdateNotificationServiceClient::connect(format!(
        "http://127.0.0.1:{}",
        notification_server_port
    ))
    .await
    .unwrap();

    let mut create_event_streaming_group_request = Request::new(CreateEventStreamingGroupRequest {
        include_subresource: true,
        resource: ResourceType::Project as i32,
        resource_id: "project_id".to_string(),
        ..Default::default()
    });

    create_event_streaming_group_request
        .metadata_mut()
        .append(TOKEN_METADATA_NAME, "test".parse().unwrap());

    let stream_group = public_event_client
        .clone()
        .create_event_streaming_group(create_event_streaming_group_request)
        .await
        .unwrap()
        .into_inner();

    // Here be dragons
    // Bidirectional is a bit more complicated since its request needs to be a stream

    let (input_channel_sender, input_channel_recv): (Sender<Vec<String>>, Receiver<Vec<String>>) =
        async_channel::unbounded();

    //async_stream::stream!
    let output = async_stream::stream! {
        yield ReadStreamGroupMessagesRequest {
            close: false,
            stream_action: Some(StreamAction::Init(NotificationStreamInit {
                stream_group_id: stream_group.stream_group_id,
            })),
        };

        loop {
            let ack_ids_err = input_channel_recv.recv().await;
            let ack_ids = match ack_ids_err {
                Ok(value) => value,
                Err(err) => {
                    error!("{}", err);
                    break;
                }
            };

            yield ReadStreamGroupMessagesRequest {
                close: false,
                stream_action: Some(StreamAction::Ack(NotficationStreamAck {
                    ack_chunk_id: ack_ids,
                })),
            };
        }

        yield ReadStreamGroupMessagesRequest {
            close: true,
            stream_action: Some(StreamAction::Ack(NotficationStreamAck {
                ack_chunk_id: vec![],
            })),
        };
    };

    let mut stream_request = Request::new(output);
    stream_request
        .metadata_mut()
        .append(TOKEN_METADATA_NAME, "test".parse().unwrap());

    let output_stream_response = public_event_client
        .clone()
        .read_stream_group_messages(stream_request)
        .await
        .unwrap();
    let mut output_stream = output_stream_response.into_inner();

    let mut id_counter = 0;
    while let Some(recv) = output_stream.next().await {
        let message = recv.unwrap();
        let response_ids = message.notification;
        let chunk_id = message.ack_chunk_id;
        let ids: Vec<String> = response_ids
            .iter()
            .map(|x| x.clone().message.unwrap().resource_id)
            .collect();

        id_counter += ids.len();
        match input_channel_sender.send(vec![chunk_id]).await {
            Ok(_) => {}
            Err(err) => {
                error!("{}", err);
                break;
            }
        };

        if id_counter > 0 {
            break;
        }
    }

    if id_counter == 0 {
        panic!("no messages found")
    }
}

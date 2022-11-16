use std::time::Duration;

use aruna_rust_api::api::storage::models::v1::ResourceType;
use aruna_rust_api::api::storage::services::v1::Hierarchy;
use async_nats::jetstream::consumer::Config;
use async_nats::jetstream::stream::Stream;
use futures::StreamExt;

use aruna_rust_api::api::internal::v1::Relation;
use aruna_rust_api::api::notification::services::v1::{EventNotificationMessage, EventType};

use async_nats::{
    jetstream::{consumer, Context},
    Client,
};

use async_trait::async_trait;
use prost::{bytes::Bytes, Message};

use crate::utils::utils::NatsIOUtils;

use super::handler::{EventHandler, EventStreamHandler};

const DEFAULT_STREAM_NAME: &str = "STORAGE_UPDATES";

#[derive(Debug, Clone)]
pub struct NatsIOEventHandler {
    jetstream_context: Context,
    stream: Stream,
}

impl NatsIOEventHandler {
    pub async fn new(
        nats_client: Client,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let jetstream_context = async_nats::jetstream::new(nats_client);
        let stream = jetstream_context.get_stream(DEFAULT_STREAM_NAME).await?;

        let nats = NatsIOEventHandler {
            jetstream_context: jetstream_context,
            stream: stream,
        };
        return Ok(nats);
    }
}

#[async_trait]
impl EventHandler for NatsIOEventHandler {
    async fn create_event_stream_handler(
        &self,
        stream_group_id: String,
    ) -> Result<Box<dyn EventStreamHandler + Send + Sync>, Box<dyn std::error::Error + Send + Sync>>
    {
        let consumer = self.stream.get_consumer(stream_group_id.as_str()).await?;
        let stream_handler = Box::new(NatsIOEventStreamHandler { consumer: consumer });

        return Ok(stream_handler);
    }

    async fn register_event(
        &self,
        resource_type: ResourceType,
        resource_id: String,
        event_type: EventType,
        relation: &Relation,
    ) -> Result<(), tonic::Status> {
        let message = EventNotificationMessage {
            resource: resource_type as i32,
            updated_type: event_type as i32,
            resource_id: resource_id.clone(),
        };

        let encoded_message = message.encode_to_vec();
        let encoded_msg_bytes = Bytes::from(encoded_message);

        let event_resource = resource_type;

        let subjects = match event_resource {
            aruna_rust_api::api::storage::models::v1::ResourceType::Unspecified => todo!(),
            aruna_rust_api::api::storage::models::v1::ResourceType::Project => {
                vec![NatsIOUtils::project_subject(resource_id)]
            }
            aruna_rust_api::api::storage::models::v1::ResourceType::Collection => {
                vec![NatsIOUtils::collection_subject(
                    relation.project.clone(),
                    resource_id,
                )]
            }
            aruna_rust_api::api::storage::models::v1::ResourceType::ObjectGroup => {
                let mut subjects = Vec::new();

                for object_group in &relation.object_groups {
                    let subject = NatsIOUtils::object_group_subject(
                        relation.project.clone(),
                        relation.collection.clone(),
                        object_group.shared_object_group_id.clone(),
                        resource_id.clone(),
                    );
                    subjects.push(subject)
                }

                subjects
            }
            aruna_rust_api::api::storage::models::v1::ResourceType::Object => {
                let mut subjects = Vec::new();

                for object_group in &relation.object_groups {
                    let subject = NatsIOUtils::object_group_subject(
                        relation.project.clone(),
                        relation.collection.clone(),
                        object_group.shared_object_group_id.clone(),
                        resource_id.clone(),
                    );
                    subjects.push(subject)
                }

                let object_subject = NatsIOUtils::object_subject(
                    relation.project.clone(),
                    relation.collection.clone(),
                    relation.shared_object.clone(),
                    resource_id,
                );

                subjects.push(object_subject);

                subjects
            }
            aruna_rust_api::api::storage::models::v1::ResourceType::All => todo!(),
        };

        let publish_futures = subjects
            .into_iter()
            .map(|x| self.jetstream_context.publish(x, encoded_msg_bytes.clone()));

        let results = futures::future::join_all(publish_futures).await;
        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(_) => {}
                Err(err) => {
                    log::error!("{}", err);
                    errs.push(err);
                }
            }
        }

        return Ok(());
    }

    async fn create_stream_group(
        &self,
        stream_group_id: String,
        hierarchy: &Hierarchy,
        resource_type: ResourceType,
        resource_id: String,
        include_subresources: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stream = self
            .jetstream_context
            .get_stream(DEFAULT_STREAM_NAME)
            .await?;

        let query_subject = match resource_type {
            ResourceType::Unspecified => todo!(),
            ResourceType::Project => NatsIOUtils::project_query(resource_id, include_subresources),
            ResourceType::Collection => NatsIOUtils::collection_query(
                hierarchy.project_id.clone(),
                resource_id,
                include_subresources,
            ),
            ResourceType::ObjectGroup => todo!(),
            ResourceType::Object => todo!(),
            ResourceType::All => todo!(),
        };

        let _consumer = stream
            .create_consumer(Config {
                name: Some(stream_group_id),
                filter_subject: query_subject,
                ..Default::default()
            })
            .await?;

        return Ok(());
    }
}

#[derive(Debug, Clone)]
pub struct NatsIOEventStreamHandler {
    pub consumer: consumer::PullConsumer,
}

#[async_trait]
impl EventStreamHandler for NatsIOEventStreamHandler {
    async fn get_stream_group_msgs(
        &self,
    ) -> Result<Vec<async_nats::jetstream::Message>, Box<dyn std::error::Error + Send + Sync>> {
        let mut batch = self
            .consumer
            .batch()
            .expires(Duration::from_millis(250))
            .messages()
            .await?;
        let mut messages = Vec::new();
        while let Some(Ok(message)) = batch.next().await {
            messages.push(message);
        }

        Ok(messages)
    }
}

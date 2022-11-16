use aruna_rust_api::api::{
    internal::v1::Relation,
    notification::services::v1::EventType,
    storage::{models::v1::ResourceType, services::v1::Hierarchy},
};
use async_nats::jetstream::Message;
use async_trait::async_trait;

#[async_trait]
pub trait EventHandler {
    async fn register_event(
        &self,
        resource_type: ResourceType,
        resource_id: String,
        event_type: EventType,
        relation: &Relation,
    ) -> Result<(), tonic::Status>;

    async fn create_stream_group(
        &self,
        stream_group_id: String,
        hierarchy: &Hierarchy,
        resource_type: ResourceType,
        resource_id: String,
        include_subresources: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn create_event_stream_handler(
        &self,
        stream_group_id: String,
    ) -> Result<Box<dyn EventStreamHandler + Send + Sync>, Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait]
pub trait EventStreamHandler {
    async fn get_stream_group_msgs(
        &self,
    ) -> Result<Vec<Message>, Box<dyn std::error::Error + Send + Sync>>;
}

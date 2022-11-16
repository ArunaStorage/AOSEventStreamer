use aruna_rust_api::api::{
    internal::v1::Relation,
    notification::services::v1::EventType,
    storage::{models::v1::ResourceType, services::v1::Hierarchy},
};
use async_nats::jetstream::Message;
use async_trait::async_trait;

// An Event handler is the main connection of the underlaying event message system like Nats.io
#[async_trait]
pub trait EventHandler {
    // Registers an event into the system
    async fn register_event(
        &self,
        resource_type: ResourceType,
        resource_id: String,
        event_type: EventType,
        relation: &Relation,
    ) -> Result<(), tonic::Status>;

    // Creates a stream group
    // A stream group is a entity of the underlaying event streaming system can be used
    // to load balance a set of incoming messages based on an individual query across multiple
    // client
    // This corresponds to a consumer in Nats.io Jetstream https://docs.nats.io/nats-concepts/jetstream
    async fn create_stream_group(
        &self,
        stream_group_id: String,
        hierarchy: &Hierarchy,
        resource_type: ResourceType,
        resource_id: String,
        include_subresources: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // Creates an event stream handler depending on th underlaying system
    // The handler is connected to a stream group to load-balance messages
    async fn create_event_stream_handler(
        &self,
        stream_group_id: String,
    ) -> Result<Box<dyn EventStreamHandler + Send + Sync>, Box<dyn std::error::Error + Send + Sync>>;
}

// An EventStreamHandler handles the message stream based on StreamGroups
#[async_trait]
pub trait EventStreamHandler {
    // Gets a batch of messages from the underlaying event system
    // This call expected to return after a certain timeout even if no messages are available
    // This is currently specific to nats.io and needs to be generalized
    // TODO: Generalize
    async fn get_stream_group_msgs(
        &self,
    ) -> Result<Vec<Message>, Box<dyn std::error::Error + Send + Sync>>;
}

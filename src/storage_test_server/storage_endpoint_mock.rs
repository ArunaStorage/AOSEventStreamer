use std::{collections::HashMap, sync::Mutex};

use aruna_rust_api::api::{
    internal::v1::{
        internal_authorize_service_server::InternalAuthorizeService,
        internal_event_service_server::InternalEventService, AuthorizeResponse,
        CreateStreamGroupResponse, GetStreamGroupResponse, StreamGroup,
    },
    storage::services::v1::{
        resource_info_service_server::ResourceInfoService, GetResourceHierarchyResponse, Hierarchy,
    },
};
use async_trait::async_trait;
use tonic::Response;

pub struct StorageEndpointMock {
    pub stream_groups: Mutex<HashMap<String, StreamGroup>>,
}

#[async_trait]
impl InternalEventService for StorageEndpointMock {
    async fn create_stream_group(
        &self,
        request: tonic::Request<aruna_rust_api::api::internal::v1::CreateStreamGroupRequest>,
    ) -> Result<
        tonic::Response<aruna_rust_api::api::internal::v1::CreateStreamGroupResponse>,
        tonic::Status,
    > {
        let inner_request = request.into_inner();

        let id = uuid::Uuid::new_v4();
        let stream_group = StreamGroup {
            event_type: inner_request.event_type,
            resource_type: inner_request.resource_type,
            resource_id: inner_request.resource_id,
            notify_on_sub_resource: inner_request.notify_on_sub_resource,
            id: id.to_string(),
        };

        let lock = self.stream_groups.lock();

        match lock {
            Ok(mut value) => value.insert(id.to_string(), stream_group.clone()),
            Err(_err) => return Err(tonic::Status::internal("error locking stream_group map")),
        };

        return Ok(Response::new(CreateStreamGroupResponse {
            stream_group: Some(stream_group),
        }));
    }

    async fn get_stream_group(
        &self,
        request: tonic::Request<aruna_rust_api::api::internal::v1::GetStreamGroupRequest>,
    ) -> Result<
        tonic::Response<aruna_rust_api::api::internal::v1::GetStreamGroupResponse>,
        tonic::Status,
    > {
        let inner_request = request.into_inner();
        let id = inner_request.stream_group_id.as_str();
        let stream_group = match self.stream_groups.lock() {
            Ok(value) => {
                let value = match value.get(id) {
                    Some(value) => value,
                    None => {
                        return Err(tonic::Status::invalid_argument("stream group id not found"))
                    }
                };
                value.to_owned()
            }
            Err(_) => return Err(tonic::Status::internal("error locking stream_group map")),
        };

        Ok(Response::new(GetStreamGroupResponse {
            stream_group: Some(stream_group),
        }))
    }

    async fn delete_stream_group(
        &self,
        _request: tonic::Request<aruna_rust_api::api::internal::v1::DeleteStreamGroupRequest>,
    ) -> Result<
        tonic::Response<aruna_rust_api::api::internal::v1::DeleteStreamGroupResponse>,
        tonic::Status,
    > {
        todo!()
    }

    async fn get_shared_revision(
        &self,
        _request: tonic::Request<aruna_rust_api::api::internal::v1::GetSharedRevisionRequest>,
    ) -> Result<
        tonic::Response<aruna_rust_api::api::internal::v1::GetSharedRevisionResponse>,
        tonic::Status,
    > {
        todo!()
    }
}

pub struct AuthzEndpointMock {}

#[async_trait]
impl InternalAuthorizeService for AuthzEndpointMock {
    async fn authorize(
        &self,
        _request: tonic::Request<aruna_rust_api::api::internal::v1::AuthorizeRequest>,
    ) -> Result<tonic::Response<aruna_rust_api::api::internal::v1::AuthorizeResponse>, tonic::Status>
    {
        return Ok(Response::new(AuthorizeResponse { ok: true }));
    }
}

pub struct ResourceInfoMock {}

#[async_trait]
impl ResourceInfoService for ResourceInfoMock {
    async fn get_resource_hierarchy(
        &self,
        request: tonic::Request<
            aruna_rust_api::api::storage::services::v1::GetResourceHierarchyRequest,
        >,
    ) -> Result<
        tonic::Response<aruna_rust_api::api::storage::services::v1::GetResourceHierarchyResponse>,
        tonic::Status,
    > {
        let inner_request = request.into_inner();
        let hierarchy = match inner_request.resource_type() {
            aruna_rust_api::api::storage::models::v1::ResourceType::Unspecified => todo!(),
            aruna_rust_api::api::storage::models::v1::ResourceType::Project => Hierarchy {
                project_id: "project_id".to_string(),
                ..Default::default()
            },
            aruna_rust_api::api::storage::models::v1::ResourceType::Collection => Hierarchy {
                project_id: "project_id".to_string(),
                collection_id: "collection_id".to_string(),
                ..Default::default()
            },
            aruna_rust_api::api::storage::models::v1::ResourceType::ObjectGroup => Hierarchy {
                project_id: "project_id".to_string(),
                collection_id: "collection_id".to_string(),
                object_id: "object_id".to_string(),
                ..Default::default()
            },
            aruna_rust_api::api::storage::models::v1::ResourceType::Object => Hierarchy {
                project_id: "project_id".to_string(),
                collection_id: "collection_id".to_string(),
                object_id: "object_id".to_string(),
                ..Default::default()
            },
            aruna_rust_api::api::storage::models::v1::ResourceType::All => todo!(),
        };

        return Ok(Response::new(GetResourceHierarchyResponse {
            hierarchies: vec![hierarchy],
        }));
    }
}

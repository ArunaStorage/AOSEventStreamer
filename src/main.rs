extern crate dotenv;
use dotenv::dotenv;
use std::{env, str::FromStr};

use async_nats::ServerAddr;
use server::server::EventServer;

use std::io::Write;

mod e2e;
mod server;
mod storage_test_server;
mod stream_handler;
mod utils;

#[tokio::main]
async fn main() {
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
        .filter_level(log::LevelFilter::Debug)
        .init();

    dotenv().ok().unwrap();
    let internal_event_token = env::var("INTERNAL_EVENT_TOKEN").unwrap();
    let nats_host = env::var("NATS_HOST").unwrap();
    let nats_port = env::var("NATS_PORT").unwrap();
    let event_service_client_host = env::var("EVENT_SERVICE").unwrap();
    let authz_service_client_host = env::var("AUTHZ_SERVICE").unwrap();
    let internal_event_service_host = env::var("INTERNAL_EVENT_SERVER_HOST").unwrap();
    let public_event_service_host = env::var("PUBLIC_EVENT_SERVER_HOST").unwrap();
    let resource_info_service_host = env::var("RESOURCE_INFO_SERVER_HOST").unwrap();

    let nats_addr = ServerAddr::from_str(format!("{}:{}", nats_host, nats_port).as_str()).unwrap();

    EventServer::start_server(
        internal_event_token,
        &[nats_addr],
        event_service_client_host,
        authz_service_client_host,
        internal_event_service_host,
        public_event_service_host,
        resource_info_service_host,
    )
    .await
    .unwrap();
}

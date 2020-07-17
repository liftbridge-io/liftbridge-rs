pub mod error;

mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

pub mod client {
    use crate::api::api_client::ApiClient;
    use crate::api::{CreateStreamRequest, DeleteStreamRequest, PauseStreamRequest};
    use crate::error::LiftbridgeError;
    use anyhow::Result;
    use std::convert::TryFrom;

    use std::time::Duration;

    use tonic::transport::{Channel, Endpoint};

    const MAX_BROKER_CONNS: i8 = 2;
    const KEEP_ALIVE_DURATION: Duration = Duration::from_secs(30);
    const RESUBSCRIBE_WAIT_TIME: Duration = Duration::from_secs(30);
    const ACK_WAIT_TIME: Duration = Duration::from_secs(5);

    pub struct Config {
        timeout: Option<Duration>,
    }

    //TODO: Make a builder for this
    // consider using https://github.com/colin-kiegel/rust-derive-builder
    #[derive(Default)]
    pub struct StreamOptions {
        pub group: String,
        pub replication_factor: i32,
        pub partitions: i32,
        pub retention_max_bytes: Option<i64>,
        pub retention_max_messages: Option<i64>,
        pub retention_max_age: Option<Duration>,
        pub cleaner_interval: Option<Duration>,
        pub segment_max_bytes: Option<i64>,
        pub segment_max_age: Option<Duration>,
        pub compact_max_goroutines: Option<i32>,
        pub compact_enabled: Option<bool>,
    }

    #[derive(Default)]
    pub struct PauseOptions {
        pub partitions: Vec<i32>,
        pub resume_all: bool,
    }

    pub struct Client {
        client: ApiClient<Channel>,
    }

    impl Client {
        pub async fn new(addrs: Vec<&str>) -> Result<Client> {
            let endpoints: Result<Vec<Endpoint>> = addrs
                .iter()
                .map(|addr| {
                    Endpoint::try_from(format!("grpc://{}", addr))
                        .map(|e| e.tcp_keepalive(Some(KEEP_ALIVE_DURATION)))
                        .map_err(|err| anyhow::Error::from(err))
                })
                .collect();
            let endpoints = endpoints?.into_iter();
            let channel = Channel::balance_list(endpoints);
            let client = ApiClient::new(channel);

            Ok(Client { client })
        }

        //TODO: This is subject to change as it needs to receive StreamOptions
        pub async fn create_stream(&mut self, subject: &str, name: &str) -> Result<()> {
            let req = tonic::Request::new(CreateStreamRequest {
                subject: subject.to_string(),
                name: name.to_string(),
                ..Default::default()
            });
            self.client
                .create_stream(req)
                .await
                .map_err(|err| match err.code() {
                    tonic::Code::AlreadyExists => LiftbridgeError::StreamExists { source: err },
                    _ => LiftbridgeError::from(err),
                })?;
            Ok(())
        }

        pub async fn delete_stream(&mut self, name: &str) -> Result<()> {
            let req = tonic::Request::new(DeleteStreamRequest {
                name: name.to_string(),
            });
            self.client
                .delete_stream(req)
                .await
                .map_err(|err| match err.code() {
                    tonic::Code::NotFound => LiftbridgeError::NoSuchStream { source: err },
                    _ => LiftbridgeError::from(err),
                })?;
            Ok(())
        }

        pub async fn pause_stream(&mut self, name: &str, options: PauseOptions) -> Result<()> {
            let req = tonic::Request::new(PauseStreamRequest {
                name: name.to_string(),
                partitions: options.partitions,
                resume_all: options.resume_all,
            });

            self.client
                .pause_stream(req)
                .await
                .map_err(|err| match err.code() {
                    tonic::Code::NotFound => LiftbridgeError::NoSuchPartition { source: err },
                    _ => LiftbridgeError::from(err),
                })?;

            Ok(())
        }
    }
}

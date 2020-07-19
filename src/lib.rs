pub mod error;

mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

pub mod client {
    use crate::api::api_client::ApiClient;
    use crate::api::{
        CreateStreamRequest, DeleteStreamRequest, Message, PauseStreamRequest, StartPosition,
        SubscribeRequest,
    };
    use crate::error::LiftbridgeError;
    use anyhow::Result;
    use std::convert::TryFrom;

    use std::time::{Duration, Instant};

    use chrono::Utc;
    use std::ops::Sub;
    use tonic::transport::{Channel, Endpoint};
    use tonic::Streaming;

    // as we are using http2 here instead of dealing in connection number
    // this is a default concurrency limit per broker connection
    const MAX_BROKER_CONCURRENCY: usize = 2;
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

    pub struct SubscriptionOptions {
        start_position: StartPosition,
        start_offset: i64,
        start_timestamp: i64,
        partition: i32,
        read_isr_replica: bool,
    }

    pub struct Subscription {
        stream: Streaming<Message>,
    }

    impl Subscription {
        pub async fn next(&mut self) -> Result<Option<Message>> {
            Ok(self.stream.message().await?)
        }
    }

    impl Default for SubscriptionOptions {
        fn default() -> Self {
            SubscriptionOptions {
                start_position: StartPosition::NewOnly,
                ..Default::default()
            }
        }
    }

    impl SubscriptionOptions {
        pub fn new() -> Self {
            SubscriptionOptions::default()
        }

        pub fn start_at_offset(mut self, offset: i64) -> Self {
            self.start_position = StartPosition::Offset;
            self.start_offset = offset;
            self
        }

        pub fn start_at_time(mut self, time: chrono::DateTime<Utc>) -> Self {
            self.start_position = StartPosition::Timestamp;
            self.start_timestamp = time.timestamp();
            self
        }

        pub fn start_at_time_delta(mut self, ago: Duration) -> Self {
            self.start_position = StartPosition::Timestamp;
            self.start_timestamp = Utc::now().timestamp() - ago.as_secs() as i64;
            self
        }

        pub fn start_at_earliest(mut self) -> Self {
            self.start_position = StartPosition::Earliest;
            self
        }

        pub fn partition(mut self, partition: u32) -> Self {
            self.partition = partition as i32;
            self
        }
    }

    impl Client {
        pub async fn new(addrs: Vec<&str>) -> Result<Client> {
            let endpoints: Result<Vec<Endpoint>> = addrs
                .iter()
                .map(|addr| {
                    Endpoint::try_from(format!("grpc://{}", addr))
                        .map(|e| {
                            e.tcp_keepalive(Some(KEEP_ALIVE_DURATION))
                                .concurrency_limit(MAX_BROKER_CONCURRENCY)
                        })
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

        pub async fn subscribe(
            &mut self,
            stream_name: &str,
            options: SubscriptionOptions,
        ) -> Result<Subscription> {
            let req = tonic::Request::new(SubscribeRequest {
                partition: options.partition,
                start_timestamp: options.start_timestamp,
                start_offset: options.start_offset,
                start_position: options.start_position.into(),
                stream: stream_name.to_string(),
                read_isr_replica: options.read_isr_replica,
            });
            let sub = self.client.subscribe(req).await?.into_inner();
            Ok(Subscription { stream: sub })
        }
    }
}

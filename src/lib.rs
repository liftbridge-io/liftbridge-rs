pub mod error;

mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
pub mod metadata {
    use crate::api::{Broker, FetchMetadataRequest, FetchMetadataResponse, StreamMetadata};
    use anyhow::Result;
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
    use std::sync::RwLock;
    use tonic::transport::Channel;

    #[derive(Default)]
    struct Metadata {
        last_updated: DateTime<Utc>,
        brokers: HashMap<String, Broker>,
        addrs: HashMap<String, Option<bool>>,
        streams: HashMap<String, StreamMetadata>,
    }

    impl Metadata {
        pub fn last_updated(&self) -> DateTime<Utc> {
            self.last_updated
        }
    }

    pub struct MetadataCache {
        metadata: RwLock<Metadata>,
    }

    impl MetadataCache {
        pub fn new() -> Self {
            MetadataCache {
                metadata: RwLock(Metadata::default()),
            }
        }

        pub async fn update(&mut self, client: &mut crate::client::Client) -> Result<()> {
            let resp = client._fetch_metadata().await?;
        }
    }
}

pub mod client {
    use crate::api::api_client::ApiClient;
    use crate::api::{
        AckPolicy, CreateStreamRequest, DeleteStreamRequest, FetchMetadataRequest,
        FetchMetadataResponse, Message, PauseStreamRequest, StartPosition, SubscribeRequest,
    };
    use crate::error::LiftbridgeError;
    use anyhow::Result;
    use std::convert::TryFrom;

    use std::time::Duration;

    use chrono::Utc;

    use crate::metadata::MetadataCache;
    use std::collections::HashMap;
    use std::sync::RwLock;
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

    enum Request {
        CreateStream(tonic::Request<CreateStreamRequest>),
        PauseStream(tonic::Request<PauseStreamRequest>),
        DeleteStream(tonic::Request<DeleteStreamRequest>),
        // Subscribe(tonic::Request<SubscribeRequest>),
        FetchMetadata(tonic::Request<FetchMetadataRequest>),
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

    pub struct Client<'a> {
        pool: RwLock<HashMap<String, ApiClient<Channel>>>,
        client: RwLock<ApiClient<Channel>>,
        metadata: MetadataCache,
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

    trait Partitioner {
        // fn partition(stream: &str, key: Vec<u8>, value: Vec<u8>, metadata: Option<Metadata>)
    }

    struct MessageOptions {
        // Key to set on the Message. If Liftbridge has stream compaction enabled,
        // the stream will retain only the last value for each key.
        key: Vec<u8>,

        // AckInbox sets the NATS subject Liftbridge should publish the Message ack
        // to. If this is not set, the server will generate a random inbox.
        ack_inbox: String,

        // correlation_id sets the identifier used to correlate an ack with the
        // published Message. If this is not set, the ack will not have a
        // correlation id.
        correlation_id: String,

        // ack_policy controls the behavior of Message acks sent by the server. By
        // default, Liftbridge will send an ack when the stream leader has written
        // the Message to its write-ahead log.
        ack_policy: AckPolicy,

        // Headers are key-value pairs to set on the Message.
        headers: HashMap<String, Vec<u8>>,

        // Partitioner specifies the strategy for mapping a Message to a stream
        // partition.
        partitioner: Partitioner,

        // Partition specifies the stream partition to publish the Message to. If
        // this is set, any Partitioner will not be used. This is a pointer to
        // allow distinguishing between unset and 0.
        partition: Option<i32>,
    }

    impl Client {
        pub async fn new(addrs: Vec<&str>) -> Result<Client> {
            let client = Client {
                pool: RwLock::new(HashMap::new()),
                metadata: MetadataCache::new(),
                client: RwLock(Client::connect(addrs).await?),
            };
            Ok(client)
        }

        async fn connect(addrs: Vec<&str>) -> Result<ApiClient<Channel>> {
            for addr in addrs.iter() {
                let endpoint = Endpoint::try_from(format!("grpc://{}", addr))
                    .map(|e| {
                        e.tcp_keepalive(Some(KEEP_ALIVE_DURATION))
                            .concurrency_limit(MAX_BROKER_CONCURRENCY)
                    })
                    .map_err(|err| anyhow::Error::from(err))?;
                let channel = endpoint.connect().await;
                match channel {
                    Ok(channel) => return Ok(ApiClient::new(channel)),
                    _ => continue,
                }
            }
            Err(LiftbridgeError::BrokersUnavailable)?
        }

        async fn request<T: prost::Message>(&mut self, msg: Request) -> Result<dyn prost::Message> {
            let client = self.client.get_mut().unwrap();
            match msg {
                Request::CreateStream(req) => {
                    client
                        .create_stream(req)
                        .await
                        .map_err(|err| match err.code() {
                            tonic::Code::AlreadyExists => {
                                LiftbridgeError::StreamExists { source: err }
                            }
                            _ => LiftbridgeError::from(err),
                        })?
                }
                Request::PauseStream(req) => {
                    client
                        .pause_stream(req)
                        .await
                        .map_err(|err| match err.code() {
                            tonic::Code::NotFound => {
                                LiftbridgeError::NoSuchPartition { source: err }
                            }
                            _ => LiftbridgeError::from(err),
                        })?
                }
                Request::DeleteStream(req) => {
                    client
                        .delete_stream(req)
                        .await
                        .map_err(|err| match err.code() {
                            tonic::Code::NotFound => LiftbridgeError::NoSuchStream { source: err },
                            _ => LiftbridgeError::from(err),
                        })?
                }
                Request::Subscribe(req) => client.subscribe(req),
                Request::FetchMetadata(req) => client.fetch_metadata(req),
            }
        }

        //TODO: This is subject to change as it needs to receive StreamOptions
        pub async fn create_stream(&mut self, subject: &str, name: &str) -> Result<()> {
            let req = tonic::Request::new(CreateStreamRequest {
                subject: subject.to_string(),
                name: name.to_string(),
                ..Default::default()
            });
            self.request(Request::CreateStream(req))?;
            Ok(())
        }

        pub async fn delete_stream(&mut self, name: &str) -> Result<()> {
            let req = tonic::Request::new(DeleteStreamRequest {
                name: name.to_string(),
            });
            self.request(Request::DeleteStream(req))?;
            Ok(())
        }

        pub async fn pause_stream(&mut self, name: &str, options: PauseOptions) -> Result<()> {
            let req = tonic::Request::new(PauseStreamRequest {
                name: name.to_string(),
                partitions: options.partitions,
                resume_all: options.resume_all,
            });

            self.request(Request::PauseStream(req))?;
            Ok(())
        }

        // pub async fn subscribe(
        //     &mut self,
        //     stream: &str,
        //     options: SubscriptionOptions,
        // ) -> Result<Subscription> {
        //     let req = tonic::Request::new(SubscribeRequest {
        //         partition: options.partition,
        //         start_timestamp: options.start_timestamp,
        //         start_offset: options.start_offset,
        //         start_position: options.start_position.into(),
        //         stream: stream.to_string(),
        //         read_isr_replica: options.read_isr_replica,
        //     });
        //     let sub = self.client.subscribe(req).await?.into_inner();
        //     Ok(Subscription { stream: sub })
        // }

        pub async fn fetch_metadata(&mut self) {}

        pub(crate) async fn _fetch_metadata(&mut self) -> Result<FetchMetadataResponse> {
            let req = tonic::Request::new(FetchMetadataRequest {
                streams: Vec::new(),
            });
            self.request(Request::FetchMetadata(req)).await as Result<FetchMetadataResponse>
        }

        // pub async fn publish(&mut self, stream: &str, message: Vec<u8>, M)
    }
}

pub mod error;
pub use error::{LiftbridgeError, Result};

mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
pub mod metadata {
    use crate::api::{Broker, FetchMetadataResponse, StreamMetadata};
    use crate::{LiftbridgeError, Result};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;
    use std::sync::{RwLock, RwLockReadGuard};

    struct Metadata {
        last_updated: DateTime<Utc>,
        brokers: HashMap<String, Broker>,
        streams: HashMap<String, StreamMetadata>,
    }

    impl Default for Metadata {
        fn default() -> Self {
            Metadata {
                last_updated: Utc::now(),
                ..Default::default()
            }
        }
    }

    impl Metadata {
        pub fn last_updated(&self) -> DateTime<Utc> {
            self.last_updated
        }

        fn get_addrs(&self) -> Vec<&str> {
            self.brokers
                .keys()
                .into_iter()
                .map(|k| k.as_str())
                .collect()
        }
    }

    pub struct MetadataCache<'a> {
        metadata: RwLock<Metadata>,
        bootstrap_addrs: Vec<&'a str>,
    }

    impl<'a> MetadataCache<'a> {
        pub fn new(addrs: Vec<&'a str>) -> Self {
            MetadataCache {
                metadata: RwLock::new(Metadata::default()),
                bootstrap_addrs: addrs,
            }
        }

        pub fn update(&self, resp: FetchMetadataResponse) {
            let mut brokers: HashMap<String, Broker> = HashMap::new();
            let mut streams: HashMap<String, StreamMetadata> = HashMap::new();

            for b in resp.brokers {
                let addr = format!("gprc://{}:{}", b.host, b.port);
                brokers.insert(addr, b);
            }

            for s in resp.metadata {
                streams.insert(s.name.clone(), s);
            }

            let mut metadata = self.metadata.write().unwrap();
            metadata.streams = streams;
            metadata.brokers = brokers;
            metadata.last_updated = Utc::now();
        }

        pub fn get_addrs(&self) -> Vec<&'a str> {
            let mut addrs = self.bootstrap_addrs.clone();
            addrs.extend(&self.get_addrs());
            addrs
        }

        pub fn get_addr(
            &self,
            stream: &str,
            partition: i32,
            read_isr_replica: bool,
        ) -> Result<String> {
            let metadata: RwLockReadGuard<Metadata> = self.metadata.read().unwrap();
            let stream = metadata
                .streams
                .get(stream)
                .ok_or(LiftbridgeError::NoSuchStream)?;
            let partition = stream
                .partitions
                .get(&partition)
                .ok_or(LiftbridgeError::NoSuchPartition)?;

            if read_isr_replica {
                let replicas = &partition.isr;
                //TODO: add rand
                return Ok(replicas.get(0).unwrap().clone());
            }

            if partition.leader.is_empty() {
                Err(LiftbridgeError::NoLeader)?
            }

            Ok(partition.leader.clone())
        }
    }
}

pub mod client {
    use crate::api::api_client::ApiClient;
    use crate::api::{
        AckPolicy, CreateStreamRequest, CreateStreamResponse, DeleteStreamRequest,
        DeleteStreamResponse, FetchMetadataRequest, FetchMetadataResponse, Message,
        PauseStreamRequest, PauseStreamResponse, StartPosition, SubscribeRequest,
    };
    use crate::{LiftbridgeError, Result};
    use std::convert::TryFrom;

    use std::time::Duration;

    use chrono::Utc;

    use crate::metadata::MetadataCache;

    use actix_rt::time::delay_for;
    use std::borrow::BorrowMut;
    use std::collections::HashMap;
    use std::sync::RwLock;
    use tonic::transport::{Channel, Endpoint};
    use tonic::{IntoRequest, Streaming};

    // as we are using http2 here instead of dealing in connection number
    // this is a default concurrency limit per broker connection
    const MAX_BROKER_CONCURRENCY: usize = 2;
    const KEEP_ALIVE_DURATION: Duration = Duration::from_secs(30);
    const RESUBSCRIBE_WAIT_TIME: Duration = Duration::from_secs(30);
    const ACK_WAIT_TIME: Duration = Duration::from_secs(5);

    pub struct Config {
        timeout: Option<Duration>,
    }

    #[derive(Clone)]
    enum Request {
        CreateStream(CreateStreamRequest),
        PauseStream(PauseStreamRequest),
        DeleteStream(DeleteStreamRequest),
        Subscribe(SubscribeRequest),
        FetchMetadata(FetchMetadataRequest),
    }

    enum Response {
        CreateStream(tonic::Response<CreateStreamResponse>),
        PauseStream(tonic::Response<PauseStreamResponse>),
        DeleteStream(tonic::Response<DeleteStreamResponse>),
        FetchMetadata(tonic::Response<FetchMetadataResponse>),
        Subscribe(tonic::Response<Streaming<Message>>),
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
        metadata: MetadataCache<'a>,
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
            Self::new()
        }
    }

    impl SubscriptionOptions {
        pub fn new() -> Self {
            SubscriptionOptions {
                start_position: StartPosition::NewOnly,
                ..Default::default()
            }
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
        partitioner: Box<dyn Partitioner>,

        // Partition specifies the stream partition to publish the Message to. If
        // this is set, any Partitioner will not be used. This is a pointer to
        // allow distinguishing between unset and 0.
        partition: Option<i32>,
    }

    impl<'a> Client<'a> {
        pub async fn new(addrs: Vec<&'a str>) -> Result<Client<'a>> {
            let client = Client {
                pool: RwLock::new(HashMap::new()),
                client: RwLock::new(Client::connect_any(&addrs).await?),
                metadata: MetadataCache::new(addrs),
            };
            Ok(client)
        }

        async fn change_broker(&self) -> Result<()> {
            let new_client = Client::connect_any(&self.metadata.get_addrs()).await?;
            let mut client = self.client.write().unwrap();
            *client = new_client;
            Ok(())
        }

        async fn connect_any(addrs: &Vec<&str>) -> Result<ApiClient<Channel>> {
            for addr in addrs.iter() {
                let client = Self::connect(addr).await;
                match client {
                    Ok(_) => return client,
                    _ => continue,
                }
            }
            Err(LiftbridgeError::BrokersUnavailable)?
        }

        async fn connect(addr: &str) -> Result<ApiClient<Channel>> {
            let endpoint = Endpoint::try_from(format!("grpc://{}", addr))
                .map(|e| {
                    e.tcp_keepalive(Some(KEEP_ALIVE_DURATION))
                        .concurrency_limit(MAX_BROKER_CONCURRENCY)
                })
                .unwrap();
            let channel = endpoint.connect().await;
            Ok(channel.map(|chan| ApiClient::new(chan))?)
        }

        async fn request(&self, msg: Request) -> Result<Response> {
            let mut client = self.client.write().unwrap();
            for _ in 0..9 {
                let res = Self::_request(client.borrow_mut(), &msg).await;
                match res {
                    Err(LiftbridgeError::GrpcError(status))
                        if status.code() == tonic::Code::Unavailable =>
                    {
                        self.change_broker().await?;
                        continue;
                    }
                    _ => return res,
                }
            }
            return Err(LiftbridgeError::BrokersUnavailable.into());
        }

        //TODO: We need to iterate the addresses if this fails to assign a new default broker
        async fn _request(client: &mut ApiClient<Channel>, msg: &Request) -> Result<Response> {
            // TODO: it would be nice to do without the clone
            let res = match msg.clone() {
                Request::CreateStream(req) => client
                    .create_stream(tonic::Request::new(req))
                    .await
                    .map_err(|err| match err.code() {
                        tonic::Code::AlreadyExists => LiftbridgeError::StreamExists,
                        _ => LiftbridgeError::from(err),
                    })
                    .map(Response::CreateStream)?,
                Request::PauseStream(req) => client
                    .pause_stream(tonic::Request::new(req))
                    .await
                    .map_err(|err| match err.code() {
                        tonic::Code::NotFound => LiftbridgeError::NoSuchPartition,
                        _ => LiftbridgeError::from(err),
                    })
                    .map(Response::PauseStream)?,
                Request::DeleteStream(req) => client
                    .delete_stream(tonic::Request::new(req))
                    .await
                    .map_err(|err| match err.code() {
                        tonic::Code::NotFound => LiftbridgeError::NoSuchStream,
                        _ => LiftbridgeError::from(err),
                    })
                    .map(Response::DeleteStream)?,
                //TODO: Sort out map_err types
                Request::Subscribe(req) => client
                    .subscribe(tonic::Request::new(req))
                    .await
                    .map(Response::Subscribe)?,
                Request::FetchMetadata(req) => client
                    .fetch_metadata(tonic::Request::new(req))
                    .await
                    .map_err(LiftbridgeError::from)
                    .map(Response::FetchMetadata)?,
            };
            Ok(res)
        }

        //TODO: This is subject to change as it needs to receive StreamOptions
        pub async fn create_stream(&self, subject: &str, name: &str) -> Result<()> {
            let req = CreateStreamRequest {
                subject: subject.to_string(),
                name: name.to_string(),
                ..Default::default()
            };
            self.request(Request::CreateStream(req)).await?;
            Ok(())
        }

        pub async fn delete_stream(&self, name: &str) -> Result<()> {
            let req = DeleteStreamRequest {
                name: name.to_string(),
            };
            self.request(Request::DeleteStream(req)).await?;
            Ok(())
        }

        pub async fn pause_stream(&self, name: &str, options: PauseOptions) -> Result<()> {
            let req = PauseStreamRequest {
                name: name.to_string(),
                partitions: options.partitions,
                resume_all: options.resume_all,
            };

            self.request(Request::PauseStream(req)).await?;
            Ok(())
        }

        pub async fn subscribe(
            &mut self,
            stream: &str,
            options: SubscriptionOptions,
        ) -> Result<Subscription> {
            let req = SubscribeRequest {
                partition: options.partition,
                start_timestamp: options.start_timestamp,
                start_offset: options.start_offset,
                start_position: options.start_position.into(),
                stream: stream.to_string(),
                read_isr_replica: options.read_isr_replica,
            };

            for i in 0..4 {
                let client = self
                    .get_conn(stream, options.partition, options.read_isr_replica)
                    .await;
                match client {
                    Err(_) => {
                        tokio::time::delay_for(Duration::from_millis(50)).await;
                        //TODO: Ignore the result of this, if it fails on all the retries
                        // this will result in a general failure
                        self.update_metadata().await;
                    }
                    Ok(client) => {
                        //TODO: check different error codes and retry as necessary
                        let sub = Self::_request(client, &Request::Subscribe(req.clone())).await;
                        match sub {
                            Ok(resp) => {
                                if let Response::Subscribe(resp) = resp {
                                    let mut sub = Subscription {
                                        stream: resp.into_inner(),
                                    };
                                    // if the subscription is a success - the server sends
                                    // an empty message first
                                    let msg = sub.next().await;
                                    match msg {
                                        Err(LiftbridgeError::GrpcError(status)) => {
                                            if status.code() == tonic::Code::FailedPrecondition {
                                                // ignore the result, this will result in general failure
                                                delay_for(Duration::from_millis(10 + i * 50)).await;
                                                self.update_metadata().await;
                                                continue;
                                            }

                                            if status.code() == tonic::Code::NotFound {
                                                Err(LiftbridgeError::NoSuchPartition)?
                                            }
                                        }
                                        _ => (),
                                    }
                                    return Ok(sub);
                                }
                                //This should never happen,
                                panic!("Subscribe request should return an appropriate response")
                            }
                            Err(LiftbridgeError::GrpcError(status))
                                if status.code() == tonic::Code::Unavailable =>
                            {
                                delay_for(Duration::from_millis(50)).await;
                                // ignore the result on purpose as we will fail eventually anyway
                                self.update_metadata().await;
                                continue;
                            }
                            _ => {
                                sub?;
                            }
                        }
                    }
                }
            }
            Err(LiftbridgeError::UnableToSubscribe)?
        }

        async fn update_metadata(&self) -> Result<()> {
            let resp = self._fetch_metadata().await?;
            self.metadata.update(resp);
            Ok(())
        }

        pub async fn fetch_metadata(&mut self) {}

        pub(crate) async fn _fetch_metadata(&self) -> Result<FetchMetadataResponse> {
            let req = FetchMetadataRequest {
                streams: Vec::new(),
            };
            self.request(Request::FetchMetadata(req))
                .await
                .map(|resp| match resp {
                    Response::FetchMetadata(r) => r.into_inner(),
                    _ => panic!("wrong response type"),
                })
        }

        async fn get_conn(
            &mut self,
            stream: &str,
            partition: i32,
            read_isr_replica: bool,
        ) -> Result<&mut ApiClient<Channel>> {
            let addr = self
                .metadata
                .get_addr(stream, partition, read_isr_replica)?;
            let mut pool = self.pool.get_mut().unwrap();
            if pool.contains_key(&addr) {
                return Ok(pool.get_mut(&addr).unwrap());
            }

            let client = Self::connect(&addr).await?;
            pool.insert(addr.clone(), client);
            Ok(pool.get_mut(&addr).unwrap())
        }
    }
}

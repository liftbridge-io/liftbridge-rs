pub mod error;
pub use error::{LiftbridgeError, Result};

mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}
pub mod metadata {
    use crate::api::{FetchMetadataResponse, StreamMetadata};
    use crate::{LiftbridgeError, Result};
    use chrono::{DateTime, Utc};
    use rand::Rng;
    use std::collections::HashMap;
    use std::sync::{RwLock, RwLockReadGuard};

    struct Metadata {
        last_updated: DateTime<Utc>,
        brokers: HashMap<String, String>,
        streams: HashMap<String, StreamMetadata>,
    }

    impl Default for Metadata {
        fn default() -> Self {
            Metadata {
                last_updated: Utc::now(),
                brokers: HashMap::new(),
                streams: HashMap::new(),
            }
        }
    }

    impl Metadata {
        pub fn last_updated(&self) -> DateTime<Utc> {
            self.last_updated
        }

        fn get_addrs(&self) -> Vec<String> {
            self.brokers
                .values()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        }
    }

    pub struct MetadataCache {
        metadata: RwLock<Metadata>,
        bootstrap_addrs: Vec<String>,
    }

    impl MetadataCache {
        pub fn new(addrs: Vec<String>) -> Self {
            MetadataCache {
                metadata: RwLock::new(Metadata::default()),
                bootstrap_addrs: addrs,
            }
        }

        pub fn update(&self, resp: FetchMetadataResponse) {
            let mut brokers: HashMap<String, String> = HashMap::new();
            let mut streams: HashMap<String, StreamMetadata> = HashMap::new();

            for b in resp.brokers {
                let addr = format!("{}:{}", b.host, b.port);
                brokers.insert(b.id, addr);
            }

            for s in resp.metadata {
                streams.insert(s.name.clone(), s);
            }

            let mut metadata = self.metadata.write().unwrap();
            metadata.streams = streams;
            metadata.brokers = brokers;
            metadata.last_updated = Utc::now();
        }

        pub fn get_addrs(&self) -> Vec<String> {
            let mut addrs = self.bootstrap_addrs.clone();
            let cached_addrs = &mut self.metadata.read().unwrap().get_addrs();

            addrs.append(cached_addrs);
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
                let replica = rand::thread_rng().gen_range(0, replicas.len());

                let replica = replicas.get(replica).unwrap();
                return Ok(metadata.brokers.get(replica).unwrap().clone());
            }

            if partition.leader.is_empty() {
                Err(LiftbridgeError::NoLeader)?
            }

            Ok(metadata.brokers.get(&partition.leader).unwrap().clone())
        }
    }
}

pub mod client {
    use crate::api::api_client::ApiClient;
    use crate::api::{
        AckPolicy, CreateStreamRequest, CreateStreamResponse, DeleteStreamRequest,
        DeleteStreamResponse, FetchMetadataRequest, FetchMetadataResponse, Message,
        PauseStreamRequest, PauseStreamResponse, PublishRequest, PublishResponse, StartPosition,
        SubscribeRequest,
    };
    use crate::{LiftbridgeError, Result};
    use std::convert::TryFrom;

    use std::time::Duration;

    use chrono::{DateTime, Datelike, Utc};

    use crate::metadata::MetadataCache;

    use rand::seq::SliceRandom;
    use rand::{thread_rng, Rng};
    use std::collections::HashMap;
    use std::ops::Add;
    use std::sync::RwLock;
    use tonic::transport::{Channel, Endpoint};
    use tonic::{Code, Status, Streaming};

    // To be implemented via load-balancing two endpoints connected to the same broker
    const MAX_BROKER_CONNECTIONS: usize = 2;
    const KEEP_ALIVE_DURATION: Duration = Duration::from_secs(30);
    const RESUBSCRIBE_WAIT_TIME: Duration = Duration::from_secs(30);
    const ACK_WAIT_TIME: Duration = Duration::from_secs(5);
    const NO_OFFSET: i64 = -1;

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
        Publish(PublishRequest),
    }

    enum Response {
        CreateStream(tonic::Response<CreateStreamResponse>),
        PauseStream(tonic::Response<PauseStreamResponse>),
        DeleteStream(tonic::Response<DeleteStreamResponse>),
        FetchMetadata(tonic::Response<FetchMetadataResponse>),
        Subscribe(tonic::Response<Streaming<Message>>),
        Publish(tonic::Response<PublishResponse>),
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
        //TODO: cleanup the pool on metadata update or pool access
        // to avoid leaking memory
        pool: RwLock<HashMap<String, ApiClient<Channel>>>,
        client: RwLock<ApiClient<Channel>>,
        metadata: MetadataCache,
    }

    #[derive(Clone)]
    pub struct SubscriptionOptions {
        start_position: StartPosition,
        start_offset: i64,
        start_timestamp: i64,
        partition: i32,
        read_isr_replica: bool,
    }

    pub struct Subscription<'a> {
        last_offset: i64,
        stream_name: &'a str,
        stream: Streaming<Message>,
        client: &'a Client,
        options: SubscriptionOptions,
    }

    impl<'a> Subscription<'a> {
        pub(crate) async fn _next(&mut self) -> Result<Option<Message>, Status> {
            self.stream.message().await
        }

        pub async fn next(&mut self) -> Result<Option<Message>> {
            let deadline: DateTime<Utc> =
                Utc::now() + chrono::Duration::from_std(RESUBSCRIBE_WAIT_TIME).unwrap();
            loop {
                if deadline.lt(&Utc::now()) {
                    break;
                }
                let msg: Result<Option<Message>, Status> = self._next().await;

                if let Ok(message) = msg {
                    return match message {
                        Some(msg) => {
                            self.last_offset = msg.offset;
                            Ok(Some(msg))
                        }
                        _ => Ok(message),
                    };
                }

                if let Err(err) = msg {
                    if err.code() == Code::Unavailable {
                        let mut options = self.options.clone();

                        if self.last_offset != NO_OFFSET {
                            options.start_offset = self.last_offset + 1;
                            options.start_position = StartPosition::Offset;
                        }

                        let sub = self
                            .client
                            .subscribe(self.stream_name, self.options.clone())
                            .await;
                        match sub {
                            Err(e) => {
                                if let LiftbridgeError::UnableToSubscribe = e {
                                    tokio::time::delay_for(
                                        Duration::from_secs(1)
                                            + Duration::from_millis(
                                                rand::thread_rng().gen_range(1, 500),
                                            ),
                                    )
                                    .await;
                                    continue;
                                }
                                Err(e)?
                            }
                            Ok(sub) => self.stream = sub.stream,
                        }
                        continue;
                    }
                    return Err(match err.code() {
                        Code::NotFound => LiftbridgeError::StreamDeleted,
                        Code::FailedPrecondition => LiftbridgeError::PartitionPaused,
                        _ => LiftbridgeError::GrpcError(err),
                    });
                }
            }

            return Err(LiftbridgeError::UnableToSubscribe);
        }
    }

    impl Default for SubscriptionOptions {
        fn default() -> Self {
            Self {
                start_position: StartPosition::NewOnly,
                start_offset: 0,
                start_timestamp: 0,
                read_isr_replica: false,
                partition: 0,
            }
        }
    }

    impl SubscriptionOptions {
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
        // partitioner: Box<dyn Partitioner>,

        // Partition specifies the stream partition to publish the Message to. If
        // this is set, any Partitioner will not be used. This is a pointer to
        // allow distinguishing between unset and 0.
        partition: Option<i32>,
    }

    impl Client {
        pub async fn new(addrs: Vec<&str>) -> Result<Client> {
            let mut addrs = addrs.into_iter().map(String::from).collect();
            let client = Client {
                pool: RwLock::new(HashMap::new()),
                client: RwLock::new(Client::connect_any(&mut addrs).await?),
                metadata: MetadataCache::new(addrs),
            };
            Ok(client)
        }

        async fn change_broker(&self) -> Result<()> {
            let new_client = Client::connect_any(&mut self.metadata.get_addrs()).await?;
            let mut client = self.client.write().unwrap();
            *client = new_client;
            Ok(())
        }

        async fn connect_any(addrs: &mut Vec<String>) -> Result<ApiClient<Channel>> {
            addrs.shuffle(&mut thread_rng());
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
                .map(|e| e.tcp_keepalive(Some(KEEP_ALIVE_DURATION)))
                .unwrap();
            let channel = endpoint.connect().await;
            Ok(channel.map(|chan| ApiClient::new(chan))?)
        }

        async fn request(&self, msg: Request) -> Result<Response> {
            for _ in 0..9 {
                let client = self.client.read().unwrap();
                let res = self._request(client.clone(), &msg).await;
                match res {
                    Err(LiftbridgeError::GrpcError(status))
                        if status.code() == tonic::Code::Unavailable =>
                    {
                        drop(client);
                        self.change_broker().await?;
                        continue;
                    }
                    _ => return res,
                }
            }
            return Err(LiftbridgeError::BrokersUnavailable.into());
        }

        async fn _request(
            &self,
            mut client: ApiClient<Channel>,
            msg: &Request,
        ) -> Result<Response> {
            // TODO: it would be nice to do without the clone
            let res = match msg.clone() {
                Request::Publish(req) => client.publish(req).await.map(Response::Publish)?,
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

        pub async fn subscribe<'a>(
            &'a self,
            stream: &'a str,
            options: SubscriptionOptions,
        ) -> Result<Subscription<'a>> {
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
                        let sub = self
                            ._request(client, &Request::Subscribe(req.clone()))
                            .await;
                        match sub {
                            Ok(resp) => {
                                if let Response::Subscribe(resp) = resp {
                                    let mut sub = Subscription {
                                        stream: resp.into_inner(),
                                        stream_name: stream,
                                        client: self,
                                        last_offset: NO_OFFSET,
                                        options: options.clone(),
                                    };
                                    // if the subscription is a success - the server sends
                                    // an empty message first
                                    let msg = sub._next().await;
                                    match msg {
                                        Err(status) => {
                                            if status.code() == tonic::Code::FailedPrecondition {
                                                // ignore the result, this will result in general failure
                                                tokio::time::delay_for(Duration::from_millis(
                                                    10 + i * 50,
                                                ))
                                                .await;
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
                                tokio::time::delay_for(Duration::from_millis(50)).await;
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

        pub async fn fetch_metadata(&self) {
            unimplemented!("fetch_metadata is currently not implemented")
        }

        //TODO: subject to change, need to support MessageOptions and proper Ack handling
        pub async fn publish(&self, stream: &str, value: Vec<u8>) -> Result<()> {
            let req = PublishRequest {
                value,
                stream: stream.to_string(),
                ack_policy: AckPolicy::Leader as i32,
                partition: 0,
                headers: HashMap::new(),
                ack_inbox: "".to_string(),
                correlation_id: "".to_string(),
                key: Vec::new(),
            };
            self.request(Request::Publish(req)).await?;
            Ok(())
        }

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
            &self,
            stream: &str,
            partition: i32,
            read_isr_replica: bool,
        ) -> Result<ApiClient<Channel>> {
            let addr = self
                .metadata
                .get_addr(stream, partition, read_isr_replica)?;

            let pool = self.pool.read().unwrap();
            if pool.contains_key(&addr) {
                return Ok(pool.get(&addr).unwrap().clone());
            }
            drop(pool);

            let mut pool = self.pool.write().unwrap();
            let client = Self::connect(&addr).await?;
            pool.insert(addr.clone(), client);
            Ok(pool.get(&addr).unwrap().clone())
        }
    }
}

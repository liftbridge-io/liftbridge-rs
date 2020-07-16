pub mod error;

pub mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

mod client {
    use crate::api::api_client::ApiClient;
    use crate::api::ActivityStreamOp::CreateStream;
    use crate::api::{CreateStreamRequest, CreateStreamResponse};
    use anyhow::{Context, Result};
    use std::convert::TryFrom;
    use std::sync::RwLock;
    use std::time::Duration;
    use tonic::codegen::Arc;
    use tonic::transport::Channel;

    const MAX_BROKER_CONNS: i8 = 2;
    const KEEP_ALIVE_DURATION: Duration = Duration::from_secs(30);
    const RESUBSCRIBE_WAIT_TIME: Duration = Duration::from_secs(30);
    const ACK_WAIT_TIME: Duration = Duration::from_secs(5);

    pub struct Config {
        timeout: Option<Duration>,
    }

    struct StreamOptions {
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

    pub struct Client {
        client: ApiClient<Channel>,
    }

    pub async fn new(uri: String) -> Result<Client> {
        // let timeout: Option<Duration> = timeout.into();
        let conn = tonic::transport::Endpoint::try_from(uri)?;
        let conn = conn.tcp_keepalive(Some(KEEP_ALIVE_DURATION));

        let conn = conn.connect().await?;
        Ok(Client {
            client: ApiClient::new(conn),
        })
    }

    impl Client {
        pub async fn create_stream(&mut self, subject: &str, name: &str) -> Result<()> {
            let req = tonic::Request::new(CreateStreamRequest {
                subject: subject.to_string(),
                name: name.to_string(),
                ..Default::default()
            });
            self.client.create_stream(req).await?;
            Ok(())
        }
    }
}

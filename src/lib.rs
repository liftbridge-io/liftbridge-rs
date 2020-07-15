pub mod error;

pub mod api {
    include!(concat!(env!("OUT_DIR"), "/proto.rs"));
}

mod client {
    use crate::api::api_client::ApiClient;
    use anyhow::{Context, Result};
    use std::time::Duration;
    use tonic::transport::Channel;

    pub struct Config {
        timeout: Option<Duration>,
    }

    pub struct Client {
        client: ApiClient<Channel>,
    }

    pub fn new(uri: &str, timeout: impl Into<Option<Duration>>) -> Result<Client> {
        let timeout: Option<Duration> = timeout.into();
        let conn = tonic::transport::Endpoint::new(uri)?;
        let conn = match timeout {
            None => conn,
            Some(timeout) => conn.timeout(timeout),
        };
        let conn = conn.connect()?;
        Ok(Client {
            client: ApiClient::new(conn),
        })
    }
}

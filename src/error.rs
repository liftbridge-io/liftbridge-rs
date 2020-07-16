use thiserror::Error;

#[derive(Error, Debug)]
pub enum LiftbridgeError {
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error("Stream already exists")]
    StreamExists { source: tonic::Status },
    #[error("Stream does not exist")]
    NoSuchStream { source: tonic::Status },
    #[error("Stream partition does not exist")]
    NoSuchPartition { source: tonic::Status },
    #[error("Stream has been deleted")]
    StreamDeleted { source: tonic::Status },
    #[error("Stream partition has been paused")]
    PartitionPaused,
    #[error("Publish ack timeout")]
    AckTimeout,
}

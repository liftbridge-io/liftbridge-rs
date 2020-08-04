use thiserror::Error;

#[derive(Error, Debug)]
pub enum LiftbridgeError {
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),
    #[error("Stream already exists")]
    StreamExists { source: tonic::Status },
    #[error("Stream does not exist")]
    NoSuchStream,
    #[error("Stream partition does not exist")]
    NoSuchPartition,
    #[error("Stream has been deleted")]
    StreamDeleted { source: tonic::Status },
    #[error("Stream partition has been paused")]
    PartitionPaused,
    #[error("Publish ack timeout")]
    AckTimeout,
    #[error("Can't connect to any of the specified brokers")]
    BrokersUnavailable,
    #[error("No known leader for partition")]
    NoLeader,
}

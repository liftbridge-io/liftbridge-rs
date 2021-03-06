use thiserror::Error;

pub type Result<T, E = LiftbridgeError> = core::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum LiftbridgeError {
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),
    #[error("Stream already exists")]
    StreamExists,
    #[error("Stream does not exist")]
    NoSuchStream,
    #[error("Stream partition does not exist")]
    NoSuchPartition,
    #[error("Stream has been deleted")]
    StreamDeleted,
    #[error("Stream partition has been paused")]
    PartitionPaused,
    #[error("Publish ack timeout")]
    AckTimeout,
    #[error("Can't connect to any of the specified brokers")]
    BrokersUnavailable,
    #[error("No known leader for partition")]
    NoLeader,
    #[error("Unable to subcribe")]
    UnableToSubscribe,
}

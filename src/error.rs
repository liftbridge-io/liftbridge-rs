use thiserror::Error;

#[derive(Error, Debug)]
pub enum LiftbridgeError {
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
}

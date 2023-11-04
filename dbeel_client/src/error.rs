use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

use rmpv::Utf8String;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    /// No addresses given to send a request to.
    #[error("No addresses given to send a request to")]
    NoAddresses,

    /// Failed to hash shard name.
    #[error("Failed to hash shard name: {0}")]
    HashShardName(std::io::Error),

    /// Failed to hash key.
    #[error("Failed to hash key: {0}")]
    HashKey(std::io::Error),

    /// Failed to parse to a socket address.
    #[error("Failed to parse to a socket address: {0}")]
    ParsingSocketAddress(std::io::Error),

    #[cfg(feature = "glommio")]
    /// Failed to connect to a shard.
    #[error("Failed to connect to a shard: {0}")]
    ConnectToShard(glommio::GlommioError<()>),

    #[cfg(feature = "tokio")]
    /// Failed to connect to a shard.
    #[error("Failed to connect to a shard: {0}")]
    ConnectToShard(tokio::io::Error),

    /// Failed to communicate with a shard.
    #[error("Failed to communicate with a shard: {0}")]
    CommunicateWithShard(std::io::Error),

    /// Failed to communicate with a shard, got timeout.
    #[error("Failed to communicate with a shard, got timeout")]
    CommunicateWithShardTimeout,

    /// Failed to communicate with remote cluster.
    /// Holds a Vec to each error that happened while trying to communicate
    /// with each shard.
    #[error("Failed to communicate with remote cluster: {0}")]
    SendRequestToCluster(VecError),

    /// Value given is not a valid utf8 string.
    #[error("Value given is not a valid utf8 string")]
    InvalidUtf8String(Utf8String),

    /// Failed to decode a msgpack buffer.
    #[error("Failed to decode a msgpack buffer")]
    MsgpackDecodeError(#[from] rmpv::decode::Error),

    /// Failed to encode to a msgpack buffer.
    #[error("Failed to encode to a msgpack buffer")]
    MsgpackEncodeError(#[from] rmpv::encode::Error),

    /// Failed to decode a msgpack buffer.
    #[error("Failed to decode a msgpack buffer")]
    MsgpackSerdeDecodeError(#[from] rmp_serde::decode::Error),

    /// Failed to encode to a msgpack buffer.
    #[error("Failed to encode to a msgpack buffer")]
    MsgpackSerdeEncodeError(#[from] rmp_serde::encode::Error),

    #[cfg(feature = "glommio")]
    /// Failed to set timeout on a socket.
    #[error("Failed to set timeout on a socket: {0}")]
    SetTimeout(glommio::GlommioError<()>),

    /// Server error.
    #[error("Server error ({0}): {1}")]
    ServerErr(String, String),
}

#[derive(Debug)]
pub struct VecError(pub Vec<Error>);

impl Deref for VecError {
    type Target = Vec<Error>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VecError {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Display for VecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        self.0.iter().enumerate().fold(Ok(()), |result, (i, e)| {
            result.and_then(|_| {
                if i == self.0.len() - 1 {
                    write!(f, "{}", e)
                } else {
                    write!(f, "{}, ", e)
                }
            })
        })?;
        write!(f, "]")
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

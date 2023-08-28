use rmpv::Utf8String;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    /// No addresses given to send a request to.
    #[error("No addresses given to send a request to")]
    NoAddresses,

    /// Failed to hash shard name.
    #[error("Failed to hash shard name")]
    HashShardName(std::io::Error),

    /// Failed to hash key.
    #[error("Failed to hash key")]
    HashKey(std::io::Error),

    /// Failed to parse to a socket address.
    #[error("Failed to parse to a socket address")]
    ParsingSocketAddress(std::io::Error),

    /// Failed to connect to a shard.
    #[error("Failed to connect to a shard")]
    ConnectToShard(glommio::GlommioError<()>),

    /// Failed to communicate with a shard.
    #[error("Failed to communicate with a shard")]
    CommunicateWithShard(std::io::Error),

    /// Failed to communicate with remote cluster.
    /// Holds a Vec to each error that happened while trying to communicate
    /// with each shard.
    #[error("Failed to communicate with remote cluster")]
    SendRequestToCluster(Vec<Error>),

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

    /// Failed to set timeout on a socket.
    #[error("Failed to set timeout on a socket")]
    SetTimeout(glommio::GlommioError<()>),
}

pub type Result<T> = ::std::result::Result<T, Error>;

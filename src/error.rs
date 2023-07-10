use async_channel::{RecvError, SendError};
use thiserror::Error;

use crate::messages::ShardPacket;

#[derive(Error, Debug)]
pub enum Error {
    #[error("creating the '{pattern}' regex failed")]
    RegexCreationError {
        source: regex::Error,
        pattern: String,
    },

    #[error(transparent)]
    GlommioError(#[from] glommio::GlommioError<()>),
    #[error(transparent)]
    StdIOError(#[from] std::io::Error),
    #[error(transparent)]
    BincodeSerdeError(#[from] bincode::Error),
    #[error(transparent)]
    RedBlackTreeError(#[from] redblacktree::Error),

    #[error(transparent)]
    ShardReceiverError(#[from] RecvError),
    #[error(transparent)]
    ShardPacketSenderError(#[from] SendError<ShardPacket>),

    #[error("timed out")]
    Timeout,

    #[error("response type not expected")]
    ResponseWrongType,
    #[error("response failed with: '{0}'")]
    ResponseError(String),
    #[error("no remote shards received from asking all seed nodes")]
    NoRemoteShardsFoundInSeedNodes,

    #[error("field '{0}' is missing")]
    MissingField(String),
    #[error("unsupported field '{0}'")]
    UnsupportedField(String),
    #[error("field '{0}' is of a bad type")]
    BadFieldType(String),
    #[error("collection '{0}' not found")]
    CollectionNotFound(String),
    #[error("collection '{0}' already exists")]
    CollectionAlreadyExists(String),
    #[error("key not found")]
    KeyNotFound,
    #[error("msgpack decode failed")]
    MsgpackDecodeError(#[from] rmpv::decode::Error),
    #[error("msgpack encode failed")]
    MsgpackEncodeError(#[from] rmpv::encode::Error),
}

pub type Result<T> = ::std::result::Result<T, Error>;

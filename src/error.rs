use async_channel::{RecvError, SendError};
use kinded::Kinded;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::messages::ShardPacket;

#[derive(Error, Debug, Kinded)]
#[kinded(display = "snake_case", derive(Serialize, Deserialize))]
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
    RedBlackTreeError(#[from] rbtree_arena::Error),

    #[error(transparent)]
    ShardReceiverError(#[from] RecvError),
    #[error(transparent)]
    ShardPacketSenderError(#[from] SendError<ShardPacket>),
    #[error(transparent)]
    ShardEmptySenderError(#[from] SendError<()>),

    #[error("timed out")]
    Timeout,
    #[error("shard stopped")]
    ShardStopped,

    #[error("response type not expected")]
    ResponseWrongType,
    #[error("response '{0}' failed with: '{1}'")]
    ResponseError(ErrorKind, String),
    #[error("no remote shards received from asking all seed nodes")]
    NoRemoteShardsFoundInSeedNodes,

    #[error("shard doesn't own the key, maybe you need to update your cluster metadata")]
    KeyNotOwnedByShard,

    #[error("field '{0}' is missing")]
    MissingField(String),
    #[error("field '{0}' is not a u16")]
    FieldNotU16(String),
    #[error("unsupported field '{0}'")]
    UnsupportedField(String),
    #[error("field '{0}' is of a bad type")]
    BadFieldType(String),
    #[error("collection '{0}' not found")]
    CollectionNotFound(String),
    #[error("collection '{0}' already exists")]
    CollectionAlreadyExists(String),
    #[error("item too large")]
    ItemTooLarge,
    #[error("key not found")]
    KeyNotFound,
    #[error("msgpack decode failed")]
    MsgpackDecodeError(#[from] rmpv::decode::Error),
    #[error("msgpack encode failed")]
    MsgpackEncodeError(#[from] rmpv::encode::Error),
    #[error("msgpack serde decode failed")]
    MsgpackSerdeDecodeError(#[from] rmp_serde::decode::Error),
    #[error("msgpack serde encode failed")]
    MsgpackSerdeEncodeError(#[from] rmp_serde::encode::Error),
}

pub type Result<T> = ::std::result::Result<T, Error>;

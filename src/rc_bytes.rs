use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    ops::{Deref, DerefMut},
    rc::Rc,
};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone)]
pub struct RcBytes(pub Rc<Vec<u8>>);

impl RcBytes {
    pub fn new() -> Self {
        Self(Rc::new(Vec::new()))
    }
}

impl Deref for RcBytes {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Into<RcBytes> for Vec<u8> {
    fn into(self) -> RcBytes {
        RcBytes(Rc::new(self))
    }
}

impl Into<RcBytes> for Rc<Vec<u8>> {
    fn into(self) -> RcBytes {
        RcBytes(self)
    }
}

impl Hash for RcBytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Ord for RcBytes {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for RcBytes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RcBytes {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for RcBytes {}

impl Serialize for RcBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for RcBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RcBytesVisitor;

        impl<'de> de::Visitor<'de> for RcBytesVisitor {
            type Value = RcBytes;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter,
            ) -> std::fmt::Result {
                formatter.write_str("byte array")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(RcBytes(Rc::new(v.to_vec())))
            }
        }

        deserializer.deserialize_bytes(RcBytesVisitor)
    }
}

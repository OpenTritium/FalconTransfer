//! UID，内部是 uuidv7 ，在外部做的 base58 序列化增加可读性
use std::{
    fmt::{self, Display},
    ops::Deref,
    str::FromStr,
};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(Uuid);

impl PeerId {
    #[inline]
    pub fn random_now() -> Self { PeerId(Uuid::now_v7()) }
}

impl Deref for PeerId {
    type Target = Uuid;

    #[inline]
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = bs58::encode(self.0.as_bytes()).into_string();
        write!(f, "{s}")
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Bs58DecodeError(#[from] bs58::decode::Error),
    #[error(transparent)]
    UuidError(#[from] uuid::Error),
}

impl FromStr for PeerId {
    type Err = Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = bs58::decode(s).into_vec()?;
        let uuid = Uuid::from_slice(&bytes)?;
        Ok(PeerId(uuid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_round_trip() {
        let original_peer_id = PeerId::random_now();
        println!("Original PeerId: {}", original_peer_id);
        let peer_id_str = original_peer_id.to_string();
        println!("Encoded to string: {}", peer_id_str);
        let decoded_peer_id = PeerId::from_str(&peer_id_str).unwrap();
        println!("Decoded from string: {}", decoded_peer_id);
        assert_eq!(original_peer_id, decoded_peer_id);
    }

    #[test]
    fn test_static_value() {
        let known_uuid = Uuid::from_bytes([
            0x01, 0x99, 0x55, 0x54, 0x19, 0x83, 0x74, 0x73, 0xa7, 0x80, 0xfe, 0x49, 0xa8, 0xb1, 0xa2, 0x86,
        ]);
        let peer_id = PeerId(known_uuid);
        let expected_base58 = "CTD7wJ78MNcQHbsadNUEZ";

        assert_eq!(peer_id.to_string(), expected_base58);
        assert_eq!(PeerId::from_str(expected_base58).unwrap(), peer_id);
    }

    #[test]
    fn test_invalid_input() {
        assert!(PeerId::from_str("This is not a valid base58 string!").is_err());
        assert!(PeerId::from_str("short").is_err());
    }
}

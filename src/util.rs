pub mod peer_id_serde {
    use std::str::FromStr;

    use libp2p::PeerId;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(peer_id: &PeerId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&peer_id.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PeerId::from_str(&s).map_err(serde::de::Error::custom)
    }
}

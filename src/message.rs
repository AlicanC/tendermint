use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::util::peer_id_serde;

pub type ProposalId = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub id: ProposalId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    #[serde(with = "peer_id_serde")]
    pub sender: PeerId,
    pub content: MessageContent,
}

impl Message {
    pub fn from_slice(slice: &[u8]) -> Result<Self, bincode::error::DecodeError> {
        bincode::serde::decode_from_slice(slice, bincode::config::standard()).map(|x| x.0)
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, bincode::error::EncodeError> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageContent {
    Proposal {
        height: u64,
        round: u64,
        proposal: Proposal,
        valid_round: Option<u64>,
    },
    Prevote {
        height: u64,
        round: u64,
        proposal: Option<ProposalId>,
    },
    Precommit {
        height: u64,
        round: u64,
        proposal: Option<ProposalId>,
    },
}

impl MessageContent {
    pub fn height(&self) -> u64 {
        match self {
            MessageContent::Proposal { height, .. } => *height,
            MessageContent::Prevote { height, .. } => *height,
            MessageContent::Precommit { height, .. } => *height,
        }
    }

    pub fn round(&self) -> u64 {
        match self {
            MessageContent::Proposal { round, .. } => *round,
            MessageContent::Prevote { round, .. } => *round,
            MessageContent::Precommit { round, .. } => *round,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity::Keypair;

    #[tokio::test]
    async fn serde() {
        let alice = Keypair::ed25519_from_bytes([1; 32]).unwrap();

        let message = Message {
            sender: PeerId::from(alice.public()),
            content: MessageContent::Proposal {
                height: 1,
                round: 1,
                proposal: Proposal { id: 1 },
                valid_round: None,
            },
        };

        let encoded = message.to_vec().unwrap();
        let decoded = Message::from_slice(&encoded).unwrap();

        assert_eq!(message, decoded);
    }
}

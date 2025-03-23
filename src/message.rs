use serde::{Deserialize, Serialize};

pub type ProposalId = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub id: ProposalId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub sender: String,
    pub content: MessageContent,
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

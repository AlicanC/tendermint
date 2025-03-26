#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::collapsible_if)]

use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use libp2p::{PeerId, identity::Keypair};
use tokio::{
    select,
    sync::{Mutex, broadcast},
    task,
    time::{self},
};

use crate::{
    genesis::Genesis,
    message::{Message, MessageContent, Proposal},
    validator_set::ValidatorSet,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsensusStep {
    Propose,
    Prevote,
    Precommit,
}

pub type NodeId = String;
pub type Height = u64;

#[derive(Debug, Clone)]
pub struct ConsensusState {
    pub height: Height,
    pub round: u64,
    pub step: ConsensusStep,
    pub decision: HashMap<Height, Proposal>,
    pub locked_value: Option<Proposal>,
    pub locked_round: Option<u64>,
    pub valid_value: Option<Proposal>,
    pub valid_round: Option<u64>,
    pub message_log: Vec<Message>,
    pub propose_handled: bool,
    pub prevote_handled: bool,
    pub precommit_handled: bool,
}

impl ConsensusState {
    fn new() -> Self {
        ConsensusState {
            height: 0,
            round: 0,
            step: ConsensusStep::Propose,
            decision: HashMap::new(),
            locked_value: None,
            locked_round: None,
            valid_value: None,
            valid_round: None,
            message_log: Vec::new(),
            propose_handled: false,
            prevote_handled: false,
            precommit_handled: false,
        }
    }
}

pub struct Consensus {
    id: NodeId,
    keypair: Keypair,
    validator_set: ValidatorSet,

    pub call_tx: flume::Sender<ConsensusCall>,
    call_rx: flume::Receiver<ConsensusCall>,
    pub event_tx: broadcast::Sender<ConsensusEvent>,
    pub event_rx: broadcast::Receiver<ConsensusEvent>,
    publish_tx: flume::Sender<Message>,
    publish_rx: flume::Receiver<Message>,

    pub state: Mutex<ConsensusState>,
}

impl Consensus {
    pub fn new(id: NodeId, keypair: Keypair, genesis: Genesis) -> Result<Self, Box<dyn Error>> {
        let validator_set = ValidatorSet::new(
            genesis
                .validators
                .iter()
                .map(|v| (v.public_key.to_peer_id(), v.voting_power))
                .collect(),
        );

        let (call_tx, call_rx) = flume::bounded(16);
        let (event_tx, event_rx) = broadcast::channel(16);
        let (publish_tx, publish_rx) = flume::bounded(16);

        Ok(Consensus {
            id,
            keypair,
            validator_set,
            call_tx,
            call_rx,
            event_tx,
            event_rx,
            publish_tx,
            publish_rx,
            state: Mutex::new(ConsensusState::new()),
        })
    }

    fn proposer(&self, height: u64, round: u64) -> PeerId {
        self.validator_set.items
            [(height as usize + round as usize) % self.validator_set.items.len()]
        .0
    }

    fn get_value(&self) -> Proposal {
        Proposal { id: 0 }
    }

    fn valid(&self, _proposal: &Proposal) -> bool {
        true
    }

    fn timeout_propose(&self, round: u64) -> Duration {
        Duration::from_millis(1000 * (round + 1))
    }

    fn timeout_prevote(&self, round: u64) -> Duration {
        Duration::from_millis(1000 * (round + 1))
    }

    fn timeout_precommit(&self, round: u64) -> Duration {
        Duration::from_millis(1000 * (round + 1))
    }

    async fn start_round(self: &Arc<Self>, round: u64) {
        let mut state = self.state.lock().await;
        tracing::info!(
            "{}: starting round {} at height {}",
            self.id,
            round,
            state.height,
        );
        self.event_tx
            .send(ConsensusEvent::StartingRound(state.height, state.round))
            .unwrap();
        state.round = round;
        state.step = ConsensusStep::Propose;
        if self.keypair.public().to_peer_id() == self.proposer(state.height, round) {
            let proposal = if let Some(value) = state.valid_value.clone() {
                value
            } else {
                self.get_value()
            };
            let height = state.height;
            let valid_round = state.valid_round;
            drop(state);
            self.broadcast(MessageContent::Proposal {
                height,
                round,
                proposal,
                valid_round,
            })
            .await;
        } else {
            let height = state.height;
            let duration = self.timeout_propose(round);
            let self_ = self.clone();
            task::spawn(async move {
                time::sleep(duration).await;
                self_.on_timeout_propose(height, round).await;
            });
        }
    }

    async fn on_timeout_propose(self: Arc<Self>, height: u64, round: u64) {
        let mut state = self.state.lock().await;
        if height == state.height && round == state.round && state.step == ConsensusStep::Propose {
            tracing::info!("{}: timeout propose", self.id);
            self.broadcast(MessageContent::Prevote {
                height,
                round,
                proposal: None,
            })
            .await;
            state.step = ConsensusStep::Prevote;
        }
    }

    async fn on_timeout_prevote(self: Arc<Self>, height: u64, round: u64) {
        let mut state = self.state.lock().await;
        if height == state.height && round == state.round && state.step == ConsensusStep::Prevote {
            tracing::info!("{}: timeout prevote", self.id);
            self.broadcast(MessageContent::Precommit {
                height,
                round,
                proposal: None,
            })
            .await;
            state.step = ConsensusStep::Precommit;
        }
    }

    async fn on_timeout_precommit(self: &Arc<Self>, height: u64, round: u64) {
        let state = self.state.lock().await;
        if height == state.height && round == state.round {
            tracing::info!("{}: timeout precommit", self.id);
            drop(state);
            self.start_round(round + 1).await;
        }
    }

    async fn broadcast(self: &Arc<Self>, message_content: MessageContent) {
        let message = Message {
            sender: self.keypair.public().to_peer_id(),
            content: message_content,
        };
        tracing::info!("{}: broadcasting: {:?}", self.id, message.content);
        self.publish_tx.send(message.clone()).unwrap();
        let mut state = self.state.lock().await;
        state.message_log.push(message);
    }

    async fn handle_message(self: &Arc<Self>, message: Message) {
        let f = self.validator_set.f();

        let state = self.state.lock().await.clone();

        // See: https://arxiv.org/pdf/1807.04938#page=6

        // 22: upon {PROPOSAL, h_p, round_p, v, −1} from proposer(h_p, round_p) while step_p = propose do
        if state.step == ConsensusStep::Propose {
            if let Some(Message {
                content: MessageContent::Proposal { proposal, .. },
                ..
            }) = state.message_log.iter().find(|m| match m.content {
                MessageContent::Proposal {
                    height,
                    round,
                    valid_round: None,
                    ..
                } if height == state.height
                    && round == state.round
                    && m.sender == self.proposer(state.height, state.round) =>
                {
                    true
                }
                _ => false,
            }) {
                tracing::info!("{}: handling 22...", self.id);
                if self.valid(proposal)
                    && (state.locked_round.is_none()
                        || state.locked_value == Some(proposal.clone()))
                {
                    self.broadcast(MessageContent::Prevote {
                        height: state.height,
                        round: state.round,
                        proposal: Some(proposal.id),
                    })
                    .await;
                } else {
                    self.broadcast(MessageContent::Prevote {
                        height: state.height,
                        round: state.round,
                        proposal: None,
                    })
                    .await;
                }
                let mut state = self.state.lock().await;
                state.step = ConsensusStep::Prevote;
            }
        }

        // 28: upon {PROPOSAL, h_p, round_p, v, vr} from proposer(h_p, round_p) AND 2f + 1 {PREVOTE, h_p, vr, id(v)} while step_p = propose ∧ (vr ≥ 0 ∧ vr < round_p) do
        if state.step == ConsensusStep::Propose {
            if let Some(Message {
                content:
                    MessageContent::Proposal {
                        proposal,
                        valid_round: Some(valid_round),
                        ..
                    },
                ..
            }) = state.message_log.iter().find(|m| match m.content {
                MessageContent::Proposal {
                    height,
                    round,
                    valid_round: Some(valid_round),
                    ..
                } if height == state.height
                    && round == state.round
                    && valid_round < state.round
                    && m.sender == self.proposer(state.height, state.round) =>
                {
                    true
                }
                _ => false,
            }) {
                tracing::info!("{}: handling 28...", self.id);
                if self.valid(proposal)
                    && ((state.locked_round.is_none()
                        || state.locked_round.unwrap() < *valid_round)
                        || state.locked_value == Some(proposal.clone()))
                {
                    self.broadcast(MessageContent::Prevote {
                        height: state.height,
                        round: state.round,
                        proposal: Some(proposal.id),
                    })
                    .await;
                } else {
                    self.broadcast(MessageContent::Prevote {
                        height: state.height,
                        round: state.round,
                        proposal: None,
                    })
                    .await;
                }
                let mut state = self.state.lock().await;
                state.step = ConsensusStep::Prevote;
            }
        }

        // 34: upon 2f + 1 {PREVOTE, h_p, round_p, ∗} while step_p = prevote for the first time do
        if state.step == ConsensusStep::Prevote && !state.prevote_handled {
            if state
                .message_log
                .iter()
                .filter(|m| m.content.height() == state.height && m.content.round() == state.round)
                .map(|m| self.validator_set.power_of(&m.sender))
                .sum::<u64>()
                > 2 * f + 1
            {
                tracing::info!("{}: handling 34 prevote...", self.id);
                let height = state.height;
                let round = state.round;
                let duration = self.timeout_prevote(round);
                let self_ = self.clone();
                task::spawn(async move {
                    time::sleep(duration).await;
                    self_.on_timeout_prevote(height, round).await;
                });
                let mut state = self.state.lock().await;
                state.prevote_handled = true;
            }
        }

        // 36: upon {PROPOSAL, h_p, round_p, v, ∗} from proposer(h_p, round_p) AND 2f + 1 {PREVOTE, h_p, round_p, id(v)} while valid(v) ∧ step_p ≥ prevote for the first time do
        if (state.step == ConsensusStep::Prevote || state.step == ConsensusStep::Precommit)
            && !state.propose_handled
        {
            if let Some(Message {
                content: MessageContent::Proposal { proposal, .. },
                ..
            }) = state.message_log.iter().find(|m| match m.content {
                MessageContent::Proposal { height, round, .. }
                    if height == state.height
                        && round == state.round
                        && m.sender == self.proposer(state.height, state.round) =>
                {
                    true
                }
                _ => false,
            }) {
                if state
                    .message_log
                    .iter()
                    .filter(|m| {
                        m.content
                            == MessageContent::Prevote {
                                height: state.height,
                                round: state.round,
                                proposal: Some(proposal.id),
                            }
                    })
                    .map(|m| self.validator_set.power_of(&m.sender))
                    .sum::<u64>()
                    > 2 * f + 1
                    && self.valid(proposal)
                {
                    tracing::info!("{}: handling 36 proposal...", self.id);
                    let height = state.height;
                    let round = state.round;
                    if state.step == ConsensusStep::Prevote {
                        let mut state = self.state.lock().await;
                        state.locked_value = Some(proposal.clone());
                        state.locked_round = Some(round);
                        drop(state);
                        self.broadcast(MessageContent::Precommit {
                            height,
                            round,
                            proposal: Some(proposal.id),
                        })
                        .await;
                        let mut state = self.state.lock().await;
                        state.step = ConsensusStep::Precommit;
                    }
                    let mut state = self.state.lock().await;
                    state.valid_value = Some(proposal.clone());
                    state.valid_round = Some(round);
                    state.propose_handled = true;
                }
            }
        }

        // 44: upon 2f + 1 {PREVOTE, h_p, round_p, nil} while step_p = prevote do
        if state.step == ConsensusStep::Prevote {
            if state
                .message_log
                .iter()
                .filter(|m| {
                    m.content
                        == MessageContent::Prevote {
                            height: state.height,
                            round: state.round,
                            proposal: None,
                        }
                })
                .map(|m| self.validator_set.power_of(&m.sender))
                .sum::<u64>()
                > 2 * f + 1
            {
                tracing::info!("{}: handling 44...", self.id);
                let height = state.height;
                let round = state.round;
                self.broadcast(MessageContent::Precommit {
                    height,
                    round,
                    proposal: None,
                })
                .await;
                let mut state = self.state.lock().await;
                state.step = ConsensusStep::Precommit;
            }
        }

        // 47: upon 2f + 1 {PRECOMMIT, h_p, round_p, ∗} for the first time do
        if !state.precommit_handled {
            if state
                .message_log
                .iter()
                .filter(|m| m.content.height() == state.height && m.content.round() == state.round)
                .map(|m| self.validator_set.power_of(&m.sender))
                .sum::<u64>()
                > 2 * f + 1
            {
                tracing::info!("{}: handling 47 precommit...", self.id);
                let height = state.height;
                let round = state.round;
                let duration = self.timeout_precommit(round);
                let self_ = self.clone();
                task::spawn(async move {
                    time::sleep(duration).await;
                    self_.on_timeout_precommit(height, round).await;
                });
                let mut state = self.state.lock().await;
                state.precommit_handled = true;
            }
        }

        // 49: upon {PROPOSAL, h_p, r, v, ∗} from proposer(h_p, r) AND 2f + 1 {PRECOMMIT, hp, r, id(v)} while decision_p[h_p] = nil do
        if !state.decision.contains_key(&state.height) {
            for message in state.message_log.iter().filter(|m| match m.content {
                MessageContent::Proposal { height, round, .. }
                    if height == state.height && m.sender == self.proposer(state.height, round) =>
                {
                    true
                }
                _ => false,
            }) {
                if let Message {
                    content:
                        MessageContent::Proposal {
                            proposal, round, ..
                        },
                    ..
                } = message
                {
                    if state
                        .message_log
                        .iter()
                        .filter(|m| {
                            m.content
                                == MessageContent::Prevote {
                                    height: state.height,
                                    round: *round,
                                    proposal: Some(proposal.id),
                                }
                        })
                        .map(|m| self.validator_set.power_of(&m.sender))
                        .sum::<u64>()
                        > 2 * f + 1
                    {
                        tracing::info!("{}: handling 49...", self.id);
                        if self.valid(proposal) {
                            let height = state.height;
                            let mut state = self.state.lock().await;
                            state.decision.insert(height, proposal.clone());
                            state.height += 1;
                            state.locked_round = None;
                            state.locked_value = None;
                            state.valid_round = None;
                            state.valid_value = None;
                            state.message_log.clear();
                            state.prevote_handled = false;
                            state.propose_handled = false;
                            state.precommit_handled = false;
                            drop(state);
                            self.start_round(0).await;
                        }
                    }
                }
            }
        }

        // 55: upon f + 1 {∗, h_p, round, ∗, ∗} with round > round_p do
        if message.content.height() == state.height
            && state
                .message_log
                .iter()
                .filter(|m| m.content.height() == state.height)
                .map(|m| self.validator_set.power_of(&m.sender))
                .sum::<u64>()
                > f + 1
            && message.content.round() > state.round
        {
            tracing::info!("{}: handling 55...", self.id);
            self.start_round(message.content.round()).await;
        }
    }

    pub async fn run(self: &Arc<Self>) -> Result<(), Box<dyn Error>> {
        tracing::info!("{}: running...", self.id);

        loop {
            select! {
                Ok(call) = self.call_rx.recv_async() => {
                    match call {
                        ConsensusCall::Start => {
                            tracing::info!("{}: starting...", self.id);
                            self.start_round(0).await;
                        }
                        ConsensusCall::Handle(message) => {
                            let mut state = self.state.lock().await;
                            state.message_log.push(message.clone());
                            drop(state);
                            self.handle_message(message).await;
                        }
                        ConsensusCall::Stop => {
                            tracing::info!("{}: stopping...", self.id);
                            break Ok(());
                        }
                    }
                }
                Ok(message) = self.publish_rx.recv_async() => {
                    self.event_tx.send(ConsensusEvent::Broadcast(message)).unwrap();
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConsensusEvent {
    StartingRound(Height, u64),
    Broadcast(Message),
}

#[derive(Debug)]
pub enum ConsensusCall {
    Start,
    Handle(Message),
    Stop,
}

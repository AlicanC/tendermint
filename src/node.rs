#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::collapsible_if)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    select,
    sync::{Mutex, broadcast},
    task,
    time::{self, sleep},
};

use crate::message::{Message, MessageContent, Proposal};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Propose,
    Prevote,
    Precommit,
}

pub type NodeId = &'static str;
pub type Height = u64;

#[derive(Debug, Clone)]
pub struct NodeState {
    pub height: Height,
    pub round: u64,
    pub step: Step,
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

impl NodeState {
    fn new() -> Self {
        NodeState {
            height: 0,
            round: 0,
            step: Step::Propose,
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

pub struct Node {
    id: NodeId,
    validator_set: ValidatorSet,
    message_tx: broadcast::Sender<Message>,
    pub call_tx: flume::Sender<NodeCall>,
    call_rx: flume::Receiver<NodeCall>,
    pub event_tx: broadcast::Sender<NodeEvent>,
    pub event_rx: broadcast::Receiver<NodeEvent>,

    pub state: Mutex<NodeState>,
}

impl Node {
    pub fn new(
        id: NodeId,
        validator_set: ValidatorSet,
        message_tx: broadcast::Sender<Message>,
    ) -> Self {
        let (call_tx, call_rx) = flume::bounded(16);
        let (event_tx, event_rx) = broadcast::channel(16);
        Node {
            id,
            validator_set,
            message_tx,
            call_tx,
            call_rx,
            event_tx,
            event_rx,
            state: Mutex::new(NodeState::new()),
        }
    }

    fn proposer(&self, height: u64, round: u64) -> &'static str {
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
        println!(
            "{}: starting round {} at height {}",
            self.id, round, state.height,
        );
        state.round = round;
        state.step = Step::Propose;
        if self.id == self.proposer(state.height, round) {
            let proposal = if let Some(value) = state.valid_value.clone() {
                value
            } else {
                self.get_value()
            };
            self.broadcast(MessageContent::Proposal {
                height: state.height,
                round,
                proposal,
                valid_round: state.valid_round,
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

    async fn on_timeout_propose(&self, height: u64, round: u64) {
        let mut state = self.state.lock().await;
        if height == state.height && round == state.round && state.step == Step::Propose {
            println!("{}: timeout propose", self.id);
            self.broadcast(MessageContent::Prevote {
                height,
                round,
                proposal: None,
            })
            .await;
            state.step = Step::Prevote;
        }
    }

    async fn on_timeout_prevote(&self, height: u64, round: u64) {
        let mut state = self.state.lock().await;
        if height == state.height && round == state.round && state.step == Step::Prevote {
            println!("{}: timeout prevote", self.id);
            self.broadcast(MessageContent::Precommit {
                height,
                round,
                proposal: None,
            })
            .await;
            state.step = Step::Precommit;
        }
    }

    async fn on_timeout_precommit(self: &Arc<Self>, height: u64, round: u64) {
        let state = self.state.lock().await;
        if height == state.height && round == state.round {
            println!("{}: timeout precommit", self.id);
            drop(state);
            self.start_round(round + 1).await;
        }
    }

    async fn broadcast(&self, message_content: MessageContent) {
        let message = Message {
            sender: self.id,
            content: message_content,
        };
        println!("{}: broadcasting: {:?}", self.id, message.content);
        self.message_tx.send(message).unwrap();
    }

    async fn handle_message(self: &Arc<Self>, message: Message) {
        let f = self.validator_set.f();

        let state = self.state.lock().await.clone();

        // 22: upon {PROPOSAL, h_p, round_p, v, −1} from proposer(h_p, round_p) while step_p = propose do
        if state.step == Step::Propose {
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
                println!("{}: handling 22...", self.id);
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
                state.step = Step::Prevote;
            }
        }

        // 28: upon {PROPOSAL, h_p, round_p, v, vr} from proposer(h_p, round_p) AND 2f + 1 {PREVOTE, h_p, vr, id(v)} while step_p = propose ∧ (vr ≥ 0 ∧ vr < round_p) do
        if state.step == Step::Propose {
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
                println!("{}: handling 28...", self.id);
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
                state.step = Step::Prevote;
            }
        }

        // 34: upon 2f + 1 {PREVOTE, h_p, round_p, ∗} while step_p = prevote for the first time do
        if state.step == Step::Prevote && !state.prevote_handled {
            if state
                .message_log
                .iter()
                .filter(|m| m.content.height() == state.height && m.content.round() == state.round)
                .map(|m| self.validator_set.power_of(m.sender))
                .sum::<u64>()
                > 2 * f + 1
            {
                println!("{}: handling 34 prevote...", self.id);
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
        if (state.step == Step::Prevote || state.step == Step::Precommit) && !state.propose_handled
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
                    .map(|m| self.validator_set.power_of(m.sender))
                    .sum::<u64>()
                    > 2 * f + 1
                    && self.valid(proposal)
                {
                    println!("{}: handling 36 proposal...", self.id);
                    let height = state.height;
                    let round = state.round;
                    if state.step == Step::Prevote {
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
                        state.step = Step::Precommit;
                    }
                    let mut state = self.state.lock().await;
                    state.valid_value = Some(proposal.clone());
                    state.valid_round = Some(round);
                    state.propose_handled = true;
                }
            }
        }

        // 44: upon 2f + 1 {PREVOTE, h_p, round_p, nil} while step_p = prevote do
        if state.step == Step::Prevote {
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
                .map(|m| self.validator_set.power_of(m.sender))
                .sum::<u64>()
                > 2 * f + 1
            {
                println!("{}: handling 44...", self.id);
                let height = state.height;
                let round = state.round;
                self.broadcast(MessageContent::Precommit {
                    height,
                    round,
                    proposal: None,
                })
                .await;
                let mut state = self.state.lock().await;
                state.step = Step::Precommit;
            }
        }

        // 47: upon 2f + 1 {PRECOMMIT, h_p, round_p, ∗} for the first time do
        if !state.precommit_handled {
            if state
                .message_log
                .iter()
                .filter(|m| m.content.height() == state.height && m.content.round() == state.round)
                .map(|m| self.validator_set.power_of(m.sender))
                .sum::<u64>()
                > 2 * f + 1
            {
                println!("{}: handling 47 precommit...", self.id);
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
                        .map(|m| self.validator_set.power_of(m.sender))
                        .sum::<u64>()
                        > 2 * f + 1
                    {
                        println!("{}: handling 49...", self.id);
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
                .map(|m| self.validator_set.power_of(m.sender))
                .sum::<u64>()
                > f + 1
            && message.content.round() > state.round
        {
            println!("{}: handling 55...", self.id);
            self.start_round(message.content.round()).await;
        }
    }

    pub async fn run(self: Arc<Self>) {
        println!("{}: running...", self.id);

        let mut message_rx = self.message_tx.subscribe();

        self.event_tx.send(NodeEvent::Ready).unwrap();

        loop {
            select! {
                Ok(message) = message_rx.recv() => {
                    let mut state = self.state.lock().await;
                    state.message_log.push(message.clone());
                    drop(state);
                    if message.sender != self.id {
                        println!("{}: received message from {}: {:?}", self.id, message.sender, message.content);
                    }
                    self.handle_message(message).await;
                    sleep(Duration::from_millis(0)).await;
                }
                Ok(call) = self.call_rx.recv_async() => {
                    match call {
                        NodeCall::Start => {
                            println!("{}: starting...", self.id);
                            self.start_round(0).await;
                        }
                        NodeCall::Stop => {
                            println!("{}: stopping...", self.id);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeEvent {
    Ready,
}

#[derive(Debug)]
pub enum NodeCall {
    Start,
    Stop,
}

#[derive(Clone, Debug)]
pub struct ValidatorSet {
    items: Vec<(NodeId, u64)>,
}

impl ValidatorSet {
    pub fn new(items: Vec<(NodeId, u64)>) -> ValidatorSet {
        ValidatorSet { items }
    }

    pub fn total_power(&self) -> u64 {
        self.items.iter().map(|(_, power)| *power).sum()
    }

    pub fn power_of(&self, id: NodeId) -> u64 {
        self.items
            .iter()
            .find(|(node_id, _)| *node_id == id)
            .map(|(_, power)| *power)
            .unwrap_or(0)
    }

    pub fn f(&self) -> u64 {
        (self.total_power() - 1) / 3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn validator_set() {
        let validator_set = ValidatorSet::new(vec![("Alice", 100), ("Bob", 100), ("Charlie", 100)]);

        let n = validator_set.total_power();
        assert_eq!(n, 300);
        let f = validator_set.f();
        assert_eq!(f, (n - 1) / 3);
    }
}

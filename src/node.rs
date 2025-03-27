#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::collapsible_if)]

use std::{error::Error, sync::Arc};

use flume::SendError;
use libp2p::{PeerId, identity::Keypair};
use tokio::{
    select,
    sync::broadcast,
    task::{self, JoinHandle},
};

use crate::{
    consensus::{Consensus, ConsensusCall, ConsensusEvent, Height, NodeId},
    genesis::Genesis,
    p2p::{P2p, P2pCall, P2pEvent},
};

pub struct Node {
    id: NodeId,

    call_tx: flume::Sender<NodeCall>,
    call_rx: flume::Receiver<NodeCall>,
    event_tx: broadcast::Sender<NodeEvent>,
    #[allow(dead_code)]
    event_rx: broadcast::Receiver<NodeEvent>,

    p2p: Arc<P2p>,
    pub consensus: Arc<Consensus>,
}

impl Node {
    pub fn new(id: NodeId, keypair: Keypair, genesis: Genesis) -> Result<Self, Box<dyn Error>> {
        let (call_tx, call_rx) = flume::bounded(16);
        let (event_tx, event_rx) = broadcast::channel(16);

        let p2p = P2p::new(&keypair)?;
        let consensus = Consensus::new(id.clone(), keypair.clone(), genesis)?;

        Ok(Node {
            id,
            call_tx,
            call_rx,
            event_tx,
            event_rx,
            p2p: Arc::new(p2p),
            consensus: Arc::new(consensus),
        })
    }

    pub fn call(&self, call: NodeCall) -> Result<(), SendError<NodeCall>> {
        self.call_tx.send(call)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<NodeEvent> {
        self.event_tx.subscribe()
    }

    pub async fn run(&self) -> Result<JoinHandle<()>, Box<dyn Error>> {
        tracing::info!("{}: running...", self.id);

        self.p2p.run().await?;
        self.consensus.run().await?;

        let id = self.id.clone();
        let call_rx = self.call_rx.clone();
        let event_tx = self.event_tx.clone();
        let p2p = self.p2p.clone();
        let consensus = self.consensus.clone();

        Ok(task::spawn(async move {
            let mut p2p_event_rx = p2p.subscribe();
            let mut consensus_event_rx = consensus.subscribe();

            loop {
                select! {
                    Ok(call) = call_rx.recv_async() => {
                        match call {
                            NodeCall::Start => {
                                tracing::info!("{}: starting...", id);
                                consensus.call(ConsensusCall::Start).unwrap();
                            }
                            NodeCall::Stop => {
                                tracing::info!("{}: stopping...", id);
                                p2p.call(P2pCall::Stop).unwrap();
                                consensus.call(ConsensusCall::Stop).unwrap();
                                break;
                            }
                        }
                    }
                    Ok(consensus_event) = consensus_event_rx.recv() => {
                        match consensus_event {
                            ConsensusEvent::Broadcast(message) => {
                                p2p.call(P2pCall::Publish(message)).unwrap();
                            }
                            ConsensusEvent::StartingRound(height, round) => {
                                event_tx.send(NodeEvent::StartingRound(height, round)).unwrap();
                            }
                        }
                    }
                    Ok(p2p_event) = p2p_event_rx.recv() => {
                        match p2p_event {
                            P2pEvent::Discovered(peer_id) => {
                                event_tx.send(NodeEvent::Discovered(peer_id)).unwrap();
                            }
                            P2pEvent::Subscribed => {
                                event_tx.send(NodeEvent::Subscribed).unwrap();
                            }
                            P2pEvent::Received(message) => {
                                consensus.call(ConsensusCall::Handle(message)).unwrap();
                            }
                        }
                    }
                }
                task::yield_now().await;
            }
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeEvent {
    Discovered(PeerId),
    Subscribed,
    StartingRound(Height, u64),
}

#[derive(Debug)]
pub enum NodeCall {
    Start,
    Stop,
}

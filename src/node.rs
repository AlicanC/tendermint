#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::collapsible_if)]

use std::{error::Error, sync::Arc, time::Duration};

use libp2p::{PeerId, identity::Keypair};
use tokio::{
    select,
    sync::{Mutex, broadcast},
    task,
    time::sleep,
};

use crate::{
    consensus::{Consensus, ConsensusCall, ConsensusEvent, Height, NodeId},
    genesis::Genesis,
    p2p::{P2p, P2pCall, P2pEvent},
};

pub struct Node {
    id: NodeId,

    pub call_tx: flume::Sender<NodeCall>,
    call_rx: flume::Receiver<NodeCall>,
    pub event_tx: broadcast::Sender<NodeEvent>,
    pub event_rx: broadcast::Receiver<NodeEvent>,

    p2p: Mutex<P2p>,
    pub consensus: Mutex<Arc<Consensus>>,
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
            p2p: Mutex::new(p2p),
            consensus: Mutex::new(Arc::new(consensus)),
        })
    }

    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn Error>> {
        tracing::info!("{}: running...", self.id);

        let p2p = self.p2p.lock().await;
        let p2p_call_tx = p2p.call_tx.clone();
        let mut p2p_event_rx = p2p.event_tx.subscribe();
        drop(p2p);
        let self_ = self.clone();
        task::spawn(async move {
            let mut p2p = self_.p2p.lock().await;
            p2p.run().await.unwrap();
        });

        let consensus = self.consensus.lock().await;
        let consensus_call_tx = consensus.call_tx.clone();
        let mut consensus_event_rx = consensus.event_tx.subscribe();
        drop(consensus);
        let self_ = self.clone();
        task::spawn(async move {
            let consensus = self_.consensus.lock().await;
            consensus.run().await.unwrap();
        });

        loop {
            select! {
                Ok(call) = self.call_rx.recv_async() => {
                    match call {
                        NodeCall::Start => {
                            tracing::info!("{}: starting...", self.id);
                            consensus_call_tx.send(ConsensusCall::Start).unwrap();
                        }
                        NodeCall::Stop => {
                            tracing::info!("{}: stopping...", self.id);
                            p2p_call_tx.send(P2pCall::Stop).unwrap();
                            consensus_call_tx.send(ConsensusCall::Stop).unwrap();
                            break Ok(());
                        }
                    }
                }
                Ok(consensus_event) = consensus_event_rx.recv() => {
                    match consensus_event {
                        ConsensusEvent::Broadcast(message) => {
                            p2p_call_tx.send(P2pCall::Publish(message)).unwrap();
                        }
                        ConsensusEvent::StartingRound(height, round) => {
                            self.event_tx.send(NodeEvent::StartingRound(height, round)).unwrap();
                        }
                    }
                }
                Ok(p2p_event) = p2p_event_rx.recv() => {
                    match p2p_event {
                        P2pEvent::Discovered(peer_id) => {
                            self.event_tx.send(NodeEvent::Discovered(peer_id)).unwrap();
                        }
                        P2pEvent::Subscribed => {
                            self.event_tx.send(NodeEvent::Subscribed).unwrap();
                        }
                        P2pEvent::Received(message) => {
                            consensus_call_tx.send(ConsensusCall::Handle(message)).unwrap();
                            sleep(Duration::from_millis(0)).await;
                        }
                    }
                }
            }
        }
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

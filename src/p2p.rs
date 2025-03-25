use std::{
    error::Error,
    hash::{DefaultHasher, Hash, Hasher},
    time::Duration,
};

use libp2p::{
    PeerId, Swarm, SwarmBuilder,
    futures::StreamExt,
    gossipsub,
    identity::Keypair,
    mdns, noise,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use tokio::{io, select, sync::broadcast};

use crate::message::Message;

pub struct P2p {
    swarm: Swarm<P2pSwarmBehaviour>,
    pub call_tx: flume::Sender<P2pCall>,
    call_rx: flume::Receiver<P2pCall>,
    pub event_tx: broadcast::Sender<P2pEvent>,
    pub event_rx: broadcast::Receiver<P2pEvent>,
}

impl P2p {
    pub fn new(keypair: &Keypair) -> Result<Self, Box<dyn Error>> {
        let (call_tx, call_rx) = flume::bounded(16);
        let (event_tx, event_rx) = broadcast::channel(16);

        let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default(),
                noise::Config::new,
                yamux::Config::default,
            )?
            .with_quic()
            .with_behaviour(|key| {
                let message_id_fn = |message: &gossipsub::Message| {
                    let mut s = DefaultHasher::new();
                    message.data.hash(&mut s);
                    gossipsub::MessageId::from(s.finish().to_string())
                };

                let gossipsub_config = gossipsub::ConfigBuilder::default()
                    .heartbeat_interval(Duration::from_secs(10))
                    .validation_mode(gossipsub::ValidationMode::Strict)
                    .message_id_fn(message_id_fn)
                    .build()
                    .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?;

                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )?;

                let mdns = mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    key.public().to_peer_id(),
                )?;
                Ok(P2pSwarmBehaviour { gossipsub, mdns })
            })?
            .build();

        Ok(P2p {
            call_tx,
            call_rx,
            event_tx,
            event_rx,
            swarm,
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        let swarm = &mut self.swarm;

        let topic = gossipsub::IdentTopic::new("consensus");
        swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

        swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

        loop {
            select! {
                Ok(call) = self.call_rx.recv_async() => {
                    match call {
                        P2pCall::Publish(message) => {
                            let encoded_message =
                            bincode::serde::encode_to_vec(&message, bincode::config::standard()).unwrap();
                            let encoded_message = message.to_vec().unwrap();
                            swarm
                                .behaviour_mut()
                                .gossipsub
                                .publish(topic.clone(), encoded_message)
                                .unwrap();
                        }
                        P2pCall::Stop => {
                            break Ok(());
                        }
                    }
                }
                event = swarm.select_next_some() => match event {
                    SwarmEvent::Behaviour(P2pSwarmBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, _multiaddr) in list {
                            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                            self.event_tx.send(P2pEvent::Discovered(peer_id)).unwrap();
                        }
                    },
                    SwarmEvent::Behaviour(P2pSwarmBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                        for (peer_id, _multiaddr) in list {
                            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        }
                    },
                    SwarmEvent::Behaviour(P2pSwarmBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed { .. })) => {
                        self.event_tx.send(P2pEvent::Subscribed).unwrap();
                    },
                    SwarmEvent::Behaviour(P2pSwarmBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                        message,
                        ..
                    })) => {
                        let message: Message = bincode::serde::decode_from_slice(&message.data, bincode::config::standard()).unwrap().0;
                        self.event_tx.send(P2pEvent::Received(message)).unwrap();
                        // sleep(Duration::from_millis(0)).await;
                    },
                    _ => {}
                },
            }
        }
    }
}

#[derive(NetworkBehaviour)]
pub struct P2pSwarmBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum P2pCall {
    Publish(Message),
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum P2pEvent {
    Discovered(PeerId),
    Subscribed,
    Received(Message),
}

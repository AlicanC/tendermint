use std::error::Error;

use libp2p::{PeerId, identity::Keypair};
use tendermint::{
    genesis::{Genesis, Validator},
    node::{Node, NodeCall, NodeEvent},
};
use tokio::task;

#[derive(Clone)]
struct TestActor {
    id: String,
    keypair: Keypair,
}

impl TestActor {
    fn new(id: impl Into<String>) -> Result<TestActor, Box<dyn Error>> {
        let id: String = id.into();
        let mut secret_bytes = id.as_bytes().to_vec();
        while secret_bytes.len() < 32 {
            secret_bytes.push(0);
        }
        let keypair = Keypair::ed25519_from_bytes(secret_bytes)?;
        Ok(TestActor { id, keypair })
    }

    pub fn peer_id(&self) -> PeerId {
        self.keypair.public().to_peer_id()
    }
}

#[tokio::test]
async fn consensus() -> Result<(), Box<dyn Error>> {
    // Setup
    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("tendermint=info".parse()?)
                .from_env_lossy(),
        )
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let alice = TestActor::new("Alice  ")?;
    let bob = TestActor::new("Bob    ")?;
    let charlie = TestActor::new("Charlie")?;

    let genesis = Genesis {
        validators: vec![
            Validator::new(alice.keypair.public().clone(), 100),
            Validator::new(bob.keypair.public().clone(), 100),
            Validator::new(charlie.keypair.public().clone(), 100),
        ],
    };

    // Create nodes
    let alice_node = Node::new(alice.id.clone(), alice.keypair.clone(), genesis.clone())?;
    let bob_node = Node::new(bob.id.clone(), bob.keypair.clone(), genesis.clone())?;
    let charlie_node = Node::new(charlie.id.clone(), charlie.keypair.clone(), genesis.clone())?;

    // Run nodes
    let alice_handle = alice_node.run().await?;
    let bob_handle = bob_node.run().await?;
    let charlie_handle = charlie_node.run().await?;

    // Wait for nodes to be ready
    println!("Waiting for nodes to discover each other...");
    let alice_ready_handle = {
        let mut alice_event_rx = alice_node.subscribe();
        let bob = bob.clone();
        let charlie = charlie.clone();
        task::spawn(async move {
            let mut discovered_bob = false;
            let mut discovered_charlie = false;
            let mut subscribed = false;
            while let Ok(event) = alice_event_rx.recv().await {
                if event == NodeEvent::Discovered(bob.peer_id()) {
                    discovered_bob = true;
                }
                if event == NodeEvent::Discovered(charlie.peer_id()) {
                    discovered_charlie = true;
                }
                if event == NodeEvent::Subscribed {
                    subscribed = true;
                }
                if subscribed && discovered_bob && discovered_charlie {
                    break;
                }
            }
        })
    };
    let bob_ready_handle = {
        let mut bob_event_rx = bob_node.subscribe();
        let alice = alice.clone();
        let charlie = charlie.clone();
        task::spawn(async move {
            let mut discovered_alice = false;
            let mut discovered_charlie = false;
            let mut subscribed = false;
            while let Ok(event) = bob_event_rx.recv().await {
                if event == NodeEvent::Discovered(alice.peer_id()) {
                    discovered_alice = true;
                }
                if event == NodeEvent::Discovered(charlie.peer_id()) {
                    discovered_charlie = true;
                }
                if event == NodeEvent::Subscribed {
                    subscribed = true;
                }
                if subscribed && discovered_alice && discovered_charlie {
                    break;
                }
            }
        })
    };
    let charlie_ready_handle = {
        let mut charlie_event_rx = charlie_node.subscribe();
        let alice = alice.clone();
        let bob = bob.clone();
        task::spawn(async move {
            let mut discovered_alice = false;
            let mut discovered_bob = false;
            let mut subscribed = false;
            while let Ok(event) = charlie_event_rx.recv().await {
                if event == NodeEvent::Discovered(alice.peer_id()) {
                    discovered_alice = true;
                }
                if event == NodeEvent::Discovered(bob.peer_id()) {
                    discovered_bob = true;
                }
                if event == NodeEvent::Subscribed {
                    subscribed = true;
                }
                if subscribed && discovered_alice && discovered_bob {
                    break;
                }
            }
        })
    };

    alice_ready_handle.await?;
    bob_ready_handle.await?;
    charlie_ready_handle.await?;

    // Send start calls
    println!("Sending start calls...");
    alice_node.call(NodeCall::Start).unwrap();
    bob_node.call(NodeCall::Start).unwrap();
    charlie_node.call(NodeCall::Start).unwrap();

    // Wait for Alice to start a round at height 3
    println!("Waiting for Alice to start a round at height 3...");
    let mut alice_event_rx = alice_node.subscribe();
    while let Ok(event) = alice_event_rx.recv().await {
        if let NodeEvent::StartingRound(height, ..) = event {
            if height == 3 {
                break;
            }
        }
    }

    // Send stop calls
    println!("Sending stop calls...");
    alice_node.call(NodeCall::Stop).unwrap();
    bob_node.call(NodeCall::Stop).unwrap();
    charlie_node.call(NodeCall::Stop).unwrap();

    // Wait for nodes to finish
    println!("Waiting for nodes to finish...");
    alice_handle.await?;
    bob_handle.await?;
    charlie_handle.await?;

    // Assert heights
    assert_eq!(alice_node.consensus.state.lock().await.height, 3);
    assert_eq!(bob_node.consensus.state.lock().await.height, 3);
    assert_eq!(charlie_node.consensus.state.lock().await.height, 3);

    Ok(())
}

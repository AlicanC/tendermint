use libp2p::{PeerId, identity::Keypair};
use std::{error::Error, sync::Arc};
use tendermint::node::{Node, NodeCall, NodeEvent, ValidatorSet};
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
    let alice = TestActor::new("Alice  ")?;
    let bob = TestActor::new("Bob    ")?;
    let charlie = TestActor::new("Charlie")?;

    let validator_set = ValidatorSet::new(vec![
        (alice.id.clone(), 100),
        (bob.id.clone(), 100),
        (charlie.id.clone(), 100),
    ]);

    // Create nodes
    let alice_node = Arc::new(Node::new(
        alice.id.clone(),
        alice.keypair.clone(),
        validator_set.clone(),
    )?);
    let bob_node = Arc::new(Node::new(
        bob.id.clone(),
        bob.keypair.clone(),
        validator_set.clone(),
    )?);
    let charlie_node = Arc::new(Node::new(
        charlie.id.clone(),
        charlie.keypair.clone(),
        validator_set.clone(),
    )?);

    let alice_call_tx = alice_node.call_tx.clone();
    let bob_call_tx = bob_node.call_tx.clone();
    let charlie_call_tx = charlie_node.call_tx.clone();
    let mut alice_event_rx = alice_node.event_tx.subscribe();
    let mut bob_event_rx = bob_node.event_tx.subscribe();
    let mut charlie_event_rx = charlie_node.event_tx.subscribe();

    // Run nodes
    let alice_handle = {
        let alice_node = alice_node.clone();
        task::spawn(async move { alice_node.run().await.unwrap() })
    };
    let bob_handle = {
        let bob_node = bob_node.clone();
        task::spawn(async move { bob_node.run().await.unwrap() })
    };
    let charlie_handle = {
        let charlie_node = charlie_node.clone();
        task::spawn(async move { charlie_node.run().await.unwrap() })
    };

    // Wait for nodes to be ready
    println!("Waiting for nodes to discover each other...");
    let alice_ready_handle = {
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
    alice_call_tx.send_async(NodeCall::Start).await?;
    bob_call_tx.send_async(NodeCall::Start).await?;
    charlie_call_tx.send_async(NodeCall::Start).await?;

    // Wait for Alice to start a round at height 3
    println!("Waiting for Alice to start a round at height 3...");
    let mut alice_event_rx = alice_node.event_tx.subscribe();
    while let Ok(event) = alice_event_rx.recv().await {
        if let NodeEvent::StartingRound(height, ..) = event {
            if height == 3 {
                break;
            }
        }
    }

    // Send stop calls
    println!("Sending stop calls...");
    alice_call_tx.send_async(NodeCall::Stop).await?;
    bob_call_tx.send_async(NodeCall::Stop).await?;
    charlie_call_tx.send_async(NodeCall::Stop).await?;

    // Wait for nodes to finish
    println!("Waiting for nodes to finish...");
    alice_handle.await?;
    bob_handle.await?;
    charlie_handle.await?;

    // Assert heights
    assert_eq!(alice_node.state.lock().await.height, 3);
    assert_eq!(bob_node.state.lock().await.height, 3);
    assert_eq!(charlie_node.state.lock().await.height, 2);

    Ok(())
}

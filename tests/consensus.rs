use std::sync::Arc;
use tendermint::{
    message::Message,
    node::{Node, NodeCall, NodeEvent, ValidatorSet},
};
use tokio::{sync::broadcast, task};

struct TestActor {
    id: &'static str,
}

impl TestActor {
    fn new(id: &'static str) -> TestActor {
        TestActor { id }
    }
}

#[tokio::test]
async fn consensus() -> Result<(), Box<dyn std::error::Error>> {
    // Setup
    let alice = TestActor::new("Alice  ");
    let bob = TestActor::new("Bob    ");
    let charlie = TestActor::new("Charlie");

    let validator_set = ValidatorSet::new(vec![(alice.id, 100), (bob.id, 100), (charlie.id, 100)]);

    let (tx, mut rx) = broadcast::channel::<Message>(256);

    // Create nodes
    let alice_node = Arc::new(Node::new(alice.id, validator_set.clone(), tx.clone()));
    let bob_node = Arc::new(Node::new(bob.id, validator_set.clone(), tx.clone()));
    let charlie_node = Arc::new(Node::new(charlie.id, validator_set.clone(), tx.clone()));

    let alice_call_tx = alice_node.call_tx.clone();
    let bob_call_tx = bob_node.call_tx.clone();
    let charlie_call_tx = charlie_node.call_tx.clone();
    let mut alice_event_rx = alice_node.event_tx.subscribe();
    let mut bob_event_rx = bob_node.event_tx.subscribe();
    let mut charlie_event_rx = charlie_node.event_tx.subscribe();

    // Run nodes
    let alice_handle = {
        let alice_node = alice_node.clone();
        task::spawn(async move { alice_node.run().await })
    };
    let bob_handle = {
        let bob_node = bob_node.clone();
        task::spawn(async move { bob_node.run().await })
    };
    let charlie_handle = {
        let charlie_node = charlie_node.clone();
        task::spawn(async move { charlie_node.run().await })
    };

    // Wait for nodes to be ready
    println!("Waiting for nodes to be ready...");
    while let Ok(event) = alice_event_rx.recv().await {
        if event == NodeEvent::Ready {
            break;
        }
    }
    while let Ok(event) = bob_event_rx.recv().await {
        if event == NodeEvent::Ready {
            break;
        }
    }
    while let Ok(event) = charlie_event_rx.recv().await {
        if event == NodeEvent::Ready {
            break;
        }
    }

    // Send start calls
    println!("Sending start calls...");
    alice_call_tx.send_async(NodeCall::Start).await?;
    bob_call_tx.send_async(NodeCall::Start).await?;
    charlie_call_tx.send_async(NodeCall::Start).await?;

    // Wait for a message at height 3
    println!("Waiting for a message at height 3...");
    while let Ok(message) = rx.recv().await {
        if message.content.height() == 3 {
            break;
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
    assert_eq!(charlie_node.state.lock().await.height, 3);

    Ok(())
}

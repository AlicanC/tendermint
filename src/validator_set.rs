use libp2p::PeerId;

#[derive(Clone, Debug)]
pub struct ValidatorSet {
    pub items: Vec<(PeerId, u64)>,
}

impl ValidatorSet {
    pub fn new(items: Vec<(PeerId, u64)>) -> ValidatorSet {
        ValidatorSet { items }
    }

    pub fn total_power(&self) -> u64 {
        self.items.iter().map(|(_, power)| *power).sum()
    }

    pub fn power_of(&self, id: &PeerId) -> u64 {
        self.items
            .iter()
            .find(|(node_id, _)| node_id == id)
            .map(|(_, power)| *power)
            .unwrap_or(0)
    }

    pub fn f(&self) -> u64 {
        (self.total_power() - 1) / 3
    }
}

#[cfg(test)]
mod tests {
    use libp2p::identity::Keypair;

    use super::*;

    #[tokio::test]
    async fn validator_set() {
        let alice = Keypair::generate_ed25519();
        let bob = Keypair::generate_ed25519();
        let charlie = Keypair::generate_ed25519();

        let validator_set = ValidatorSet::new(vec![
            (alice.public().to_peer_id(), 100),
            (bob.public().to_peer_id(), 100),
            (charlie.public().to_peer_id(), 100),
        ]);

        let n = validator_set.total_power();
        assert_eq!(n, 300);
        let f = validator_set.f();
        assert_eq!(f, (n - 1) / 3);
    }
}

use libp2p::identity::PublicKey;

#[derive(Clone)]
pub struct Validator {
    pub public_key: PublicKey,
    pub voting_power: u64,
}

impl Validator {
    pub fn new(public_key: PublicKey, voting_power: u64) -> Validator {
        Validator {
            public_key,
            voting_power,
        }
    }
}

/// See: https://docs.tendermint.com/master/spec/core/genesis.html
#[derive(Clone)]
pub struct Genesis {
    pub validators: Vec<Validator>,
}

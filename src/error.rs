#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid helius cluster")]
    InvalidHeliusCluster,
    #[error("missing async solana client")]
    MissingHeliusSolanaAsyncClient,
    #[error("invalid pubkey bytes")]
    InvalidPubkeyBytes,
    #[error("clock still ticking")]
    ClockStillTicking,
    #[error("unconfirmed jito bundle")]
    UnconfirmedJitoBundle,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid helius cluster")]
    InvalidHeliusCluster,
    #[error("missing async solana client")]
    MissingHeliusSolanaAsyncClient,
}

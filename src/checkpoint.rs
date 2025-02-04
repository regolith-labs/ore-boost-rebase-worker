use anyhow::Result;
use ore_boost_api::consts::CHECKPOINT_INTERVAL;
use solana_sdk::{pubkey::Pubkey, signer::Signer};

use crate::client::{AsyncClient, Client};

pub async fn run(client: &Client) -> Result<()> {
    Ok(())
}

async fn checkpoint_once(client: &Client, mint: &Pubkey) -> Result<()> {
    // derive address
    let (boost_pda, _) = ore_boost_api::state::boost_pda(*mint);
    let (checkpoint_pda, _) = ore_boost_api::state::checkpoint_pda(boost_pda);
    // get accounts
    let _boost = client.rpc.get_boost(&boost_pda).await?;
    let checkpoint = client.rpc.get_checkpoint(&checkpoint_pda).await?;
    // check if enough time has passed since last checkpoint
    let clock = client.rpc.get_clock().await?;
    let time_since_last = clock.unix_timestamp - checkpoint.ts;
    if time_since_last < CHECKPOINT_INTERVAL {
        log::info!(
            "not enough time has passed since last checkpoint. Wait {} more seconds.",
            CHECKPOINT_INTERVAL - time_since_last
        );
        return Ok(());
    }
    // get all stake accounts for this boost
    let mut accounts = client.rpc.get_boost_stake_accounts(&boost_pda).await?;
    if accounts.is_empty() {
        log::info!("no stake accounts found for this boost.");
        return Ok(());
    }
    // sort accounts by stake id
    accounts.sort_by(|(_, stake_a), (_, stake_b)| stake_a.id.cmp(&stake_b.id));
    // filter accounts starting from checkpoint.current_id
    let remaining_accounts: Vec<_> = accounts
        .into_iter()
        .filter(|(_, stake)| stake.id >= checkpoint.current_id)
        .collect();
    // pack instructions for rebase
    let mut ixs = vec![];
    if remaining_accounts.is_empty() {
        // if total stakers is zero
        // but the checkpoint interval is still passed,
        // use default account
        ixs.push(ore_boost_api::sdk::rebase(
            client.keypair.pubkey(),
            *mint,
            Pubkey::default(),
        ));
        let _sig = client.send_transaction(ixs.as_slice()).await?;
    }
    Ok(())
}

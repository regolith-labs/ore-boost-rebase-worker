use std::sync::Arc;

use anyhow::Result;
use ore_boost_api::{consts::CHECKPOINT_INTERVAL, state::Checkpoint};
use solana_sdk::{pubkey::Pubkey, signer::Signer};

use crate::client::{AsyncClient, Client};
use crate::error::Error::ClockStillTicking;
use crate::lookup_tables;

const MAX_ACCOUNTS_PER_TX: usize = 10;
const COMPUTE_BUDGET: u32 = 100_000;

pub async fn run(client: &Client, mint: &Pubkey) -> Result<()> {
    // create new lookup tables (cold start)

    // loop start

    // get boost and checkpoint

    // check for interval (loop)
    // continue or sleep

    // create new lookup tables

    // loop end

    // derive address
    let (boost_pda, _) = ore_boost_api::state::boost_pda(*mint);
    let (checkpoint_pda, _) = ore_boost_api::state::checkpoint_pda(boost_pda);
    // get accounts
    let _boost = client.rpc.get_boost(&boost_pda).await?;
    let mut checkpoint = client.rpc.get_checkpoint(&checkpoint_pda).await?;
    // create new lookup tables (cold start)
    loop {}
    // log::info!("///////////////////////////////////////////////////////////");
    // log::info!("// checkpoint");
    // log::info!("{:?} -- {:?}\n", boost_pda, checkpoint);
    // // check for time
    // if let

    Ok(())
}

async fn get_current_stake_accounts() {}

/// check if enough time has passed since last checkpoint
async fn check_for_time(client: &Client, checkpoint: &Checkpoint) -> Result<()> {
    let clock = client.rpc.get_clock().await?;
    let time_since_last = clock.unix_timestamp - checkpoint.ts;
    if time_since_last < CHECKPOINT_INTERVAL {
        log::info!(
            "not enough time has passed since last checkpoint. Wait {} more seconds.",
            CHECKPOINT_INTERVAL - time_since_last
        );
        return Err(ClockStillTicking).map_err(From::from);
    }
    Ok(())
}

async fn checkpoint_once(client: &Client, mint: &Pubkey) -> Result<()> {
    // derive address
    let (boost_pda, _) = ore_boost_api::state::boost_pda(*mint);
    let (checkpoint_pda, _) = ore_boost_api::state::checkpoint_pda(boost_pda);
    // get accounts
    let _boost = client.rpc.get_boost(&boost_pda).await?;
    let checkpoint = client.rpc.get_checkpoint(&checkpoint_pda).await?;
    log::info!("///////////////////////////////////////////////////////////");
    log::info!("// checkpoint");
    log::info!("{:?}\n", checkpoint);
    // check if enough time has passed since last checkpoint
    let clock = client.rpc.get_clock().await?;
    let time_since_last = clock.unix_timestamp - checkpoint.ts;
    log::info!("checking if interval has elapsed");
    if time_since_last < CHECKPOINT_INTERVAL {
        log::info!(
            "not enough time has passed since last checkpoint. Wait {} more seconds.",
            CHECKPOINT_INTERVAL - time_since_last
        );
        return Ok(());
    }
    log::info!("interval elapsed\n");
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
    log::info!("checkpoint current id: {:?}", checkpoint.current_id);
    log::info!("num remaining accounts: {:?}\n", remaining_accounts.len());
    // pack instructions for rebase
    let mut ixs = vec![];
    if remaining_accounts.is_empty() {
        // if total stakers is zero
        // but the checkpoint interval is still passed,
        // use default account to reset checkpoint for new stakers
        ixs.push(ore_boost_api::sdk::rebase(
            client.keypair.pubkey(),
            *mint,
            Pubkey::default(),
        ));
        log::info!("remaining accounts is empty -- but checkpoint is still elpased. resetting.");
        let sig = client.send_transaction(ixs.as_slice()).await?;
        log::info!("reset signature: {:?}\n", sig);
    } else {
        // chunk stake accounts into batches
        let chunks = remaining_accounts.chunks(MAX_ACCOUNTS_PER_TX);
        for chunk in chunks {
            ixs.clear();
            log::info!("chunking");
            for (stake_pubkey, _stake) in chunk {
                log::info!("stake account: {:?}", stake_pubkey);
                let signer = Arc::clone(&client.keypair);
                ixs.push(ore_boost_api::sdk::rebase(
                    signer.pubkey(),
                    *mint,
                    *stake_pubkey,
                ));
            }
            if !ixs.is_empty() {
                log::info!("submitting chunk");
                let sig = client.send_transaction(ixs.as_slice()).await?;
                log::info!("chunk signature: {:?}\n", sig);
            } else {
                log::info!("checkpoint complete\n");
            }
        }
    }
    Ok(())
}

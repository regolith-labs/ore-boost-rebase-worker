use std::sync::Arc;

use anyhow::Result;
use ore_boost_api::{consts::CHECKPOINT_INTERVAL, state::Checkpoint};
use solana_sdk::{pubkey::Pubkey, signer::Signer};

use crate::client::{AsyncClient, Client};
use crate::error::Error::ClockStillTicking;
use crate::lookup_tables;

const MAX_ACCOUNTS_PER_TX: usize = 10;
const COMPUTE_BUDGET_REBASE: u32 = 20_000;
const COMPUTE_BUDGET_REBASE_MANY: u32 = COMPUTE_BUDGET_REBASE * (MAX_ACCOUNTS_PER_TX as u32);

pub async fn run(client: &Client, mint: &Pubkey) -> Result<()> {
    // derive address
    let (boost_pda, _) = ore_boost_api::state::boost_pda(*mint);
    let (checkpoint_pda, _) = ore_boost_api::state::checkpoint_pda(boost_pda);
    // get accounts
    let _boost = client.rpc.get_boost(&boost_pda).await?;
    let mut checkpoint = client.rpc.get_checkpoint(&checkpoint_pda).await?;
    // -- cold start --
    // get stake accounts for current checkpoint
    // and create new lookup tables
    let mut stake_accounts = get_stake_accounts(client, &boost_pda, &checkpoint).await?;
    let mut lookup_tables =
        lookup_tables::open_new(client, &boost_pda, stake_accounts.as_slice()).await?;
    let mut needs_reset = false;
    // start checkpoint loop
    // 1) check for checkpoint interval
    // 2) rebase or sleep and break
    // 3) close lookup tables
    // 4) create new lookup tables for next checkpoint
    loop {
        log::info!("///////////////////////////////////////////////////////////");
        log::info!("// checkpoint");
        log::info!("{:?} -- {:?}", boost_pda, checkpoint);
        if needs_reset {
            match reset(
                client,
                &boost_pda,
                &checkpoint_pda,
                &mut checkpoint,
                &mut stake_accounts,
                &mut lookup_tables,
            )
            .await
            {
                Ok(()) => {
                    needs_reset = false;
                }
                Err(err) => {
                    log::error!("{:?} -- {:?}", boost_pda, err);
                }
            }
        }
        // check for time
        if let Err(err) = check_for_time(client, &checkpoint).await {
            // time has not elapsed or error
            // sleep then continue loop
            log::info!("{:?} -- {:?}", boost_pda, err);
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            continue;
        }
        // rebase all stake accounts
        if let Err(err) = rebase_all(
            client,
            mint,
            &boost_pda,
            stake_accounts.as_slice(),
            lookup_tables.as_slice(),
        )
        .await
        {
            log::error!("{:?} -- {:?}", boost_pda, err);
            continue;
        }
        needs_reset = true;
        // reset
        match reset(
            client,
            &boost_pda,
            &checkpoint_pda,
            &mut checkpoint,
            &mut stake_accounts,
            &mut lookup_tables,
        )
        .await
        {
            Ok(()) => {
                needs_reset = false;
            }
            Err(err) => {
                log::error!("{:?} -- {:?}", boost_pda, err);
            }
        }
    }
}

async fn reset(
    client: &Client,
    boost_pda: &Pubkey,
    checkpoint_pda: &Pubkey,
    checkpoint: &mut Checkpoint,
    stake_accounts: &mut Vec<Pubkey>,
    lookup_tables: &mut Vec<Pubkey>,
) -> Result<()> {
    log::info!("{:?} -- resetting for next checkpoint", boost_pda);
    // close lookup tables
    if let Err(err) = lookup_tables::close_prior(client, &boost_pda).await {
        log::error!("{:?} -- {:?}", boost_pda, err);
    };
    // fetch updated accounts for next checkpoint
    *checkpoint = client.rpc.get_checkpoint(checkpoint_pda).await?;
    *stake_accounts = get_stake_accounts(client, boost_pda, checkpoint).await?;
    // create new lookup tables for next checkpoint
    *lookup_tables = lookup_tables::open_new(client, &boost_pda, stake_accounts.as_slice()).await?;
    log::info!("{:?} -- reset for next checkpoint complete", boost_pda);
    Ok(())
}

/// get stake accounts for current checkpoint
async fn get_stake_accounts(
    client: &Client,
    boost: &Pubkey,
    checkpoint: &Checkpoint,
) -> Result<Vec<Pubkey>> {
    log::info!("get stake accounts for current checkpoint");
    let mut accounts = client.rpc.get_boost_stake_accounts(boost).await?;
    if accounts.is_empty() {
        log::info!("no stake accounts found for this boost.");
        return Ok(vec![]);
    }
    // sort accounts by stake id
    accounts.sort_by(|(_, stake_a), (_, stake_b)| stake_a.id.cmp(&stake_b.id));
    // filter accounts starting from checkpoint.current_id
    let remaining_accounts: Vec<_> = accounts
        .into_iter()
        .filter_map(|(pubkey, stake)| {
            if stake.id >= checkpoint.current_id {
                Some(pubkey)
            } else {
                None
            }
        })
        .collect();
    log::info!("checkpoint current id: {:?}", checkpoint.current_id);
    log::info!("num remaining accounts: {:?}\n", remaining_accounts.len());
    Ok(remaining_accounts)
}

/// check if enough time has passed since last checkpoint
async fn check_for_time(client: &Client, checkpoint: &Checkpoint) -> Result<()> {
    log::info!("checking if interval has elapsed");
    let clock = client.rpc.get_clock().await?;
    let time_since_last = clock.unix_timestamp - checkpoint.ts;
    if time_since_last < CHECKPOINT_INTERVAL {
        log::info!(
            "not enough time has passed since last checkpoint. Wait {} more seconds.",
            CHECKPOINT_INTERVAL - time_since_last
        );
        return Err(ClockStillTicking).map_err(From::from);
    }
    log::info!("interval elapsed\n");
    Ok(())
}

async fn rebase_all(
    client: &Client,
    mint: &Pubkey,
    boost: &Pubkey,
    stake_accounts: &[Pubkey],
    _lookup_tables: &[Pubkey],
) -> Result<()> {
    log::info!("{:?} -- rebasing stake accounts", boost);
    // pack instructions for rebase
    let mut ixs = vec![];
    if stake_accounts.is_empty() {
        // if total stakers is zero
        // but the checkpoint interval is still passed,
        // use default account to reset checkpoint for new stakers
        ixs.push(ore_boost_api::sdk::rebase(
            client.keypair.pubkey(),
            *mint,
            Pubkey::default(),
        ));
        log::info!(
            "{:?} -- remaining accounts is empty -- but checkpoint is still elpased. resetting.",
            boost
        );
        let sig = client
            .send_transaction(ixs.as_slice(), COMPUTE_BUDGET_REBASE)
            .await?;
        log::info!("{:?} -- reset signature: {:?}", boost, sig);
    } else {
        // chunk stake accounts into batches
        let chunks = stake_accounts.chunks(MAX_ACCOUNTS_PER_TX);
        for chunk in chunks {
            ixs.clear();
            for stake in chunk {
                let signer = Arc::clone(&client.keypair);
                ixs.push(ore_boost_api::sdk::rebase(signer.pubkey(), *mint, *stake));
            }
            if !ixs.is_empty() {
                log::info!("{:?} -- submitting chunk", boost);
                let sig = client
                    .send_transaction(ixs.as_slice(), COMPUTE_BUDGET_REBASE_MANY)
                    .await?;
                log::info!("{:?} -- chunk signature: {:?}", boost, sig);
            } else {
                log::info!("{:?} -- checkpoint complete", boost);
            }
        }
    }
    Ok(())
}

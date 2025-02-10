use std::sync::Arc;

use anyhow::Result;
use ore_boost_api::{consts::CHECKPOINT_INTERVAL, state::Checkpoint};
use solana_sdk::{instruction::Instruction, pubkey::Pubkey, signer::Signer};

use crate::client::{AsyncClient, Client};
use crate::error::Error::ClockStillTicking;
use crate::lookup_tables;

const MAX_ACCOUNTS_PER_TX: usize = 38;

pub async fn run(client: &Client, mint: &Pubkey) -> Result<()> {
    // derive address
    let (boost_pda, _) = ore_boost_api::state::boost_pda(*mint);
    let (checkpoint_pda, _) = ore_boost_api::state::checkpoint_pda(boost_pda);
    // get accounts
    let _boost = client.rpc.get_boost(&boost_pda).await?;
    let mut checkpoint = client.rpc.get_checkpoint(&checkpoint_pda).await?;
    let _time = check_for_time(client, &checkpoint, &boost_pda).await;
    lookup_tables::sync(client, &boost_pda).await?;
    Ok(())
    // // -- cold start --
    // // get stake accounts for current checkpoint
    // // and create new lookup tables
    // let mut stake_accounts = get_stake_accounts(client, &boost_pda, &checkpoint).await?;
    // let mut lookup_tables =
    //     lookup_tables::open_new(client, &boost_pda, stake_accounts.as_slice()).await?;
    // let mut needs_reset = false;
    // // start checkpoint loop
    // // 1) fetch checkpoint
    // // 2) check for checkpoint interval
    // // 3) rebase, or sleep and break
    // // 4) close lookup tables
    // // 5) create new lookup tables for next checkpoint
    // loop {
    //     log::info!("///////////////////////////////////////////////////////////");
    //     log::info!("// checkpoint");
    //     log::info!("{:?} -- {:?}", boost_pda, checkpoint);
    //     if needs_reset {
    //         match reset(client, &boost_pda, &checkpoint_pda, &mut lookup_tables).await {
    //             Ok(()) => {
    //                 needs_reset = false;
    //             }
    //             Err(err) => {
    //                 log::error!("{:?} -- {:?}", boost_pda, err);
    //                 tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    //                 continue;
    //             }
    //         }
    //     }
    //     // fetch checkpoint
    //     match client.rpc.get_checkpoint(&checkpoint_pda).await {
    //         Ok(cp) => {
    //             checkpoint = cp;
    //         }
    //         Err(err) => {
    //             log::error!("{:?} -- {:?}", boost_pda, err);
    //             tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    //             continue;
    //         }
    //     }
    //     // check for time
    //     if let Err(err) = check_for_time(client, &checkpoint, &boost_pda).await {
    //         // time has not elapsed or error
    //         // sleep then continue loop
    //         log::info!("{:?} -- {:?}", boost_pda, err);
    //         tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    //         continue;
    //     }
    //     // filter stake accounts
    //     // against the checkpoint current-id,
    //     // recovering from a partial checkpoint if necessary
    //     match get_stake_accounts(client, &boost_pda, &checkpoint).await {
    //         Ok(vec) => {
    //             stake_accounts = vec;
    //         }
    //         Err(err) => {
    //             log::error!("{:?} -- {:?}", boost_pda, err);
    //             tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    //             continue;
    //         }
    //     }
    //     // rebase all stake accounts
    //     if let Err(err) = rebase_all(
    //         client,
    //         mint,
    //         &boost_pda,
    //         stake_accounts.as_slice(),
    //         lookup_tables.as_slice(),
    //     )
    //     .await
    //     {
    //         log::error!("{:?} -- {:?}", boost_pda, err);
    //         tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    //         continue;
    //     }
    //     needs_reset = true;
    //     // reset
    //     match reset(client, &boost_pda, &checkpoint_pda, &mut lookup_tables).await {
    //         Ok(()) => {
    //             needs_reset = false;
    //         }
    //         Err(err) => {
    //             log::error!("{:?} -- {:?}", boost_pda, err);
    //             tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    //         }
    //     }
    // }
}

// // opens and/or extends lookup tables
// // for new stake accounts in next checkpoint
// async fn reset(
//     client: &Client,
//     boost_pda: &Pubkey,
//     checkpoint_pda: &Pubkey,
//     lookup_tables: &mut Vec<Pubkey>,
// ) -> Result<()> {
//     log::info!("{:?} -- resetting for next checkpoint", boost_pda);
//     // fetch updated accounts for next checkpoint
//     let checkpoint = client.rpc.get_checkpoint(checkpoint_pda).await?;
//     let stake_accounts = get_stake_accounts(client, boost_pda, &checkpoint).await?;
//     // create new lookup tables for next checkpoint
//     *lookup_tables = lookup_tables::open_new(client, &boost_pda, stake_accounts.as_slice()).await?;
//     log::info!("{:?} -- reset for next checkpoint complete", boost_pda);
//     Ok(())
// }

/// get stake accounts for current checkpoint
async fn get_stake_accounts(
    client: &Client,
    boost_pda: &Pubkey,
    checkpoint: &Checkpoint,
) -> Result<Vec<Pubkey>> {
    log::info!(
        "{:?} -- get stake accounts for current checkpoint",
        boost_pda
    );
    let mut accounts = client.rpc.get_boost_stake_accounts(boost_pda).await?;
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
    log::info!(
        "{:?} -- checkpoint current id: {:?}",
        boost_pda,
        checkpoint.current_id
    );
    log::info!(
        "{:?} -- num remaining accounts: {:?}",
        boost_pda,
        remaining_accounts.len()
    );
    Ok(remaining_accounts)
}

/// check if enough time has passed since last checkpoint
async fn check_for_time(
    client: &Client,
    checkpoint: &Checkpoint,
    boost_pda: &Pubkey,
) -> Result<()> {
    log::info!("{:?} -- checking if interval has elapsed", boost_pda);
    let clock = client.rpc.get_clock().await?;
    let time_since_last = clock.unix_timestamp - checkpoint.ts;
    if time_since_last < CHECKPOINT_INTERVAL {
        log::info!(
            "{:?} -- not enough time has passed since last checkpoint. Wait {} more seconds.",
            boost_pda,
            CHECKPOINT_INTERVAL - time_since_last
        );
        return Err(ClockStillTicking).map_err(From::from);
    }
    log::info!("{:?} -- interval elapsed", boost_pda);
    Ok(())
}

async fn rebase_all(
    client: &Client,
    mint: &Pubkey,
    boost: &Pubkey,
    stake_accounts: &[Pubkey],
    lookup_tables: &[Pubkey],
) -> Result<()> {
    log::info!("{:?} -- rebasing stake accounts", boost);
    // pack instructions for rebase
    if stake_accounts.is_empty() {
        // if total stakers is zero
        // but the checkpoint interval is still passed,
        // use default account to reset checkpoint for new stakers
        let ix = ore_boost_api::sdk::rebase(client.keypair.pubkey(), *mint, Pubkey::default());
        log::info!(
            "{:?} -- remaining accounts is empty -- but checkpoint is still elpased. resetting.",
            boost
        );
        let sig = client.send_transaction(&[ix]).await?;
        log::info!("{:?} -- reset signature: {:?}", boost, sig);
    } else {
        // chunk stake accounts into batches
        let mut bundles: Vec<Vec<Instruction>> = vec![];
        for chunk in stake_accounts.chunks(MAX_ACCOUNTS_PER_TX) {
            // build transaction
            let mut transaction = vec![];
            for account in chunk {
                let signer = Arc::clone(&client.keypair);
                transaction.push(ore_boost_api::sdk::rebase(signer.pubkey(), *mint, *account));
            }
            bundles.push(transaction);
        }
        // bundle transactions
        for tx in bundles.chunks(4) {
            let bundle: Vec<&[Instruction]> = tx.iter().map(|vec| vec.as_slice()).collect();
            log::info!("{:?} -- submitting rebase", boost);
            let sig = client
                .send_jito_bundle_with_luts(bundle.as_slice(), lookup_tables)
                .await?;
            log::info!("{:?} -- rebase signature: {:?}", boost, sig);
        }
    }
    log::info!("{:?} -- checkpoint complete", boost);
    Ok(())
}

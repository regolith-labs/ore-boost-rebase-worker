use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    sync::Arc,
};

use anyhow::Result;
use ore_boost_api::state::Checkpoint;
use solana_sdk::{
    address_lookup_table, instruction::Instruction, pubkey::Pubkey, signature::Signature,
    signer::Signer,
};

use crate::{
    client::{AsyncClient, Client},
    error::Error::InvalidPubkeyBytes,
};

const MAX_ACCOUNTS_PER_LUT: usize = 256;

/// sync lookup tables
///
/// add and/or extend lookup tables
/// for new stake accounts for next checkpoint
pub async fn sync(client: &Client, boost: &Pubkey) -> Result<()> {
    log::info!("{} -- syncing lookup tables", boost);
    // read existing lookup table addresses
    let existing = read_file(boost)?;
    // fetch lookup table accounts for the stake addresses they hold
    let lookup_tables = client.rpc.get_lookup_tables(existing.as_slice()).await?;
    // fetch all stake accounts
    let stake_accounts = client.rpc.get_boost_stake_accounts(boost).await?;
    // filter for stake accounts that don't already have a lookup table
    let tabled_stake_account_addresses = lookup_tables
        .iter()
        .flat_map(|lut| lut.addresses.to_vec())
        .collect::<Vec<_>>();
    let untabled_stake_account_addresses = stake_accounts
        .into_iter()
        .filter(|(pubkey, _stake)| !tabled_stake_account_addresses.contains(pubkey))
        .collect::<Vec<_>>();
    log::info!(
        "{} -- num tabled addresses: {}",
        boost,
        tabled_stake_account_addresses.len()
    );
    log::info!(
        "{} -- num untabled addresses: {}",
        boost,
        untabled_stake_account_addresses.len()
    );
    // check for a lookup table that still has capacity
    let capacity = lookup_tables
        .into_iter()
        .filter(|lut| lut.addresses.len().lt(&MAX_ACCOUNTS_PER_LUT))
        .collect::<Vec<_>>()
        .first();
    // if capacity, extend with new stake addresses
    let (extended, rest) = (vec![], vec![]);
    if let Some(ref capacity) = capacity {
        extend_lookup_table(client, boost, lookup_table, stake_accounts);
    }
    Ok(())
}

async fn extend_lookup_table(
    client: &Client,
    boost: &Pubkey,
    lookup_table: &Pubkey,
    stake_accounts: &[Pubkey],
) -> Result<()> {
    let mut bundles: Vec<Vec<Instruction>> = Vec::with_capacity(5);
    for chunk in stake_accounts.chunks(26) {
        let signer = client.keypair.pubkey();
        let extend_ix = address_lookup_table::instruction::extend_lookup_table(
            *lookup_table,
            signer,
            Some(signer),
            chunk.to_vec(),
        );
        bundles.push(vec![extend_ix]);
        if bundles.len().eq(&5) {
            let compiled: Vec<&[Instruction]> = bundles.iter().map(|vec| vec.as_slice()).collect();
            log::info!("{:?} -- sending extend instructions as bundle", boost);
            client.send_jito_bundle(compiled.as_slice()).await?;
            bundles.clear();
        }
    }
    // submit last jito bundle
    if !bundles.is_empty() {
        log::info!("{:?} -- found left over extend bundles", boost);
        let compiled: Vec<&[Instruction]> = bundles.iter().map(|vec| vec.as_slice()).collect();
        log::info!("{:?} -- sending extend instructions as bundle", boost);
        client.send_jito_bundle(compiled.as_slice()).await?;
    }
    Ok(())
}

async fn create_lookup_table(client: &Client, boost: &Pubkey) -> Result<Pubkey> {
    log::info!("{:?} -- opening new lookup table", boost);
    let clock = client.rpc.get_clock().await?;
    let signer = client.keypair.pubkey();
    // build and submit create instruction first
    let (create_ix, lut_pda) =
        address_lookup_table::instruction::create_lookup_table(signer, signer, clock.slot);
    let sig = client.send_transaction(&[create_ix]).await?;
    log::info!("{:?} -- new lookup table signature: {:?}", boost, sig);
    Ok(lut_pda)
}

fn write_file(luts: &[Lut], boost: &Pubkey) -> Result<()> {
    log::info!("{:?} -- writing new lookup tables", boost);
    let luts_path = luts_path()?;
    let path = format!("{}-{}", luts_path, boost);
    log::info!("path: {}", path);
    let mut file = OpenOptions::new()
        .create(true) // open or create
        .append(true) // append
        .open(path)?;
    for lut in luts {
        file.write_all(lut.to_bytes().as_slice())?;
        file.write_all(b"\n")?;
    }
    log::info!("{:?} -- new lookup tables written", boost);
    Ok(())
}

type Lut = Pubkey;
fn read_file(boost: &Pubkey) -> Result<Vec<Lut>> {
    log::info!("{:?} -- reading prior lookup tables", boost);
    let luts_path = luts_path()?;
    let path = format!("{}-{}", luts_path, boost);
    let file = File::open(path)?;
    log::info!("{:?} -- found prior lookup tables file", boost);
    let mut luts = vec![];
    let mut line = vec![];
    let mut reader = BufReader::new(file);
    // read lines
    while reader.read_until(b'\n', &mut line)? > 0 {
        // pop new line char
        line.pop();
        // decode
        let bytes = line.clone();
        let pubkey: Result<[u8; 32]> = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!(InvalidPubkeyBytes));
        if let Ok(ref arr) = pubkey {
            let pubkey = Pubkey::new_from_array(*arr);
            // add pubkey to list
            luts.push(pubkey);
        };
        if let Err(err) = pubkey {
            log::error!("{:?}", err);
        }
        // clear and read next line
        line.clear();
    }
    log::info!("{:?} -- parsed prior lookup tables", boost);
    Ok(luts)
}

fn luts_path() -> Result<String> {
    let path = std::env::var("LUTS_PATH")?;
    Ok(path)
}

use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
};

use anyhow::Result;
use solana_sdk::{address_lookup_table, pubkey::Pubkey, signature::Signature, signer::Signer};

use crate::{
    client::{AsyncClient, Client},
    error::Error::InvalidPubkeyBytes,
};

const MAX_ACCOUNTS_PER_TX_EXTEND: usize = 256;
const MAX_ACCOUNTS_PER_TX_CLOSE: usize = 10;

pub async fn close_prior(client: &Client, boost: &Pubkey) -> Result<()> {
    log::info!("///////////////////////////////////////////////////////////");
    log::info!("// resolving previous lookup tables");
    let prior = read_file(boost);
    if let Ok(luts) = prior {
        for chunk in luts.chunks(MAX_ACCOUNTS_PER_TX_CLOSE) {
            let sig = close(client, chunk).await;
            match sig {
                Ok(sig) => {
                    log::info!("chunk closed signature: {:?}", sig);
                }
                Err(err) => {
                    log::error!("{:?}", err);
                    log::error!("chunk failed to close: {:?}", chunk);
                }
            }
        }
        clear_file(boost)?;
    }
    log::info!("resolved prior lookup tables");
    Ok(())
}

pub async fn open_new(
    client: &Client,
    boost: &Pubkey,
    stake_accounts: &[Pubkey],
) -> Result<Vec<Lut>> {
    log::info!("{:?} -- opening new lookup tables", boost);
    let mut lookup_tables = vec![];
    // create new lookup table for each chunk of stake accounts
    for chunk in stake_accounts.chunks(MAX_ACCOUNTS_PER_TX_EXTEND) {
        let clock = client.rpc.get_clock().await?;
        let signer = client.keypair.pubkey();
        // build create and extend instructions
        let (create_ix, lut_pda) =
            address_lookup_table::instruction::create_lookup_table(signer, signer, clock.slot);
        let extend_ix = address_lookup_table::instruction::extend_lookup_table(
            lut_pda,
            signer,
            Some(signer),
            chunk.to_vec(),
        );
        // submit and confirm transaction
        let sig = client.send_transaction(&[create_ix, extend_ix]).await?;
        log::info!("{:?} -- new lookup table signature: {:?}", boost, sig);
        // write lookup table addresses to file
        // to be closed before next checkpoint
        write_file(&[lut_pda], boost)?;
        // push to lookup tables
        lookup_tables.push(lut_pda);
    }
    log::info!("{:?} -- new lookup tables opened", boost);
    Ok(lookup_tables)
}

async fn close(client: &Client, luts: &[Lut]) -> Result<Signature> {
    let mut ixs = vec![];
    for lut in luts {
        let ix = address_lookup_table::instruction::close_lookup_table(
            *lut,
            client.keypair.pubkey(),
            client.keypair.pubkey(),
        );
        ixs.push(ix);
    }
    let sig = client.send_transaction(ixs.as_slice()).await?;
    Ok(sig)
}

fn clear_file(boost: &Pubkey) -> Result<()> {
    log::info!("{:?} -- clearing prior lookup tables", boost);
    let path = format!("{}/{}", LUTS_PATH, boost);
    let _file = File::create(path)?; // create by default truncates if already exists
    log::info!("{:?} -- prior lookup tables cleared", boost);
    Ok(())
}

fn write_file(luts: &[Lut], boost: &Pubkey) -> Result<()> {
    log::info!("{:?} -- writing new lookup tables", boost);
    let path = format!("{}/{}", LUTS_PATH, boost);
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
    let path = format!("{}/{}", LUTS_PATH, boost);
    let file = File::open(path)?;
    log::info!("{:?} -- found prior lookup tables file", boost);
    let mut luts = vec![];
    let mut line = vec![];
    let mut reader = BufReader::new(file);
    while reader.read_until(b'\n', &mut line)? > 0 {
        let bytes = line.clone();
        let arr: [u8; 32] = bytes.try_into().map_err(|_| InvalidPubkeyBytes)?;
        let pubkey = Pubkey::new_from_array(arr);
        luts.push(pubkey);
        line.clear();
    }
    log::info!("{:?} -- parsed prior lookup tables", boost);
    Ok(luts)
}

const LUTS_PATH: &str = std::env!("LUTS_PATH");

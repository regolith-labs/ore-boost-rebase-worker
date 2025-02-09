use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
};

use anyhow::Result;
use solana_sdk::{
    address_lookup_table, instruction::Instruction, pubkey::Pubkey, signature::Signature,
    signer::Signer,
};

use crate::{
    client::{AsyncClient, Client},
    error::Error::InvalidPubkeyBytes,
};

const MAX_ACCOUNTS_PER_LUT: usize = 256;
const MAX_ACCOUNTS_PER_TX_CLOSE: usize = 10;

pub async fn close_prior(client: &Client, boost: &Pubkey) -> Result<()> {
    log::info!("///////////////////////////////////////////////////////////");
    log::info!("// resolving previous lookup tables");
    let prior = read_file(boost);
    if let Ok(ref luts) = prior {
        // first, deactivate accounts
        for chunk in luts.chunks(MAX_ACCOUNTS_PER_TX_CLOSE) {
            // deactivate
            match deactivate(client, luts).await {
                Ok(sig) => {
                    log::info!("{:?} -- chunk deactivate signature: {:?}", boost, sig);
                }
                Err(err) => {
                    log::error!("{:?} -- {:?}", boost, err);
                    log::error!("{:?} -- chunk failed to deactivate: {:?}", boost, chunk);
                }
            }
        }
        // then sleep for 5 minutes
        // to allow 500 blocks to pass
        log::info!(
            "{:?} -- sleep 5 minutes for deactivations to settle before closing",
            boost
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(60 * 5)).await;
        for chunk in luts.chunks(MAX_ACCOUNTS_PER_TX_CLOSE) {
            // deactivate
            // sleep then close
            match close(client, luts).await {
                Ok(sig) => {
                    log::info!("{:?} -- chunk closed signature: {:?}", boost, sig);
                }
                Err(err) => {
                    log::error!("{:?} -- {:?}", boost, err);
                    log::error!("{:?} -- chunk failed to close: {:?}", boost, chunk);
                }
            }
        }
        // clear the cache file
        clear_file(boost)?;
    }
    if let Err(err) = prior {
        log::error!("{:?} -- {:?}", boost, err);
    }
    log::info!("{:?} -- resolved prior lookup tables", boost);
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
    for chunk in stake_accounts.chunks(MAX_ACCOUNTS_PER_LUT) {
        let clock = client.rpc.get_clock().await?;
        let signer = client.keypair.pubkey();
        // build and submit create instruction first
        let (create_ix, lut_pda) =
            address_lookup_table::instruction::create_lookup_table(signer, signer, clock.slot);
        let sig = client.send_transaction(&[create_ix]).await?;
        log::info!("{:?} -- new lookup table signature: {:?}", boost, sig);
        // write lookup table addresses to file
        // to be closed before next checkpoint
        write_file(&[lut_pda], boost)?;
        // then bundle the extend instructions as jito bundles
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        let mut bundles: Vec<Vec<Instruction>> = Vec::with_capacity(5);
        for sub in chunk.chunks(26) {
            let extend_ix = address_lookup_table::instruction::extend_lookup_table(
                lut_pda,
                signer,
                Some(signer),
                sub.to_vec(),
            );
            bundles.push(vec![extend_ix]);
            if bundles.len().eq(&5) {
                let compiled: Vec<&[Instruction]> =
                    bundles.iter().map(|vec| vec.as_slice()).collect();
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
        // push to lookup tables
        lookup_tables.push(lut_pda);
    }
    log::info!("{:?} -- new lookup tables opened", boost);
    Ok(lookup_tables)
}

async fn deactivate(client: &Client, luts: &[Lut]) -> Result<Signature> {
    let mut ixs = vec![];
    for lut in luts {
        let ix = address_lookup_table::instruction::deactivate_lookup_table(
            *lut,
            client.keypair.pubkey(),
        );
        ixs.push(ix);
    }
    let sig = client.send_transaction(ixs.as_slice()).await?;
    Ok(sig)
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
    let luts_path = luts_path()?;
    let path = format!("{}-{}", luts_path, boost);
    let _file = File::create(path)?; // create by default truncates if already exists
    log::info!("{:?} -- prior lookup tables cleared", boost);
    Ok(())
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
        log::info!("bytes: {:?}", bytes);
        log::info!("bytes len: {:?}", bytes.len());
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

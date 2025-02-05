use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use helius::types::{Cluster, SmartTransactionConfig, Timeout};
use ore_boost_api::state::{Boost, Checkpoint, Stake};
use solana_account_decoder::UiAccountEncoding;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, RpcFilterType};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use solana_sdk::{signature::Keypair, signer::EncodableKey};
use steel::{sysvar, AccountDeserialize, Clock, Discriminator, Instruction};

use crate::error::Error::{InvalidHeliusCluster, MissingHeliusSolanaAsyncClient};

pub struct Client {
    pub rpc: helius::Helius,
    pub keypair: Arc<Keypair>,
}

impl Client {
    pub fn new() -> Result<Self> {
        let helius_api_key = helius_api_key();
        let helius_cluster = helius_cluster()?;
        let keypair = keypair()?;
        let rpc = helius::Helius::new_with_async_solana(helius_api_key.as_str(), helius_cluster)?;
        let client = Self {
            rpc,
            keypair: Arc::new(keypair),
        };
        Ok(client)
    }
    pub async fn send_transaction(&self, ixs: &[Instruction]) -> Result<Signature> {
        let signer = Arc::clone(&self.keypair);
        let signers: Vec<Arc<dyn Signer>> = vec![signer];
        let tx = SmartTransactionConfig::new(ixs.to_vec(), signers, Timeout::default());
        let sig = self.rpc.send_smart_transaction(tx).await?;
        Ok(sig)
    }
}

#[async_trait]
pub trait AsyncClient {
    fn get_async_client(&self) -> Result<Arc<RpcClient>>;
    async fn get_boost(&self, boost: &Pubkey) -> Result<Boost>;
    async fn get_boost_stake_accounts(&self, boost: &Pubkey) -> Result<Vec<(Pubkey, Stake)>>;
    async fn get_checkpoint(&self, checkpoint: &Pubkey) -> Result<Checkpoint>;
    async fn get_clock(&self) -> Result<Clock>;
}

#[async_trait]
impl AsyncClient for helius::Helius {
    fn get_async_client(&self) -> Result<Arc<RpcClient>> {
        let res = match &self.async_rpc_client {
            Some(rpc) => {
                let rpc = Arc::clone(rpc);
                Ok(rpc)
            }
            None => Err(MissingHeliusSolanaAsyncClient),
        };
        res.map_err(From::from)
    }
    async fn get_boost(&self, boost: &Pubkey) -> Result<Boost> {
        let data = self.get_async_client()?.get_account_data(boost).await?;
        let boost = Boost::try_from_bytes(data.as_slice())?;
        Ok(*boost)
    }
    async fn get_boost_stake_accounts(&self, boost: &Pubkey) -> Result<Vec<(Pubkey, Stake)>> {
        let filter = RpcFilterType::Memcmp(Memcmp::new_raw_bytes(56, boost.to_bytes().to_vec()));
        get_program_accounts::<Stake>(
            self.get_async_client()?.as_ref(),
            &ore_boost_api::ID,
            vec![filter],
        )
        .await
    }
    async fn get_checkpoint(&self, checkpoint: &Pubkey) -> Result<Checkpoint> {
        let data = self
            .get_async_client()?
            .get_account_data(checkpoint)
            .await?;
        let checkpoint = Checkpoint::try_from_bytes(data.as_slice())?;
        Ok(*checkpoint)
    }
    async fn get_clock(&self) -> Result<Clock> {
        let data = self
            .get_async_client()?
            .get_account_data(&sysvar::clock::ID)
            .await?;
        let clock = bincode::deserialize::<Clock>(data.as_slice())?;
        Ok(clock)
    }
}

async fn get_program_accounts<T>(
    client: &RpcClient,
    program_id: &Pubkey,
    filters: Vec<RpcFilterType>,
) -> Result<Vec<(Pubkey, T)>>
where
    T: AccountDeserialize + Discriminator + Copy,
{
    let mut all_filters = vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
        0,
        T::discriminator().to_le_bytes().to_vec(),
    ))];
    all_filters.extend(filters);
    let result = client
        .get_program_accounts_with_config(
            program_id,
            RpcProgramAccountsConfig {
                filters: Some(all_filters),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?;
    let accounts = result
        .into_iter()
        .flat_map(|(pubkey, account)| {
            let account = T::try_from_bytes(&account.data)?;
            Ok::<_, anyhow::Error>((pubkey, *account))
        })
        .collect();
    Ok(accounts)
}

fn helius_api_key() -> String {
    std::env!("HELIUS_API_KEY").to_string()
}

fn helius_cluster() -> Result<Cluster> {
    let cluster_str = std::env!("HELIUS_CLUSTER");
    let res = match cluster_str {
        "mainnet" => Ok(Cluster::MainnetBeta),
        "mainnet-staked" => Ok(Cluster::StakedMainnetBeta),
        "devnet" => Ok(Cluster::Devnet),
        _ => Err(InvalidHeliusCluster),
    };
    res.map_err(From::from)
}

fn keypair() -> Result<Keypair> {
    let keypair_path = std::env!("KEYPAIR_PATH");
    let keypair =
        Keypair::read_from_file(keypair_path).map_err(|err| anyhow::anyhow!(err.to_string()))?;
    Ok(keypair)
}

use alloy::json_abi::Event;
use alloy::primitives::{keccak256, Address, B256, U64};
use alloy::rpc::types::ValueOrArray;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::event::contract_setup::{AddressDetails, IndexingContractSetup};
use crate::event::factory_event_filter_sync::update_known_factory_deployed_addresses;
use crate::event::rindexer_event_filter::FactoryFilter;
use crate::manifest::config::Config;
use crate::manifest::contract::EventInputIndexedFilters;
use crate::{
    event::{
        callback_registry::{
            EventCallbackRegistry, EventResult, TraceCallbackRegistry, TraceResult,
        },
        contract_setup::NetworkContract,
        BuildRindexerFilterError, RindexerEventFilter,
    },
    indexer::IndexingEventsProgressState,
    manifest::{native_transfer::TraceProcessingMethod, storage::CsvDetails},
    PostgresClient,
};

pub struct ContractEventProcessingConfig {
    pub id: String,
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub topic_id: B256,
    pub event_name: String,
    pub config: Config,
    pub network_contract: Arc<NetworkContract>,
    pub timestamps: bool,
    pub start_block: U64,
    pub end_block: U64,
    pub registry: Arc<EventCallbackRegistry>,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    pub csv_details: Option<CsvDetails>,
    pub stream_last_synced_block_file_path: Option<String>,
    pub index_event_in_order: bool,
    pub live_indexing: bool,
    pub indexing_distance_from_head: U64,
}

pub const LEGACY_DETAIL_KEY: &str = "__event__";

/// Derives the public cursor key for one manifest detail.
///
/// The format is lowercase
/// `{contract_address}:i{position}:{value}` for each populated indexed filter. Multiple values at
/// one position are comma-joined in manifest order, and positions are colon-joined. Details
/// without both an address and an indexed filter use [`LEGACY_DETAIL_KEY`]. Address arrays are
/// comma-joined in manifest order.
pub fn derive_detail_key(
    indexing_contract_setup: &IndexingContractSetup,
    event_name: &str,
) -> String {
    let (address, indexed_filters) = match indexing_contract_setup {
        IndexingContractSetup::Address(details) => {
            (&details.address, details.indexed_filters.as_ref())
        }
        IndexingContractSetup::Factory(details) => {
            (&details.address, details.indexed_filters.as_ref())
        }
        IndexingContractSetup::Filter(_) => return LEGACY_DETAIL_KEY.to_string(),
    };

    let address = match address {
        ValueOrArray::Value(address) => address.to_string(),
        ValueOrArray::Array(addresses) if !addresses.is_empty() => {
            addresses.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
        }
        ValueOrArray::Array(_) => return LEGACY_DETAIL_KEY.to_string(),
    };
    let Some(indexed_filter) = indexed_filters
        .and_then(|filters| filters.iter().find(|filter| filter.event_name == event_name))
    else {
        return LEGACY_DETAIL_KEY.to_string();
    };

    let positions = [
        (1, indexed_filter.indexed_1.as_ref()),
        (2, indexed_filter.indexed_2.as_ref()),
        (3, indexed_filter.indexed_3.as_ref()),
    ]
    .into_iter()
    .filter_map(|(position, values)| {
        values
            .filter(|values| !values.is_empty())
            .map(|values| format!("i{position}:{}", values.join(",")))
    })
    .collect::<Vec<_>>();

    if positions.is_empty() {
        LEGACY_DETAIL_KEY.to_string()
    } else {
        format!("{}:{}", address, positions.join(":")).to_ascii_lowercase()
    }
}

impl ContractEventProcessingConfig {
    pub fn info_log_name(&self) -> String {
        format!("{}::{}::{}", self.contract_name, self.event_name, self.network_contract.network)
    }

    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        match &self.network_contract.indexing_contract_setup {
            IndexingContractSetup::Address(details) => RindexerEventFilter::new_address_filter(
                &self.topic_id,
                &self.event_name,
                details,
                self.start_block,
                self.end_block,
            ),
            IndexingContractSetup::Filter(details) => RindexerEventFilter::new_filter(
                &self.topic_id,
                &self.event_name,
                details,
                self.start_block,
                self.end_block,
            ),
            IndexingContractSetup::Factory(details) => {
                let index_filter = details.indexed_filters.iter().find_map(|indexed_filters| {
                    indexed_filters.iter().find(|&n| n.event_name == self.event_name)
                });

                Ok(RindexerEventFilter::Factory(FactoryFilter {
                    project_path: self.project_path.clone(),
                    indexer_name: self.indexer_name.clone(),
                    factory_contract_name: details.contract_name.clone(),
                    factory_address: details.address.clone(),
                    factory_event_name: details.event.name.clone(),
                    factory_input_name: details.input_name.clone(),
                    network: self.network_contract.network.clone(),
                    topic_id: self.topic_id,
                    topics: index_filter.cloned().map(Into::into).unwrap_or_default(),
                    clickhouse: self.clickhouse.clone(),
                    postgres: self.postgres.clone(),
                    csv_details: self.csv_details.clone(),

                    current_block: self.start_block,
                    next_block: self.end_block,
                }))
            }
        }
    }

    pub async fn trigger_event(&self, fn_data: Vec<EventResult>) -> Result<(), String> {
        self.registry.trigger_event(&self.id, fn_data).await
    }
}

#[cfg(test)]
mod tests {
    use super::{derive_detail_key, LEGACY_DETAIL_KEY};
    use crate::{
        event::contract_setup::{AddressDetails, FilterDetails, IndexingContractSetup},
        manifest::contract::EventInputIndexedFilters,
    };
    use alloy::{primitives::Address, rpc::types::ValueOrArray};
    use std::{collections::HashSet, str::FromStr};

    fn detail(address: &str, position: usize, value: &str) -> IndexingContractSetup {
        let mut filter = EventInputIndexedFilters {
            event_name: "Transfer".to_string(),
            indexed_1: None,
            indexed_2: None,
            indexed_3: None,
        };
        match position {
            1 => filter.indexed_1 = Some(vec![value.to_string()]),
            2 => filter.indexed_2 = Some(vec![value.to_string()]),
            3 => filter.indexed_3 = Some(vec![value.to_string()]),
            _ => unreachable!(),
        }

        IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::from_str(address).unwrap()),
            indexed_filters: Some(vec![filter]),
        })
    }

    #[test]
    fn detail_key_is_stable_unique_and_independent_of_detail_order() {
        let wallet = "0x1111111111111111111111111111111111111111";
        let tokens = [
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "0xcccccccccccccccccccccccccccccccccccccccc",
        ];
        let details = tokens
            .iter()
            .flat_map(|token| [detail(token, 1, wallet), detail(token, 2, wallet)])
            .collect::<Vec<_>>();

        let keys =
            details.iter().map(|detail| derive_detail_key(detail, "Transfer")).collect::<Vec<_>>();
        let reversed_keys = details
            .iter()
            .rev()
            .map(|detail| derive_detail_key(detail, "Transfer"))
            .collect::<HashSet<_>>();

        assert_eq!(keys.iter().collect::<HashSet<_>>().len(), 6);
        assert_eq!(keys.iter().cloned().collect::<HashSet<_>>(), reversed_keys);
        assert_eq!(keys[0], format!("{}:i1:{}", tokens[0], wallet));
    }

    #[test]
    fn detail_key_preserves_value_order_and_joins_filter_positions() {
        let setup = IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(
                Address::from_str("0xaAaAaAaaAaAaAaaAaAAAAAAAAaaaAaAaAaaAaaAa").unwrap(),
            ),
            indexed_filters: Some(vec![EventInputIndexedFilters {
                event_name: "Transfer".to_string(),
                indexed_1: Some(vec!["B".to_string(), "A".to_string()]),
                indexed_2: None,
                indexed_3: Some(vec!["C".to_string()]),
            }]),
        });

        assert_eq!(
            derive_detail_key(&setup, "Transfer"),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:i1:b,a:i3:c"
        );
    }

    #[test]
    fn detail_key_falls_back_without_address_or_indexed_filters() {
        let no_address = IndexingContractSetup::Filter(FilterDetails {
            events: ValueOrArray::Value("Transfer".to_string()),
            indexed_filters: Some(vec![EventInputIndexedFilters {
                event_name: "Transfer".to_string(),
                indexed_1: Some(vec!["wallet".to_string()]),
                indexed_2: None,
                indexed_3: None,
            }]),
        });
        let no_filters = IndexingContractSetup::Address(AddressDetails {
            address: ValueOrArray::Value(Address::ZERO),
            indexed_filters: None,
        });

        assert_eq!(derive_detail_key(&no_address, "Transfer"), LEGACY_DETAIL_KEY);
        assert_eq!(derive_detail_key(&no_filters, "Transfer"), LEGACY_DETAIL_KEY);
    }
}

pub struct FactoryEventProcessingConfig {
    pub id: String,
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub address: ValueOrArray<Address>,
    pub input_name: ValueOrArray<String>,
    pub event: Event,
    pub config: Config,
    pub network_contract: Arc<NetworkContract>,
    pub timestamps: bool,
    pub start_block: U64,
    pub end_block: U64,
    pub registry: Arc<EventCallbackRegistry>,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    pub csv_details: Option<CsvDetails>,
    pub stream_last_synced_block_file_path: Option<String>,
    pub index_event_in_order: bool,
    pub live_indexing: bool,
    pub indexing_distance_from_head: U64,
}

impl FactoryEventProcessingConfig {
    pub fn input_names(&self) -> Vec<String> {
        match &self.input_name {
            ValueOrArray::Value(name) => vec![name.clone()],
            ValueOrArray::Array(names) => names.clone(),
        }
    }

    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        let event_name = self.event.name.clone();
        let event_selector = self.event.selector();

        let details = AddressDetails {
            address: self.address.clone(),
            indexed_filters: Some(vec![EventInputIndexedFilters {
                event_name: event_name.clone(),
                indexed_1: None,
                indexed_2: None,
                indexed_3: None,
            }]),
        };

        RindexerEventFilter::new_address_filter(
            &event_selector,
            &event_name,
            &details,
            self.start_block,
            self.end_block,
        )
    }

    pub async fn trigger_event(&self, events: Vec<EventResult>) -> Result<(), String> {
        self.registry.trigger_event(&self.id, events.clone()).await?;

        update_known_factory_deployed_addresses(self, &events).await.map_err(|e| e.to_string())
    }

    pub fn info_log_name(&self) -> String {
        format!("{}::{}::{}", self.contract_name, self.event.name, self.network_contract.network)
    }
}

pub enum EventProcessingConfig {
    ContractEventProcessing(ContractEventProcessingConfig),
    FactoryEventProcessing(FactoryEventProcessingConfig),
}

impl From<ContractEventProcessingConfig> for EventProcessingConfig {
    fn from(config: ContractEventProcessingConfig) -> Self {
        Self::ContractEventProcessing(config)
    }
}

impl From<FactoryEventProcessingConfig> for EventProcessingConfig {
    fn from(config: FactoryEventProcessingConfig) -> Self {
        Self::FactoryEventProcessing(config)
    }
}

impl EventProcessingConfig {
    pub fn is_factory_event(&self) -> bool {
        match self {
            Self::ContractEventProcessing(_) => false,
            Self::FactoryEventProcessing(_) => true,
        }
    }

    pub fn topic_id(&self) -> B256 {
        match self {
            Self::ContractEventProcessing(config) => config.topic_id,
            Self::FactoryEventProcessing(config) => config.event.selector(),
        }
    }

    pub fn id(&self) -> B256 {
        let topic_id = self.topic_id();
        let contract_name = self.contract_name();
        let network = self.network_contract().network.to_string();

        let combined = format!("{topic_id}{contract_name}{network}");
        keccak256(combined.as_bytes())
    }

    pub fn config(&self) -> &Config {
        match self {
            Self::ContractEventProcessing(config) => &config.config,
            Self::FactoryEventProcessing(config) => &config.config,
        }
    }

    pub fn timestamps(&self) -> bool {
        match self {
            Self::ContractEventProcessing(config) => config.timestamps,
            Self::FactoryEventProcessing(config) => config.timestamps,
        }
    }

    pub fn info_log_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.info_log_name().clone(),
            Self::FactoryEventProcessing(config) => config.info_log_name(),
        }
    }

    pub fn detail_key(&self) -> String {
        derive_detail_key(&self.network_contract().indexing_contract_setup, &self.event_name())
    }

    pub fn network_contract(&self) -> Arc<NetworkContract> {
        match self {
            Self::ContractEventProcessing(config) => config.network_contract.clone(),
            Self::FactoryEventProcessing(config) => config.network_contract.clone(),
        }
    }

    pub fn index_event_in_order(&self) -> bool {
        match self {
            Self::ContractEventProcessing(config) => config.index_event_in_order,
            Self::FactoryEventProcessing(config) => config.index_event_in_order,
        }
    }

    pub fn contract_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.contract_name.clone(),
            Self::FactoryEventProcessing(config) => config.contract_name.clone(),
        }
    }

    pub fn indexer_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.indexer_name.clone(),
            Self::FactoryEventProcessing(config) => config.indexer_name.clone(),
        }
    }

    pub fn event_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.event_name.clone(),
            Self::FactoryEventProcessing(config) => config.event.name.clone(),
        }
    }

    pub fn live_indexing(&self) -> bool {
        match self {
            Self::ContractEventProcessing(config) => config.live_indexing,
            Self::FactoryEventProcessing(config) => config.live_indexing,
        }
    }

    pub fn indexing_distance_from_head(&self) -> U64 {
        match self {
            Self::ContractEventProcessing(config) => config.indexing_distance_from_head,
            Self::FactoryEventProcessing(config) => config.indexing_distance_from_head,
        }
    }

    pub fn progress(&self) -> Arc<Mutex<IndexingEventsProgressState>> {
        match self {
            Self::ContractEventProcessing(config) => config.progress.clone(),
            Self::FactoryEventProcessing(config) => config.progress.clone(),
        }
    }

    pub fn postgres(&self) -> Option<Arc<PostgresClient>> {
        match self {
            Self::ContractEventProcessing(config) => config.postgres.clone(),
            Self::FactoryEventProcessing(config) => config.postgres.clone(),
        }
    }

    pub fn clickhouse(&self) -> Option<Arc<ClickhouseClient>> {
        match self {
            Self::ContractEventProcessing(config) => config.clickhouse.clone(),
            Self::FactoryEventProcessing(config) => config.clickhouse.clone(),
        }
    }

    pub fn csv_details(&self) -> Option<CsvDetails> {
        match self {
            Self::ContractEventProcessing(config) => config.csv_details.clone(),
            Self::FactoryEventProcessing(config) => config.csv_details.clone(),
        }
    }

    pub fn stream_last_synced_block_file_path(&self) -> Option<String> {
        match self {
            Self::ContractEventProcessing(config) => {
                config.stream_last_synced_block_file_path.clone()
            }
            Self::FactoryEventProcessing(config) => {
                config.stream_last_synced_block_file_path.clone()
            }
        }
    }

    pub fn project_path(&self) -> PathBuf {
        match self {
            Self::ContractEventProcessing(config) => config.project_path.clone(),
            Self::FactoryEventProcessing(config) => config.project_path.clone(),
        }
    }

    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        match self {
            Self::ContractEventProcessing(config) => config.to_event_filter(),
            Self::FactoryEventProcessing(config) => config.to_event_filter(),
        }
    }

    pub async fn trigger_event(&self, fn_data: Vec<EventResult>) -> Result<(), String> {
        match self {
            Self::ContractEventProcessing(config) => config.trigger_event(fn_data).await,
            Self::FactoryEventProcessing(config) => config.trigger_event(fn_data).await,
        }
    }
}

#[derive(Clone)]
pub struct TraceProcessingConfig {
    pub id: String,
    pub project_path: PathBuf,
    pub start_block: U64,
    pub end_block: U64,
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub network: String,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
    pub registry: Arc<TraceCallbackRegistry>,
    pub method: TraceProcessingMethod,
    pub stream_last_synced_block_file_path: Option<String>,
}

impl TraceProcessingConfig {
    pub async fn trigger_event(&self, fn_data: Vec<TraceResult>) {
        // Trigger events for all registered events in this network's registry
        for event in &self.registry.events {
            let _ = self.registry.trigger_event(&event.id, fn_data.clone()).await;
        }
    }
}

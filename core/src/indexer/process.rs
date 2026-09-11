use alloy::primitives::{B256, U64};

use futures::future::join_all;
use futures::StreamExt;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tracing::{debug, error, info};

use crate::helpers::is_relevant_block;
use crate::indexer::reorg::reorg_safe_distance_for_chain;
use crate::metrics::indexing as metrics;
use crate::provider::JsonRpcCachedProvider;
use crate::{
    event::{
        callback_registry::EventResult, config::EventProcessingConfig, BuildRindexerFilterError,
        RindexerEventFilter,
    },
    indexer::{
        dependency::{ContractEventsDependenciesConfig, EventDependencies},
        fetch_logs::{fetch_logs_stream, FetchLogsResult},
        last_synced::update_progress_and_last_synced_task,
        progress::IndexingEventProgressStatus,
        task_tracker::{indexing_event_processed, indexing_event_processing},
    },
    is_running,
    provider::ProviderError,
};

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),

    #[error("Could not get block number from provider: {0}")]
    ProviderCallError(#[from] ProviderError),
}

/// Processes an event that doesn't have dependencies.
/// First processes historical logs, then starts live indexing if the event is configured for live indexing.
/// This function returns immediately without waiting for the indexing to complete.
pub async fn process_non_blocking_event(
    config: EventProcessingConfig,
) -> Result<(), ProcessEventError> {
    debug!("{} - Processing non blocking event", config.info_log_name());

    process_event_logs(Arc::new(config), false, false).await?;

    Ok(())
}

/// Processes historical logs for a blocking event that has dependencies.
/// This function waits for the indexing to complete before returning.
pub async fn process_blocking_event_historical_data(
    config: Arc<EventProcessingConfig>,
) -> Result<(), Box<ProviderError>> {
    debug!("{} - Processing blocking event historical data", config.info_log_name());

    process_event_logs(config, true, true).await?;

    Ok(())
}

/// note block_until_indexed:
/// Whether to wait for all indexing tasks to complete for an event before returning
//  (needed for dependency indexing)
async fn process_event_logs(
    config: Arc<EventProcessingConfig>,
    force_no_live_indexing: bool,
    block_until_indexed: bool,
) -> Result<(), Box<ProviderError>> {
    // The concurrency with which we can call the trigger. If the indexer is running in-order
    // we can only call one at a time, otherwise we can call multiple in parallel based on what is
    // best for the application.
    //
    // We default to `2`, but the user will ideally override this based on the logic in the handler.
    // TODO: this feature is not safe need to review it
    let callback_concurrency = if config.index_event_in_order() {
        1usize
    } else {
        config.config().callback_concurrency.unwrap_or(1)
    };

    let callback_permits = Arc::new(Semaphore::new(callback_concurrency));

    let mut logs_stream = fetch_logs_stream(Arc::clone(&config), force_no_live_indexing);
    let mut tasks = Vec::new();

    while let Some(result) = logs_stream.next().await {
        let task = handle_logs_result(Arc::clone(&config), callback_permits.clone(), result)
            .await
            .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;

        if block_until_indexed {
            task.await.map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;
        } else {
            tasks.push(task);
        }
    }

    ensure_logs_stream_end_is_expected(
        config.live_indexing(),
        force_no_live_indexing,
        &config.info_log_name(),
        &config.detail_key(),
    )?;

    // Wait for all remaining tasks to complete
    if !tasks.is_empty() {
        futures::future::try_join_all(tasks)
            .await
            .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;
    }

    Ok(())
}

fn ensure_logs_stream_end_is_expected(
    live_indexing: bool,
    force_no_live_indexing: bool,
    info_log_name: &str,
    detail_key: &str,
) -> Result<(), Box<ProviderError>> {
    if live_indexing && !force_no_live_indexing {
        return Err(Box::new(ProviderError::CustomError(format!(
            "{info_log_name} - live logs stream ended unexpectedly for detail {detail_key}"
        ))));
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractsEventsWithDependenciesError {
    #[error("{0}")]
    ProcessContractEventsWithDependenciesError(#[from] ProcessContractEventsWithDependenciesError),

    #[error("{0}")]
    JoinError(#[from] JoinError),
}

pub async fn process_contracts_events_with_dependencies(
    contracts_events_config: Vec<ContractEventsDependenciesConfig>,
) -> Result<(), ProcessContractsEventsWithDependenciesError> {
    let mut handles: Vec<JoinHandle<Result<(), ProcessContractEventsWithDependenciesError>>> =
        Vec::new();

    for contract_events in contracts_events_config {
        let handle = tokio::spawn(async move {
            process_contract_events_with_dependencies(
                contract_events.event_dependencies,
                Arc::new(contract_events.events_config),
            )
            .await
        });
        handles.push(handle);
    }

    let results = join_all(handles).await;

    for result in results {
        match result {
            Ok(inner_result) => inner_result?,
            Err(join_error) => {
                return Err(ProcessContractsEventsWithDependenciesError::JoinError(join_error))
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractEventsWithDependenciesError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),

    #[error("Event config not found for contract: {0} and event: {1}")]
    EventConfigNotFound(String, String),

    #[error("Could not run all the logs processes {0}")]
    JoinError(#[from] JoinError),
}

#[derive(Clone)]
pub struct OrderedLiveIndexingDetails {
    pub filter: RindexerEventFilter,
    pub last_seen_block_number: U64,
    pub last_no_new_block_log_time: Instant,
}

async fn process_contract_events_with_dependencies(
    dependencies: EventDependencies,
    events_processing_config: Arc<Vec<Arc<EventProcessingConfig>>>,
) -> Result<(), ProcessContractEventsWithDependenciesError> {
    let mut stack = vec![dependencies.tree];

    let live_indexing_events =
        Arc::new(Mutex::new(HashMap::<String, EventDependenciesIndexingConfig>::new()));

    while let Some(current_tree) = stack.pop() {
        let mut tasks = vec![];

        for dependency in &current_tree.contract_events {
            // multi network can have many of the same event names so we need to get them all
            let event_processing_configs = events_processing_config
                .iter()
                .filter(|e| {
                    // TODO - this is a hacky way to check if it's a filter event
                    (e.contract_name() == dependency.contract_name
                        || e.contract_name().replace("Filter", "") == dependency.contract_name)
                        && e.event_name() == dependency.event_name
                })
                .cloned()
                .collect::<Vec<Arc<EventProcessingConfig>>>();

            for event_processing_config in event_processing_configs {
                let task = tokio::spawn({
                    let live_indexing_events = Arc::clone(&live_indexing_events);
                    async move {
                        process_blocking_event_historical_data(Arc::clone(
                            &event_processing_config,
                        ))
                        .await?;

                        if event_processing_config.live_indexing() {
                            let network_contract = event_processing_config.network_contract();

                            let mut live_indexing_events = live_indexing_events.lock().await;
                            let entry = live_indexing_events
                                .entry(network_contract.network.clone())
                                .or_insert_with(|| EventDependenciesIndexingConfig {
                                    network: network_contract.network.clone(),
                                    cached_provider: network_contract.cached_provider.clone(),
                                    events: Vec::new(),
                                });

                            let rindexer_event_filter =
                                event_processing_config.to_event_filter()?;

                            entry.events.push((
                                Arc::clone(&event_processing_config),
                                rindexer_event_filter,
                            ));
                        }

                        Ok::<(), ProcessContractEventsWithDependenciesError>(())
                    }
                });
                tasks.push(task);
            }
        }

        let results = join_all(tasks).await;
        for result in results {
            match result {
                Ok(result) => match result {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error processing logs due to dependencies error: {:?}", e);
                        return Err(e);
                    }
                },
                Err(e) => {
                    error!("Error processing logs: {:?}", e);
                    return Err(ProcessContractEventsWithDependenciesError::JoinError(e));
                }
            }
        }

        // If there are more dependencies to process, push the next level onto the stack
        if let Some(next_tree) = &*current_tree.then {
            stack.push(Arc::clone(next_tree));
        }
    }

    let live_indexing_events = live_indexing_events.lock().await;
    if live_indexing_events.is_empty() {
        return Ok(());
    }

    let live_indexing_tasks = live_indexing_events
        .values()
        .map(|config| tokio::spawn(live_indexing_for_contract_event_dependencies(config.clone())))
        .collect::<Vec<_>>();

    futures::future::try_join_all(live_indexing_tasks).await?;

    Ok(())
}

#[derive(Clone)]
pub struct EventDependenciesIndexingConfig {
    pub network: String,
    pub cached_provider: Arc<JsonRpcCachedProvider>,
    pub events: Vec<(Arc<EventProcessingConfig>, RindexerEventFilter)>,
}

// TODO - this is a similar to live_indexing_stream but has to be a bit different we should merge
// code
#[allow(clippy::type_complexity)]
async fn live_indexing_for_contract_event_dependencies(
    EventDependenciesIndexingConfig { cached_provider, events, network }: EventDependenciesIndexingConfig,
) {
    debug!(
        "Live indexing events on {} in order: {}",
        network,
        events
            .iter()
            .map(|(config, _)| format!("{}::{}", config.contract_name(), config.event_name()))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut ordering_live_indexing_details_map: HashMap<
        B256,
        Arc<Mutex<OrderedLiveIndexingDetails>>,
    > = HashMap::with_capacity(events.len());

    for (config, event_filter) in events.iter() {
        let mut filter = event_filter.clone();
        let last_seen_block_number = filter.to_block();
        let next_block_number = last_seen_block_number + U64::from(1);

        filter = filter.set_from_block(next_block_number).set_to_block(next_block_number);

        ordering_live_indexing_details_map.insert(
            config.id(),
            Arc::new(Mutex::new(OrderedLiveIndexingDetails {
                filter,
                last_seen_block_number,
                last_no_new_block_log_time: Instant::now(),
            })),
        );
    }

    // this is used for less busy chains to make sure they know rindexer is still alive
    let log_no_new_block_interval = Duration::from_secs(300);
    let target_iteration_duration = Duration::from_millis(200);
    let callback_permits = Arc::new(Semaphore::new(1));

    loop {
        if !is_running() {
            break;
        }

        let iteration_start = Instant::now();

        // a consistent latest block number across all events in the batch is required to avoid race conditions
        let latest_block = match cached_provider.get_latest_block().await {
            Ok(Some(block)) => block,
            Ok(None) => {
                error!("Empty latest block returned from provider, will try again in 200ms");

                tokio::time::sleep(Duration::from_millis(200)).await;

                continue;
            }
            Err(error) => {
                error!(
                    "Failed to get latest block, will try again in 1 second - error: {}",
                    error.to_string()
                );

                tokio::time::sleep(Duration::from_secs(1)).await;

                continue;
            }
        };
        let latest_block_number = U64::from(latest_block.header.number);

        for (config, _) in events.iter() {
            let mut ordering_live_indexing_details = ordering_live_indexing_details_map
                .get(&config.id())
                .expect("Failed to get ordering_live_indexing_details_map")
                .lock()
                .await
                .clone();

            if ordering_live_indexing_details.last_seen_block_number == latest_block_number {
                debug!(
                    "{} - {} - No new blocks to process...",
                    &config.info_log_name(),
                    IndexingEventProgressStatus::Live.log()
                );
                if ordering_live_indexing_details.last_no_new_block_log_time.elapsed()
                    >= log_no_new_block_interval
                {
                    info!(
                        "{} - {} - No new blocks published in the last 5 minutes - latest block number {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        latest_block_number
                    );
                    ordering_live_indexing_details.last_no_new_block_log_time = Instant::now();
                    *ordering_live_indexing_details_map
                        .get(&config.id())
                        .expect("Failed to get ordering_live_indexing_details_map")
                        .lock()
                        .await = ordering_live_indexing_details;
                }
                continue;
            }
            debug!(
                "{} - {} - New block seen {} - Last seen block {}",
                &config.info_log_name(),
                IndexingEventProgressStatus::Live.log(),
                latest_block_number,
                ordering_live_indexing_details.last_seen_block_number
            );
            let reorg_safe_distance = &config.indexing_distance_from_head();
            let safe_block_number = latest_block_number - reorg_safe_distance;
            let from_block = ordering_live_indexing_details.filter.from_block();

            // check reorg distance and skip if not safe
            if from_block > safe_block_number {
                if reorg_safe_distance.is_zero() {
                    let block_distance = from_block - latest_block_number;
                    let is_outside_reorg_range =
                        block_distance > reorg_safe_distance_for_chain(cached_provider.chain.id());

                    // it should never get under normal conditions outside the reorg range,
                    // therefore, we log an error as means RCP state is not in sync with the blockchain
                    if is_outside_reorg_range {
                        error!(
                            "{} - {} - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                            &config.info_log_name(),
                            IndexingEventProgressStatus::Live.log(),
                            latest_block_number,
                            from_block
                        );
                    } else {
                        info!(
                            "{} - {} - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                            &config.info_log_name(),
                            IndexingEventProgressStatus::Live.log(),
                            latest_block_number,
                            from_block
                        );
                    }

                    continue;
                } else {
                    info!(
                        "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        from_block,
                        safe_block_number
                    );
                    continue;
                }
            }

            let to_block = safe_block_number;
            // A Bloom-proven empty singleton skips the getLogs RPC but still flows
            // through the shared consumer path below so durable progress advances.
            // The latest block header's Bloom is only valid when the safe range ends
            // exactly at the latest block; with a reorg distance the safe head is
            // older and must always be queried.
            let bloom_proven_empty = from_block == to_block
                && to_block == latest_block_number
                && !config.network_contract().disable_logs_bloom_checks
                && !is_relevant_block(
                    &ordering_live_indexing_details.filter.contract_addresses().await,
                    &config.topic_id(),
                    &latest_block,
                );
            if bloom_proven_empty {
                debug!(
                    "{} - {} - Skipping block {} as it's not relevant",
                    &config.info_log_name(),
                    IndexingEventProgressStatus::Live.log(),
                    from_block
                );
                debug!(
                    "{} - {} - Did not need to hit RPC as no events in {} block - LogsBloom for block checked",
                    &config.info_log_name(),
                    IndexingEventProgressStatus::Live.log(),
                    from_block
                );
            }

            ordering_live_indexing_details.filter =
                ordering_live_indexing_details.filter.set_to_block(to_block);

            debug!(
                "{} - {} - Processing live filter: {:?}",
                &config.info_log_name(),
                IndexingEventProgressStatus::Live.log(),
                ordering_live_indexing_details.filter
            );

            let logs_result = if bloom_proven_empty {
                Ok(Vec::new())
            } else {
                cached_provider.get_logs(&ordering_live_indexing_details.filter).await
            };
            match logs_result {
                Ok(logs) => {
                    debug!(
                        "{} - {} - Live id {} topic_id {}, Logs: {} from {} to {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        &config.id(),
                        &config.topic_id(),
                        logs.len(),
                        from_block,
                        to_block
                    );

                    debug!(
                        "{} - {} - Fetched {} event logs - blocks: {} - {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        logs.len(),
                        from_block,
                        to_block
                    );

                    let logs_empty = logs.is_empty();
                    // clone here over the full logs way less overhead
                    let last_log = logs.last().cloned();

                    let fetched_logs = Ok(FetchLogsResult { logs, from_block, to_block });

                    let result = handle_logs_result(
                        Arc::clone(config),
                        callback_permits.clone(),
                        fetched_logs,
                    )
                    .await;

                    match result {
                        Ok(task) => {
                            let complete = task.await;
                            if let Err(e) = complete {
                                error!(
                                    "{} - {} - Error indexing task: {} - will try again in 200ms",
                                    &config.info_log_name(),
                                    IndexingEventProgressStatus::Live.log(),
                                    e
                                );
                                break;
                            }
                            ordering_live_indexing_details.last_seen_block_number = to_block;
                            if logs_empty {
                                ordering_live_indexing_details.filter =
                                    ordering_live_indexing_details
                                        .filter
                                        .set_from_block(to_block + U64::from(1));
                                debug!(
                                    "{} - {} - No events found between blocks {} - {}",
                                    &config.info_log_name(),
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block,
                                    to_block
                                );
                            } else if let Some(last_log) = last_log {
                                if let Some(last_log_block_number) = last_log.block_number {
                                    ordering_live_indexing_details.filter =
                                        ordering_live_indexing_details
                                            .filter
                                            .set_from_block(U64::from(last_log_block_number + 1));
                                } else {
                                    error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                }
                            }

                            *ordering_live_indexing_details_map
                                .get(&config.id())
                                .expect("Failed to get ordering_live_indexing_details_map")
                                .lock()
                                .await = ordering_live_indexing_details;
                        }
                        Err(err) => {
                            error!(
                                "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                &config.info_log_name(),
                                IndexingEventProgressStatus::Live.log(),
                                err
                            );
                            break;
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "{} - {} - Error fetching logs: {} - will try again in 200ms",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        err
                    );
                    break;
                }
            }
        }

        let elapsed = iteration_start.elapsed();
        if elapsed < target_iteration_duration {
            tokio::time::sleep(target_iteration_duration - elapsed).await;
        }
    }
}

async fn trigger_event(
    config: Arc<EventProcessingConfig>,
    fn_data: Vec<EventResult>,
    to_block: U64,
) {
    indexing_event_processing();

    // Record events processed metric
    let event_count = fn_data.len() as u64;
    if event_count > 0 {
        metrics::record_events_indexed(
            &config.network_contract().network,
            &config.contract_name(),
            &config.event_name(),
            event_count,
            to_block.to::<u64>(),
            None, // Latest chain block updated elsewhere
        );
    }

    let should_update_progress = if fn_data.is_empty() {
        #[allow(clippy::needless_bool)]
        if !is_running() {
            false
        } else {
            true
        }
    } else {
        config.trigger_event(fn_data.clone()).await.is_ok()
    };

    if should_update_progress {
        // TODO: There is a double-index race condition here. If we get a crash or failure between
        //       triggering the event and syncing the last updated block, we may double index.
        update_progress_and_last_synced_task(config, to_block, indexing_event_processed).await;
    } else {
        indexing_event_processed();
    }
}

async fn handle_logs_result(
    config: Arc<EventProcessingConfig>,
    callback_permits: Arc<Semaphore>,
    result: Result<FetchLogsResult, Box<dyn std::error::Error + Send>>,
) -> Result<JoinHandle<()>, Box<dyn std::error::Error + Send>> {
    match result {
        Ok(result) => {
            debug!("{} - Processing {} logs", config.info_log_name(), result.logs.len());

            let fn_data = result
                .logs
                .into_iter()
                .map(|log| {
                    EventResult::new(
                        Arc::clone(&config.network_contract()),
                        log,
                        result.from_block,
                        result.to_block,
                    )
                })
                .collect::<Vec<_>>();

            if let Ok(permit) = callback_permits.clone().acquire_owned().await {
                let task = tokio::spawn(async move {
                    trigger_event(config, fn_data, result.to_block).await;
                    drop(permit)
                });

                Ok(task)
            } else {
                trigger_event(config, fn_data, result.to_block).await;
                Ok(tokio::spawn(async {}))
            }
        }
        Err(e) => {
            error!(
                "[{}] - {} - {} - Error fetching logs: {}",
                config.network_contract().network,
                config.event_name(),
                IndexingEventProgressStatus::Live.log(),
                e
            );
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ensure_logs_stream_end_is_expected;

    #[test]
    fn live_stream_eof_returns_an_identified_error() {
        let error = ensure_logs_stream_end_is_expected(
            true,
            false,
            "WalletERC20Transfers::Transfer::mainnet",
            "0xabc:i1:0xwallet",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("WalletERC20Transfers::Transfer::mainnet"));
        assert!(error.contains("0xabc:i1:0xwallet"));
        assert!(error.contains("live logs stream ended unexpectedly"));
    }

    #[test]
    fn finite_and_forced_historical_stream_eof_are_successful() {
        assert!(ensure_logs_stream_end_is_expected(false, false, "event", "detail").is_ok());
        assert!(ensure_logs_stream_end_is_expected(true, true, "event", "detail").is_ok());
    }

    use std::any::Any;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use alloy::network::AnyRpcBlock;
    use alloy::primitives::{Address, Bloom, Bytes, TxHash, B256, U64};
    use alloy::rpc::types::ValueOrArray;
    use axum::{extract::State, routing::post, Json, Router};
    use tokio::net::TcpListener;
    use tokio::sync::RwLock;
    use tokio::task::JoinHandle;
    use tokio::time::Instant;

    use super::*;
    use crate::blockclock::BlockClock;
    use crate::database::generate::generate_indexer_contract_schema_name;
    use crate::database::postgres::generate::generate_internal_event_table_name;
    use crate::event::callback_registry::EventCallbackRegistry;
    use crate::event::config::{ContractEventProcessingConfig, EventProcessingConfig};
    use crate::event::contract_setup::{AddressDetails, IndexingContractSetup, NetworkContract};
    use crate::indexer::last_synced::{
        get_last_synced_block_number, postgres_cursor_upsert_query, SyncConfig,
    };
    use crate::indexer::progress::IndexingEventsProgressState;
    use crate::manifest::config::Config;
    use crate::manifest::network::BlockPollFrequency;
    use crate::{EthereumSqlTypeWrapper, PostgresClient};

    static TEST_INDEXER_SEQ: AtomicUsize = AtomicUsize::new(1);

    fn make_test_block(number: u64, bloom: Bloom) -> AnyRpcBlock {
        let mut block = AnyRpcBlock::new(Default::default());
        block.header.number = number;
        block.header.timestamp = 1_700_000_000 + number;
        block.header.logs_bloom = bloom;
        block
    }

    struct MockRpcState {
        chain_id: u64,
        current_block: RwLock<AnyRpcBlock>,
        block_request_count: AtomicUsize,
        logs_request_count: AtomicUsize,
        last_log_request_from: AtomicU64,
        last_log_request_to: AtomicU64,
    }

    impl MockRpcState {
        fn new(chain_id: u64, initial_block: AnyRpcBlock) -> Self {
            Self {
                chain_id,
                current_block: RwLock::new(initial_block),
                block_request_count: AtomicUsize::new(0),
                logs_request_count: AtomicUsize::new(0),
                last_log_request_from: AtomicU64::new(0),
                last_log_request_to: AtomicU64::new(0),
            }
        }

        async fn handle_rpc(&self, req: &serde_json::Value) -> serde_json::Value {
            let id = req.get("id").cloned().unwrap_or(serde_json::json!(1));
            let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");

            match method {
                "eth_chainId" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": format!("0x{:x}", self.chain_id)
                }),
                "net_version" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": self.chain_id.to_string()
                }),
                "eth_getBlockByNumber" => {
                    self.block_request_count.fetch_add(1, Ordering::SeqCst);
                    let block = self.current_block.read().await.clone();
                    let block_val = serde_json::to_value(&block).unwrap_or(serde_json::Value::Null);
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": block_val
                    })
                }
                "eth_getLogs" => {
                    self.logs_request_count.fetch_add(1, Ordering::SeqCst);
                    if let Some(params) = req.get("params").and_then(|p| p.as_array()) {
                        if let Some(filter) = params.first() {
                            let from_hex =
                                filter.get("fromBlock").and_then(|v| v.as_str()).unwrap_or("");
                            let to_hex =
                                filter.get("toBlock").and_then(|v| v.as_str()).unwrap_or("");
                            let from = u64::from_str_radix(from_hex.trim_start_matches("0x"), 16)
                                .unwrap_or(0);
                            let to = u64::from_str_radix(to_hex.trim_start_matches("0x"), 16)
                                .unwrap_or(0);
                            if from == 0 || to == 0 || from > to {
                                return serde_json::json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "error": {
                                        "code": -32602,
                                        "message": format!(
                                            "Invalid block range in eth_getLogs: from={} to={}",
                                            from, to
                                        )
                                    }
                                });
                            }
                            self.last_log_request_from.store(from, Ordering::SeqCst);
                            self.last_log_request_to.store(to, Ordering::SeqCst);
                        }
                    }
                    serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": []
                    })
                }
                other => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": -32601,
                        "message": format!("Method not found or unexpected in test mock: {}", other)
                    }
                }),
            }
        }
    }

    async fn rpc_handler(
        State(state): State<Arc<MockRpcState>>,
        Json(payload): Json<serde_json::Value>,
    ) -> Json<serde_json::Value> {
        if let Some(batch) = payload.as_array() {
            let mut responses = Vec::with_capacity(batch.len());
            for req in batch {
                responses.push(state.handle_rpc(req).await);
            }
            Json(serde_json::Value::Array(responses))
        } else {
            Json(state.handle_rpc(&payload).await)
        }
    }

    /// Shared fixture for the PostgreSQL-backed live-indexing tests below.
    ///
    /// These tests are explicit opt-in: run them with an isolated endpoint via
    /// `cargo test -p rindexer --lib indexer::process::tests -- --include-ignored`.
    struct TestFixture {
        rpc_state: Arc<MockRpcState>,
        server_handle: JoinHandle<()>,
        postgres: Arc<PostgresClient>,
        table_name: String,
        network: String,
        detail_key: String,
        config: Arc<EventProcessingConfig>,
        cached_provider: Arc<JsonRpcCachedProvider>,
    }

    impl TestFixture {
        async fn new(disable_logs_bloom_checks: bool, initial_cursor: u64) -> Self {
            Self::new_with_distance(disable_logs_bloom_checks, initial_cursor, 0).await
        }

        async fn new_with_distance(
            disable_logs_bloom_checks: bool,
            initial_cursor: u64,
            reorg_distance: u64,
        ) -> Self {
            let _ = std::env::var("DATABASE_URL")
                .or_else(|_| std::env::var("TEST_DATABASE_URL"))
                .expect(
                    "DATABASE_URL must be provided to run the real PostgreSQL durable cursor test. Main will supply an isolated endpoint."
                );

            let postgres = Arc::new(
                PostgresClient::new()
                    .await
                    .expect("Failed to connect to PostgreSQL with DATABASE_URL"),
            );

            let seq = TEST_INDEXER_SEQ.fetch_add(1, Ordering::SeqCst);
            let indexer_name = format!("test_bloom_repro_{}", seq);
            let contract_name = "TestContract".to_string();
            let event_name = "Transfer".to_string();
            let network = "ethereum".to_string();

            let block_101 = make_test_block(101, Bloom::ZERO);
            let rpc_state = Arc::new(MockRpcState::new(1, block_101));

            let app =
                Router::new().route("/", post(rpc_handler)).with_state(Arc::clone(&rpc_state));
            let listener =
                TcpListener::bind("127.0.0.1:0").await.expect("Failed to bind mock rpc listener");
            let addr = listener.local_addr().expect("Failed to get local addr");
            let server_handle = tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let rpc_url = format!("http://{}", addr);

            let cached_provider = crate::provider::create_client(
                &rpc_url,
                1,
                None,
                None,
                Some(BlockPollFrequency::PollRateMs { millis: 5 }),
                reqwest::header::HeaderMap::new(),
                None,
                None,
            )
            .await
            .expect("Failed to create cached provider");

            let contract_address = Address::repeat_byte(0x42);
            let topic_id = B256::repeat_byte(0xbb);

            let decoder = Arc::new(|_topics: Vec<TxHash>, _data: Bytes| {
                Arc::new(()) as Arc<dyn Any + Send + Sync>
            });

            let network_contract = Arc::new(NetworkContract {
                id: format!("{}-{}-{}", indexer_name, contract_name, network),
                network: network.clone(),
                indexing_contract_setup: IndexingContractSetup::Address(AddressDetails {
                    address: ValueOrArray::Value(contract_address),
                    indexed_filters: None,
                }),
                cached_provider: Arc::clone(&cached_provider),
                block_clock: BlockClock::new(Some(false), None, Arc::clone(&cached_provider)),
                decoder,
                start_block: Some(U64::from(101)),
                end_block: None,
                disable_logs_bloom_checks,
            });

            let config = Arc::new(EventProcessingConfig::ContractEventProcessing(
                ContractEventProcessingConfig {
                    id: format!("{}-{}-{}", indexer_name, contract_name, event_name),
                    project_path: Path::new("/tmp").to_path_buf(),
                    indexer_name: indexer_name.clone(),
                    contract_name: contract_name.clone(),
                    topic_id,
                    event_name: event_name.clone(),
                    config: Config::default(),
                    network_contract: Arc::clone(&network_contract),
                    timestamps: false,
                    start_block: U64::from(101),
                    end_block: U64::from(100), // historical drained (from > to)
                    registry: Arc::new(EventCallbackRegistry::new()),
                    progress: Arc::new(tokio::sync::Mutex::new(IndexingEventsProgressState {
                        events: Vec::new(),
                    })),
                    postgres: Some(Arc::clone(&postgres)),
                    clickhouse: None,
                    csv_details: None,
                    stream_last_synced_block_file_path: None,
                    index_event_in_order: false,
                    live_indexing: true,
                    indexing_distance_from_head: U64::from(reorg_distance),
                },
            ));

            let schema = generate_indexer_contract_schema_name(
                &config.indexer_name(),
                &config.contract_name(),
            );
            let table_name = generate_internal_event_table_name(&schema, &config.event_name());
            let detail_key = config.detail_key();

            postgres
                .batch_execute(&format!(
                    r#"
                    CREATE SCHEMA IF NOT EXISTS rindexer_internal;
                    CREATE TABLE IF NOT EXISTS rindexer_internal.latest_block (
                        "network" TEXT PRIMARY KEY,
                        "block" NUMERIC
                    );
                    CREATE TABLE IF NOT EXISTS rindexer_internal.{table_name} (
                        "network" TEXT NOT NULL,
                        "detail_key" TEXT NOT NULL DEFAULT '__event__',
                        "last_synced_block" NUMERIC,
                        PRIMARY KEY ("network", "detail_key")
                    );
                    DELETE FROM rindexer_internal.{table_name} WHERE network = '{network}' AND detail_key = '{detail_key}';
                    "#
                ))
                .await
                .expect("Failed to initialize test table in PostgreSQL");

            postgres
                .execute(
                    &postgres_cursor_upsert_query(&table_name),
                    &[&network, &detail_key, &EthereumSqlTypeWrapper::U64(initial_cursor)],
                )
                .await
                .expect("Failed to seed initial cursor C in PostgreSQL");

            Self {
                rpc_state,
                server_handle,
                postgres,
                table_name,
                network,
                detail_key,
                config,
                cached_provider,
            }
        }

        fn reset_request_counters(&self) {
            self.rpc_state.block_request_count.store(0, Ordering::SeqCst);
            self.rpc_state.logs_request_count.store(0, Ordering::SeqCst);
            self.rpc_state.last_log_request_from.store(0, Ordering::SeqCst);
            self.rpc_state.last_log_request_to.store(0, Ordering::SeqCst);
        }

        async fn read_persisted_cursor(&self) -> Option<U64> {
            get_last_synced_block_number(SyncConfig {
                project_path: Path::new("/tmp"),
                postgres: &Some(Arc::clone(&self.postgres)),
                clickhouse: &None,
                csv_details: &None,
                stream_details: &None,
                contract_csv_enabled: false,
                indexer_name: &self.config.indexer_name(),
                contract_name: &self.config.contract_name(),
                event_name: &self.config.event_name(),
                network: &self.network,
                detail_key: &self.detail_key,
            })
            .await
        }

        async fn wait_for_block_requests(&self, min_count: usize, timeout: Duration) -> bool {
            let start = Instant::now();
            while start.elapsed() < timeout {
                if self.rpc_state.block_request_count.load(Ordering::SeqCst) >= min_count {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            false
        }

        async fn wait_for_log_requests(&self, min_count: usize, timeout: Duration) -> bool {
            let start = Instant::now();
            while start.elapsed() < timeout {
                if self.rpc_state.logs_request_count.load(Ordering::SeqCst) >= min_count {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            false
        }

        async fn tear_down(self) {
            let _ = self
                .postgres
                .batch_execute(&format!(
                    "DROP TABLE IF EXISTS rindexer_internal.{} CASCADE;",
                    self.table_name
                ))
                .await;
            self.server_handle.abort();
        }
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL DATABASE_URL"]
    async fn test_live_bloom_skip_persists_skipped_cursor_without_get_logs() {
        // Starts with drained work and initial cursor C = 100.
        // Head B = 101 has Bloom::ZERO (irrelevant), with Bloom skipping enabled.
        // The Bloom-proven empty range must advance the durable cursor through the
        // normal consumer path, without a getLogs RPC.
        let fixture = TestFixture::new(false, 100).await;
        assert_eq!(fixture.read_persisted_cursor().await, Some(U64::from(100)));

        // Reset request counters immediately before running the live loop so counts
        // are strictly attributable to the loop's iterations.
        fixture.reset_request_counters();

        let handle = tokio::spawn({
            let config = Arc::clone(&fixture.config);
            async move {
                let _ = process_event_logs(config, false, false).await;
            }
        });

        // Wait until live loop has completed iteration 1 (skipped block 101) and commenced iteration 2
        let reached = fixture.wait_for_block_requests(2, Duration::from_secs(5)).await;
        assert!(reached, "Expected at least 2 get_block requests from live loop");

        // Await durable cursor convergence instead of racing async persistence.
        let mut persisted = None;
        for _ in 0..100 {
            persisted = fixture.read_persisted_cursor().await;
            if persisted == Some(U64::from(101)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let log_reqs = fixture.rpc_state.logs_request_count.load(Ordering::SeqCst);
        handle.abort();
        fixture.tear_down().await;

        assert_eq!(log_reqs, 0, "Bloom-proven empty block 101 must not trigger a getLogs RPC");
        assert_eq!(
            persisted,
            Some(U64::from(101)),
            "Bloom-proven empty block B=101 must advance the durable cursor from C=100"
        );
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL DATABASE_URL"]
    async fn test_control_live_bloom_skip_disabled_persists_empty_block() {
        // Discriminating control: disable Bloom skipping (disable_logs_bloom_checks = true).
        // Head B = 101 returns empty getLogs result; real consumer MUST persist B without an event row.
        let fixture = TestFixture::new(true, 100).await;
        assert_eq!(fixture.read_persisted_cursor().await, Some(U64::from(100)));

        fixture.reset_request_counters();

        let handle = tokio::spawn({
            let config = Arc::clone(&fixture.config);
            async move {
                let _ = process_event_logs(config, false, false).await;
            }
        });

        let reached = fixture.wait_for_log_requests(1, Duration::from_secs(5)).await;
        assert!(reached, "Expected getLogs request for block 101");
        assert_eq!(fixture.rpc_state.last_log_request_from.load(Ordering::SeqCst), 101);
        assert_eq!(fixture.rpc_state.last_log_request_to.load(Ordering::SeqCst), 101);

        let mut persisted = None;
        for _ in 0..100 {
            persisted = fixture.read_persisted_cursor().await;
            if persisted == Some(U64::from(101)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        handle.abort();
        fixture.tear_down().await;

        // On current source, this PASSES:
        assert_eq!(
            persisted,
            Some(U64::from(101)),
            "CONTROL CONFIRMED: When Bloom checks are disabled, consumer persists block B=101 without event rows"
        );
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL DATABASE_URL"]
    async fn test_control_live_stream_catches_up_after_bloom_skip() {
        // Shows that a subsequent non-skipped empty range catches up after the skip,
        // proving this is not a stopped or deadlocked stream.
        let fixture = TestFixture::new(false, 100).await;
        assert_eq!(fixture.read_persisted_cursor().await, Some(U64::from(100)));

        fixture.reset_request_counters();

        let handle = tokio::spawn({
            let config = Arc::clone(&fixture.config);
            async move {
                let _ = process_event_logs(config, false, false).await;
            }
        });

        // 1. Block 101 is Bloom-proven empty: no getLogs, but cursor converges to 101
        let reached_101 = fixture.wait_for_block_requests(2, Duration::from_secs(5)).await;
        assert!(reached_101);
        let mut after_skip = None;
        for _ in 0..100 {
            after_skip = fixture.read_persisted_cursor().await;
            if after_skip == Some(U64::from(101)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(after_skip, Some(U64::from(101)));

        // 2. Supply next head B+1 = 102 with relevant Bloom (all bits set)
        let block_102 = make_test_block(102, Bloom::repeat_byte(0xff));
        *fixture.rpc_state.current_block.write().await = block_102;

        // 3. Wait for block 102 to be queried via getLogs
        let reached_102 = fixture.wait_for_log_requests(1, Duration::from_secs(5)).await;
        assert!(reached_102, "Block 102 should trigger getLogs request");
        assert_eq!(fixture.rpc_state.last_log_request_from.load(Ordering::SeqCst), 102);
        assert_eq!(fixture.rpc_state.last_log_request_to.load(Ordering::SeqCst), 102);

        let mut persisted = None;
        for _ in 0..100 {
            persisted = fixture.read_persisted_cursor().await;
            if persisted == Some(U64::from(102)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        handle.abort();
        fixture.tear_down().await;

        // On fixed source, this PASSES:
        assert_eq!(
            persisted,
            Some(U64::from(102)),
            "CATCH-UP CONFIRMED: cursor advanced 100 -> 101 on the Bloom-proven empty block, then to 102 on the subsequent non-skipped block"
        );
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL DATABASE_URL"]
    async fn test_dependency_ordered_bloom_skip_regression_and_control() {
        // Covers the analogous dependency-ordered skip path in live_indexing_for_contract_event_dependencies.

        // Case A: Bloom skip enabled -> cursor still advances to 101 through the
        // consumer path, without a getLogs RPC (fixed regression)
        {
            let fixture = TestFixture::new(false, 100).await;
            let filter = fixture
                .config
                .to_event_filter()
                .expect("event filter")
                .set_to_block(U64::from(100));
            let dep_config = EventDependenciesIndexingConfig {
                cached_provider: Arc::clone(&fixture.cached_provider),
                events: vec![(Arc::clone(&fixture.config), filter)],
                network: fixture.network.clone(),
            };

            fixture.reset_request_counters();

            let handle = tokio::spawn(async move {
                live_indexing_for_contract_event_dependencies(dep_config).await;
            });

            let reached = fixture.wait_for_block_requests(2, Duration::from_secs(5)).await;
            assert!(reached);

            // Await durable cursor convergence instead of racing async persistence.
            let mut persisted = None;
            for _ in 0..100 {
                persisted = fixture.read_persisted_cursor().await;
                if persisted == Some(U64::from(101)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let log_reqs = fixture.rpc_state.logs_request_count.load(Ordering::SeqCst);
            handle.abort();
            fixture.tear_down().await;

            assert_eq!(log_reqs, 0, "Bloom-proven empty block must not trigger a getLogs RPC");
            assert_eq!(
                persisted,
                Some(U64::from(101)),
                "Dependency-ordered path must persist the Bloom-proven empty block to 101"
            );
        }

        // Case B: Bloom skip disabled -> cursor advances to 101 (discriminating control)
        {
            let fixture = TestFixture::new(true, 100).await;
            let filter = fixture
                .config
                .to_event_filter()
                .expect("event filter")
                .set_to_block(U64::from(100));
            let dep_config = EventDependenciesIndexingConfig {
                cached_provider: Arc::clone(&fixture.cached_provider),
                events: vec![(Arc::clone(&fixture.config), filter)],
                network: fixture.network.clone(),
            };

            fixture.reset_request_counters();

            let handle = tokio::spawn(async move {
                live_indexing_for_contract_event_dependencies(dep_config).await;
            });

            let reached = fixture.wait_for_log_requests(1, Duration::from_secs(5)).await;
            assert!(reached);
            assert_eq!(fixture.rpc_state.last_log_request_from.load(Ordering::SeqCst), 101);
            assert_eq!(fixture.rpc_state.last_log_request_to.load(Ordering::SeqCst), 101);

            let mut persisted = None;
            for _ in 0..100 {
                persisted = fixture.read_persisted_cursor().await;
                if persisted == Some(U64::from(101)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            handle.abort();
            fixture.tear_down().await;

            assert_eq!(
                persisted,
                Some(U64::from(101)),
                "Dependency-ordered path persists cursor to 101 when Bloom checks are disabled"
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires an isolated PostgreSQL DATABASE_URL"]
    async fn test_dependency_ordered_safe_head_mismatch_always_queries() {
        // With a reorg distance of 1, latest = 101 but safe head = 100. The Bloom of
        // the latest header must NOT authorize skipping the older safe block: the
        // range [100, 100] must be queried via getLogs and persisted.
        let fixture = TestFixture::new_with_distance(false, 99, 1).await;
        assert_eq!(fixture.read_persisted_cursor().await, Some(U64::from(99)));
        let filter =
            fixture.config.to_event_filter().expect("event filter").set_to_block(U64::from(99));
        let dep_config = EventDependenciesIndexingConfig {
            cached_provider: Arc::clone(&fixture.cached_provider),
            events: vec![(Arc::clone(&fixture.config), filter)],
            network: fixture.network.clone(),
        };

        fixture.reset_request_counters();

        let handle = tokio::spawn(async move {
            live_indexing_for_contract_event_dependencies(dep_config).await;
        });

        let reached = fixture.wait_for_log_requests(1, Duration::from_secs(5)).await;
        assert!(reached, "Safe block 100 older than latest 101 must be queried via getLogs");
        assert_eq!(fixture.rpc_state.last_log_request_from.load(Ordering::SeqCst), 100);
        assert_eq!(fixture.rpc_state.last_log_request_to.load(Ordering::SeqCst), 100);

        let mut persisted = None;
        for _ in 0..100 {
            persisted = fixture.read_persisted_cursor().await;
            if persisted == Some(U64::from(100)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        handle.abort();
        fixture.tear_down().await;

        assert_eq!(
            persisted,
            Some(U64::from(100)),
            "Safe-head/latest-header mismatch must query and persist block 100, never Bloom-skip it"
        );
    }
}

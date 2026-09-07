CREATE TABLE IF NOT EXISTS market_data.pipeline_runs (
  run_id text PRIMARY KEY,
  dataset_id text,
  state text,
  expected_counts map<text, bigint>,
  start_offsets_inclusive map<int, bigint>,
  end_offsets_exclusive map<int, bigint>,
  data_contract_status text,
  portfolio_release_status text,
  data_contract_code int,
  portfolio_release_code int,
  created_at timestamp,
  started_at timestamp,
  failure_at timestamp,
  recovered_at timestamp,
  completed_at timestamp,
  state_reason text
);

CREATE TABLE IF NOT EXISTS market_data.bronze_records_by_run_partition (
  owner_run_id text,
  source_topic text,
  source_partition int,
  source_offset bigint,
  kafka_timestamp timestamp,
  kafka_key blob,
  raw_value blob,
  schema_id int,
  claimed_run_id text,
  claimed_dataset_id text,
  claimed_source_sequence bigint,
  claimed_payload_type text,
  claimed_produced_at_ms bigint,
  PRIMARY KEY ((owner_run_id, source_topic, source_partition), source_offset)
) WITH CLUSTERING ORDER BY (source_offset ASC);

CREATE TABLE IF NOT EXISTS market_data.silver_events_by_run (
  run_id text,
  event_id text,
  symbol text,
  event_time timestamp,
  event_date date,
  price double,
  volume bigint,
  conditions list<text>,
  source_sequence bigint,
  PRIMARY KEY ((run_id), event_id)
);

CREATE TABLE IF NOT EXISTS market_data.silver_events_by_symbol_day (
  run_id text,
  symbol text,
  event_date date,
  event_time timestamp,
  event_id text,
  price double,
  volume bigint,
  conditions list<text>,
  PRIMARY KEY ((run_id, symbol, event_date), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time ASC, event_id ASC);

CREATE TABLE IF NOT EXISTS market_data.dlq_records_by_run (
  owner_run_id text,
  source_topic text,
  source_partition int,
  source_offset bigint,
  claimed_run_id text,
  rejection_code text,
  rejection_detail text,
  raw_value blob,
  source_timestamp timestamp,
  PRIMARY KEY ((owner_run_id), source_topic, source_partition, source_offset)
);

CREATE TABLE IF NOT EXISTS market_data.stream_batches_by_query (
  run_id text,
  query_name text,
  batch_id bigint,
  status text,
  input_count bigint,
  valid_count bigint,
  invalid_count bigint,
  batch_start_offsets map<int, bigint>,
  batch_end_offsets_exclusive map<int, bigint>,
  cumulative_next_offsets map<int, bigint>,
  attempt_count int,
  processing_latency_p95_ms bigint,
  last_error text,
  started_at timestamp,
  completed_at timestamp,
  PRIMARY KEY ((run_id, query_name), batch_id)
) WITH CLUSTERING ORDER BY (batch_id ASC);

CREATE TABLE IF NOT EXISTS market_data.run_state_transitions_by_run (
  run_id text,
  transition_seq int,
  state text,
  actor text,
  occurred_at timestamp,
  reason text,
  PRIMARY KEY ((run_id), transition_seq)
) WITH CLUSTERING ORDER BY (transition_seq ASC);

CREATE TABLE IF NOT EXISTS market_data.run_checks_by_run (
  run_id text,
  check_name text,
  expected_value text,
  actual_value text,
  status text,
  detail text,
  checked_at timestamp,
  PRIMARY KEY ((run_id), check_name)
);

CREATE TABLE IF NOT EXISTS market_data.run_metrics_by_run (
  run_id text,
  sampled_at timestamp,
  query_name text,
  total_lag bigint,
  partition_lag map<int, bigint>,
  processed_count bigint,
  processing_latency_p95_ms bigint,
  PRIMARY KEY ((run_id), sampled_at, query_name)
) WITH CLUSTERING ORDER BY (sampled_at ASC, query_name ASC);

CREATE TABLE IF NOT EXISTS market_data.gold_ohlcv_5m_by_symbol_day (
  run_id text,
  symbol text,
  window_date date,
  window_start timestamp,
  window_end timestamp,
  open double,
  high double,
  low double,
  close double,
  volume bigint,
  trade_count bigint,
  vwap double,
  PRIMARY KEY ((run_id, symbol, window_date), window_start)
) WITH CLUSTERING ORDER BY (window_start ASC);

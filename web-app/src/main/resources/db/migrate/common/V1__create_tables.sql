-- HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
-- (c) 2016-2018 Hortonworks, Inc. All rights reserved.
-- This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
-- Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
-- to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
-- properly licensed third party, you do not have any rights to this code.
-- If this code is provided to you under the terms of the AGPLv3:
--   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
-- (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
-- LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
-- (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
-- FROM OR RELATED TO THE CODE; AND
-- (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
-- DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
-- DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
-- OR LOSS OR CORRUPTION OF DATA.

CREATE SCHEMA IF NOT EXISTS das;

/*
 * Table definition for the compose functionality
 */

CREATE TABLE das.ide_settings (
  id    SERIAL PRIMARY KEY,
  key   VARCHAR(100),
  value VARCHAR(200),
  owner VARCHAR(256)
);
CREATE UNIQUE INDEX idx_ide_settings_unique ON das.ide_settings (owner, key);

CREATE TABLE das.udf (
  id           SERIAL PRIMARY KEY,
  name         VARCHAR(100),
  classname    VARCHAR(1000),
  fileResource INTEGER,
  owner        VARCHAR(256)
);
CREATE UNIQUE INDEX idx_udf_unique ON das.udf (owner, name);

CREATE TABLE das.file_resource (
  id    SERIAL PRIMARY KEY,
  name  VARCHAR(100),
  path  VARCHAR(200),
  owner VARCHAR(256)
);
CREATE UNIQUE INDEX idx_file_resource_unique ON das.file_resource (owner, name);

CREATE TABLE das.saved_query (
  id       SERIAL PRIMARY KEY,
  title    VARCHAR(512),
  selected_database VARCHAR(256),
  query    TEXT,
  owner    VARCHAR(256)
);
CREATE INDEX idx_owner ON das.saved_query (owner);

CREATE TABLE das.jobs (
  id SERIAL PRIMARY KEY,
  owner VARCHAR (256),
  title VARCHAR (256),
  status_dir VARCHAR (2048),
  date_submitted BIGINT,
  duration BIGINT,
  query TEXT,
  selected_database VARCHAR (256),
  status VARCHAR(10),
  referrer VARCHAR(32),
  global_settings TEXT,
  log_file VARCHAR(2048),
  guid VARCHAR(100)
);


/*
 * Table definition for the query search functionality
 */

CREATE EXTENSION if not exists pg_trgm WITH SCHEMA das;

CREATE TABLE das.hive_query (
  id                    SERIAL PRIMARY KEY,
  query_id              VARCHAR(512) UNIQUE,
  query                 TEXT,
  query_fts             TSVECTOR,
  start_time            BIGINT,
  end_time              BIGINT,
  elapsed_time          BIGINT,
  status                VARCHAR(32),
  queue_name            VARCHAR(767),
  user_id               VARCHAR(256),
  request_user          VARCHAR(256),
  cpu_time              BIGINT,
  physical_memory       BIGINT,
  virtual_memory        BIGINT,
  data_read             BIGINT,
  data_written          BIGINT,
  operation_id          VARCHAR(512),
  client_ip_address     VARCHAR(64),
  hive_instance_address VARCHAR(512),
  hive_instance_type    VARCHAR(512),
  session_id            VARCHAR(512),
  log_id                VARCHAR(512),
  thread_id             VARCHAR(512),
  execution_mode        VARCHAR(16),
  tables_read           JSONB,
  tables_written        JSONB,
  domain_id             VARCHAR(512),
  llap_app_id           VARCHAR(512),
  used_cbo              VARCHAR(16),
  processed             BOOLEAN      DEFAULT FALSE,
  created_at            TIMESTAMP(0) DEFAULT NOW()
);

CREATE TRIGGER trg_query_tsvector_update
BEFORE INSERT OR UPDATE ON das.hive_query
FOR EACH ROW
EXECUTE PROCEDURE
  tsvector_update_trigger(query_fts, 'pg_catalog.english', query);

CREATE FUNCTION update_elapse_time()
  RETURNS TRIGGER AS $$
BEGIN
  NEW.elapsed_time = NEW.end_time - NEW.start_time;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_elapsed_time
BEFORE INSERT OR UPDATE ON das.hive_query
FOR EACH ROW
WHEN (NEW.end_time IS NOT NULL)
EXECUTE PROCEDURE update_elapse_time();

CREATE INDEX idx_hq_query_id
  ON das.hive_query (query_id);
CREATE INDEX idx_hq_llap_app_id
  ON das.hive_query (llap_app_id);
CREATE INDEX idx_hq_fts
  ON das.hive_query USING GIN (query_fts);
CREATE INDEX idx_hq_start_time
  ON das.hive_query (start_time);
CREATE INDEX idx_hq_end_time
  ON das.hive_query (end_time);
CREATE INDEX idx_hq_elapsed_time
  ON das.hive_query (elapsed_time);
CREATE INDEX idx_hq_status
  ON das.hive_query (status);
CREATE INDEX idx_hq_query_name
  ON das.hive_query (queue_name);
CREATE INDEX idx_hq_request_user
  ON das.hive_query (request_user);
CREATE INDEX idx_hq_client_ip
  ON das.hive_query (client_ip_address);
CREATE INDEX idx_hq_tables_read
  ON das.hive_query USING GIN (tables_read);
CREATE INDEX idx_hq_tables_written
  ON das.hive_query USING GIN (tables_written);
CREATE INDEX idx_hq_processed
  ON das.hive_query (processed);
CREATE INDEX idx_hq_created_at
  ON das.hive_query (created_at);
CREATE INDEX idx_hq_cpu_time
  ON das.hive_query (cpu_time);
CREATE INDEX idx_hq_physical_memory
  ON das.hive_query (physical_memory);
CREATE INDEX idx_hq_virtual_memory
  ON das.hive_query (virtual_memory);
CREATE INDEX idx_hq_data_read
  ON das.hive_query (data_read);
CREATE INDEX idx_hq_data_written
  ON das.hive_query (data_written);


CREATE TABLE das.dag_info (
  id                SERIAL PRIMARY KEY,
  dag_id            VARCHAR(512) UNIQUE,
  dag_name          VARCHAR(512),
  application_id    VARCHAR(512),
  init_time         BIGINT,
  start_time        BIGINT,
  end_time          BIGINT,
  time_taken        BIGINT,
  status            VARCHAR(64),
  am_webservice_ver VARCHAR(16),
  am_log_url        VARCHAR(512),
  queue_name        VARCHAR(64),
  caller_id         VARCHAR(512),
  caller_type       VARCHAR(128),
  hive_query_id     INTEGER,
  created_at        TIMESTAMP(0) DEFAULT NOW(),
  source_file       TEXT,
  FOREIGN KEY (hive_query_id) REFERENCES das.hive_query (id) ON DELETE CASCADE
);

CREATE INDEX idx_di_dag_id
  ON das.dag_info (dag_id);
CREATE INDEX idx_di_dag_name
  ON das.dag_info (dag_name);
CREATE INDEX idx_di_init_time
  ON das.dag_info (init_time);
CREATE INDEX idx_di_start_time
  ON das.dag_info (start_time);
CREATE INDEX idx_di_end_time
  ON das.dag_info (end_time);
CREATE INDEX idx_di_time_taken
  ON das.dag_info (time_taken);
CREATE INDEX idx_di_status
  ON das.dag_info (status);
CREATE INDEX idx_di_queue_name
  ON das.dag_info (queue_name);
CREATE INDEX idx_di_application_id
  ON das.dag_info (application_id);
CREATE INDEX idx_di_hive_query_id
  ON das.dag_info (hive_query_id);
CREATE INDEX idx_di_created_at
  ON das.dag_info (created_at);

CREATE TABLE das.query_details (
  id                     SERIAL PRIMARY KEY,
  hive_query_id          INTEGER UNIQUE,
  explain_plan           JSONB,
  configuration          JSONB,
  perf                   JSONB,
  FOREIGN KEY (hive_query_id) REFERENCES das.hive_query (id) ON DELETE CASCADE
);

CREATE TABLE das.dag_details (
  id                     SERIAL PRIMARY KEY,
  dag_info_id            INTEGER UNIQUE,
  hive_query_id          INTEGER,
  dag_plan               JSONB,
  vertex_name_id_mapping JSONB,
  diagnostics            TEXT,
  counters               JSONB,
  FOREIGN KEY (dag_info_id) REFERENCES das.dag_info (id) ON DELETE CASCADE,
  FOREIGN KEY (hive_query_id) REFERENCES das.hive_query (id) ON DELETE CASCADE
);

CREATE INDEX idx_dd_hive_query_id
  ON das.dag_details (hive_query_id);

CREATE TABLE das.vertex_info (
  id                        SERIAL PRIMARY KEY,
  vertex_id                 VARCHAR(512) UNIQUE,
  name                      VARCHAR(256),
  domain_id                 VARCHAR(512),
  task_count                INT,
  completed_task_count      INT,
  succeeded_task_count      INT,
  failed_task_count         INT,
  killed_task_count         INT,
  failed_task_attempt_count INT,
  killed_task_attempt_count INT,
  class_name                VARCHAR(512),
  start_time                BIGINT,
  end_time                  BIGINT,
  init_requested_time       BIGINT,
  start_requested_time      BIGINT,
  status                    VARCHAR(64),
  counters                  JSONB,
  stats                     JSONB,
  events                    JSONB,
  dag_id                    INTEGER,
  FOREIGN KEY (dag_id) REFERENCES das.dag_info (id) ON DELETE CASCADE
);

CREATE INDEX idx_vi_vertex_id
  ON das.vertex_info (vertex_id);
CREATE INDEX idx_vi_dag_id
  ON das.vertex_info (dag_id);

/*
 * Table definition for the reporting functionality
 */

-- Dimension tables

CREATE TABLE das.databases (
  id              SERIAL PRIMARY KEY,
  name            VARCHAR(256) NOT NULL,
  create_time      TIMESTAMP,
  dropped         BOOLEAN DEFAULT FALSE NOT NULL,
  dropped_at      TIMESTAMP,
  last_updated_at TIMESTAMP,
  creation_source VARCHAR(25) NOT NULL
);

-- TODO : we should create index on database Name as well as that is also frequently searched.

-- this index is for making sure that there is exactly one database of a given name which is not dropped
CREATE UNIQUE INDEX idx_databases_unique_partial_index ON das.databases (name,dropped)
WHERE not dropped;

CREATE TABLE das.tables (
    id                 SERIAL PRIMARY KEY,
    db_id              INTEGER      NOT NULL,
    name               VARCHAR(767) NOT NULL,
    owner              VARCHAR(256),
    create_time        TIMESTAMP,
    last_access_time   TIMESTAMP,
    parsed_table_type  VARCHAR(64),
    table_type         VARCHAR(64),
    location           VARCHAR(1024),
    storage            VARCHAR(32),
    serde              VARCHAR(512),
    input_format       VARCHAR(512),
    output_format      VARCHAR(512),
    compressed         BOOLEAN,
    num_buckets        SMALLINT,
    comment            TEXT,
    dropped            BOOLEAN DEFAULT FALSE NOT NULL,
    dropped_at         TIMESTAMP,
    last_updated_at    TIMESTAMP,
    properties         JSONB,          -- table properties
    creation_source    VARCHAR(25) NOT NULL,
    retention          INT,
    storage_parameters JSONB,

  FOREIGN KEY (db_id) REFERENCES das.databases (id) ON DELETE CASCADE
);
-- TODO : we should create index on tableName as well as that is also frequently searched.
CREATE INDEX idx_tables_db_id
  ON das.tables (db_id);

-- this index is for making sure that there is exactly one table of a given name for each db_id which is not dropped
CREATE UNIQUE INDEX idx_tables_unique_partial_index ON das.tables (name,db_id,dropped)
WHERE not dropped;

CREATE TABLE das.columns (
  id              SERIAL PRIMARY KEY,
  table_id        INTEGER      NOT NULL,
  name            VARCHAR(767) NOT NULL,
  datatype        TEXT,
  column_type     VARCHAR(767),
  precision       SMALLINT,
  scale           SMALLINT,
  comment         TEXT,
  create_time     TIMESTAMP,
  is_primary      BOOLEAN DEFAULT FALSE,
  is_partitioned  BOOLEAN DEFAULT FALSE,
  is_clustered    BOOLEAN DEFAULT FALSE,
  sort_order      SMALLINT DEFAULT 2,
  is_sort_key     BOOLEAN DEFAULT FALSE,
  column_position SMALLINT DEFAULT -1,
  dropped         BOOLEAN DEFAULT FALSE NOT NULL,
  dropped_at      TIMESTAMP,
  last_updated_at TIMESTAMP,
  creation_source       VARCHAR(25) NOT NULL,   -- 0 = event-processor, 1 = replication

  FOREIGN KEY (table_id) REFERENCES das.tables (id) ON DELETE CASCADE
);

CREATE INDEX idX_columns_table_id
  ON das.columns (table_id);

-- this index is for making sure that there is exactly one column of a given name for each table_id which is not dropped
CREATE UNIQUE INDEX idx_columns_unique_partial_index ON das.columns (name,table_id,dropped)
WHERE not dropped;

-- partition info table
CREATE TABLE das.table_partition_info (
  id              SERIAL PRIMARY KEY,
  table_id        INTEGER NOT NULL,
  partition_name  VARCHAR(1024) NOT NULL,
  details         JSONB,
  raw_data_size   BIGINT NOT NULL,
  num_rows        INTEGER NOT NULL,
  num_files       INTEGER NOT NULL,

  FOREIGN KEY (table_id) REFERENCES das.tables (id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_table_partition_info_table_id_partition_name
  ON das.table_partition_info (table_id,partition_name);

-- Facts tables

-- daily tables
CREATE TABLE das.join_column_stats_daily (
  id                     SERIAL PRIMARY KEY,
  left_column            INTEGER,
  right_column           INTEGER,
  algorithm              VARCHAR(128),
  inner_join_count       INTEGER DEFAULT 0,
  left_outer_join_count  INTEGER DEFAULT 0,
  right_outer_join_count INTEGER DEFAULT 0,
  full_outer_join_count  INTEGER DEFAULT 0,
  left_semi_join_count   INTEGER DEFAULT 0,
  unique_join_count      INTEGER DEFAULT 0,
  unknown_join_count     INTEGER DEFAULT 0,
  total_join_count       INTEGER DEFAULT 0,
  date                   DATE,
  FOREIGN KEY (left_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  FOREIGN KEY (right_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (left_column, right_column, algorithm, date)
);
CREATE INDEX idx_daily_stats_left_column
  ON das.join_column_stats_daily (left_column);
CREATE INDEX idx_daily_stats_right_column
  ON das.join_column_stats_daily (right_column);
CREATE INDEX idx_daily_stats_algorithm
  ON das.join_column_stats_daily (algorithm);
CREATE INDEX idx_daily_stats_date
  ON das.join_column_stats_daily (date);

CREATE TABLE das.table_stats_daily (
  id              SERIAL PRIMARY KEY,
  table_id        INTEGER,
  read_count      INTEGER DEFAULT 0,
  write_count     INTEGER DEFAULT 0,
  bytes_read      BIGINT DEFAULT 0,
  records_read    BIGINT DEFAULT 0,
  bytes_written   BIGINT DEFAULT 0,
  records_written BIGINT DEFAULT 0,
  date            DATE,
  FOREIGN KEY (table_id) REFERENCES das.tables (id) ON DELETE CASCADE,
  UNIQUE (table_id, date)
);
CREATE INDEX idx_table_stats_daily_table_id
  ON das.table_stats_daily (table_id);
CREATE INDEX idx_table_stats_daily_date
  ON das.table_stats_daily (date);

CREATE TABLE das.column_stats_daily (
  id                SERIAL PRIMARY KEY,
  column_id         INTEGER,
  join_count        INTEGER DEFAULT 0,
  filter_count      INTEGER DEFAULT 0,
  aggregation_count INTEGER DEFAULT 0,
  projection_count  INTEGER DEFAULT 0,
  date              DATE,
  FOREIGN KEY (column_id) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (column_id, date)
);
CREATE INDEX idx_column_stats_daily_column_id
  ON das.column_stats_daily (column_id);

CREATE INDEX idx_column_stats_daily_date
  ON das.column_stats_daily (date);

--weekly tables
CREATE TABLE das.join_column_stats_weekly (
  id                     SERIAL PRIMARY KEY,
  left_column            INTEGER,
  right_column           INTEGER,
  algorithm              VARCHAR(128),
  inner_join_count       INTEGER DEFAULT 0,
  left_outer_join_count  INTEGER DEFAULT 0,
  right_outer_join_count INTEGER DEFAULT 0,
  full_outer_join_count  INTEGER DEFAULT 0,
  left_semi_join_count   INTEGER DEFAULT 0,
  unique_join_count      INTEGER DEFAULT 0,
  unknown_join_count     INTEGER DEFAULT 0,
  total_join_count       INTEGER DEFAULT 0,
  date                   DATE,
  FOREIGN KEY (left_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  FOREIGN KEY (right_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (left_column, right_column, algorithm, date)
);
CREATE INDEX idx_weekly_stats_left_column
  ON das.join_column_stats_weekly (left_column);
CREATE INDEX idx_weekly_stats_right_column
  ON das.join_column_stats_weekly (right_column);
CREATE INDEX idx_weekly_stats_algorithm
  ON das.join_column_stats_weekly (algorithm);
CREATE INDEX idx_weekly_stats_week
  ON das.join_column_stats_weekly (date);

CREATE TABLE das.table_stats_weekly (
  id              SERIAL PRIMARY KEY,
  table_id        INTEGER,
  read_count      INTEGER DEFAULT 0,
  write_count     INTEGER DEFAULT 0,
  bytes_read      BIGINT DEFAULT 0,
  records_read    BIGINT DEFAULT 0,
  bytes_written   BIGINT DEFAULT 0,
  records_written BIGINT DEFAULT 0,
  date            DATE,
  FOREIGN KEY (table_id) REFERENCES das.tables (id) ON DELETE CASCADE,
  UNIQUE (table_id, date)
);
CREATE INDEX idx_table_stats_weekly_table_id
  ON das.table_stats_weekly (table_id);
CREATE INDEX idx_table_stats_weekly_week
  ON das.table_stats_weekly (date);

CREATE TABLE das.column_stats_weekly (
  id                SERIAL PRIMARY KEY,
  column_id         INTEGER,
  join_count        INTEGER DEFAULT 0,
  filter_count      INTEGER DEFAULT 0,
  aggregation_count INTEGER DEFAULT 0,
  projection_count  INTEGER DEFAULT 0,
  date              DATE,
  FOREIGN KEY (column_id) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (column_id, date)
);
CREATE INDEX idx_column_stats_weekly_column_id
  ON das.column_stats_weekly (column_id);
CREATE INDEX idx_column_stats_weekly_week
  ON das.column_stats_weekly (date);

--monthly tables
CREATE TABLE das.join_column_stats_monthly (
  id                     SERIAL PRIMARY KEY,
  left_column            INTEGER,
  right_column           INTEGER,
  algorithm              VARCHAR(128),
  inner_join_count       INTEGER DEFAULT 0,
  left_outer_join_count  INTEGER DEFAULT 0,
  right_outer_join_count INTEGER DEFAULT 0,
  full_outer_join_count  INTEGER DEFAULT 0,
  left_semi_join_count   INTEGER DEFAULT 0,
  unique_join_count      INTEGER DEFAULT 0,
  unknown_join_count     INTEGER DEFAULT 0,
  total_join_count       INTEGER DEFAULT 0,
  date                   DATE,
  FOREIGN KEY (left_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  FOREIGN KEY (right_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (left_column, right_column, algorithm, date)
);
CREATE INDEX idx_monthly_stats_left_column
  ON das.join_column_stats_monthly (left_column);
CREATE INDEX idx_monthly_stats_right_column
  ON das.join_column_stats_monthly (right_column);
CREATE INDEX idx_monthly_stats_algorithm
  ON das.join_column_stats_monthly (algorithm);
CREATE INDEX idx_monthly_stats_month
  ON das.join_column_stats_monthly (date);

CREATE TABLE das.table_stats_monthly (
  id               SERIAL PRIMARY KEY,
  table_id         INTEGER,
  read_count       INTEGER DEFAULT 0,
  write_count      INTEGER DEFAULT 0,
  bytes_read       BIGINT DEFAULT 0,
  records_read     BIGINT DEFAULT 0,
  bytes_written    BIGINT DEFAULT 0,
  records_written  BIGINT DEFAULT 0,
  date             DATE,
  FOREIGN KEY (table_id) REFERENCES das.tables (id) ON DELETE CASCADE,
  UNIQUE (table_id, date)
);
CREATE INDEX idx_table_stats_monthly_table_id
  ON das.table_stats_monthly (table_id);
CREATE INDEX idx_table_stats_monthly_month
  ON das.table_stats_monthly (date);

CREATE TABLE das.column_stats_monthly (
  id                SERIAL PRIMARY KEY,
  column_id         INTEGER,
  join_count        INTEGER DEFAULT 0,
  filter_count      INTEGER DEFAULT 0,
  aggregation_count INTEGER DEFAULT 0,
  projection_count  INTEGER DEFAULT 0,
  date              DATE,
  FOREIGN KEY (column_id) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (column_id, date)
);
CREATE INDEX idx_column_stats_monthly_column_id
  ON das.column_stats_monthly (column_id);
CREATE INDEX idx_column_stats_monthly_month
  ON das.column_stats_monthly (date);

--quarterly tables
CREATE TABLE das.join_column_stats_quarterly (
  id                     SERIAL PRIMARY KEY,
  left_column            INTEGER,
  right_column           INTEGER,
  algorithm              VARCHAR(128),
  inner_join_count       INTEGER DEFAULT 0,
  left_outer_join_count  INTEGER DEFAULT 0,
  right_outer_join_count INTEGER DEFAULT 0,
  full_outer_join_count  INTEGER DEFAULT 0,
  left_semi_join_count   INTEGER DEFAULT 0,
  unique_join_count      INTEGER DEFAULT 0,
  unknown_join_count     INTEGER DEFAULT 0,
  total_join_count       INTEGER DEFAULT 0,
  date                   DATE,
  FOREIGN KEY (left_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  FOREIGN KEY (right_column) REFERENCES das.columns (id) ON DELETE CASCADE,
  UNIQUE (left_column, right_column, algorithm, date)
);
CREATE INDEX idx_quarterly_stats_left_column
  ON das.join_column_stats_quarterly (left_column);
CREATE INDEX idx_quarterly_stats_right_column
  ON das.join_column_stats_quarterly (right_column);
CREATE INDEX idx_quarterly_stats_algorithm
  ON das.join_column_stats_quarterly (algorithm);
CREATE INDEX idx_quarterly_stats_quarter
  ON das.join_column_stats_quarterly (date);

CREATE TABLE das.table_stats_quarterly (
  id                 SERIAL PRIMARY KEY,
  table_id           INTEGER,
  read_count         INTEGER DEFAULT 0,
  write_count        INTEGER DEFAULT 0,
  bytes_read         BIGINT DEFAULT 0,
  records_read       BIGINT DEFAULT 0,
  bytes_written      BIGINT DEFAULT 0,
  records_written    BIGINT DEFAULT 0,
  date               DATE,
  FOREIGN KEY (table_id) REFERENCES das.tables (id) ON DELETE CASCADE,
  UNIQUE (table_id, date)
);
CREATE INDEX idx_table_stats_quarterly_table_id
  ON das.table_stats_quarterly (table_id);
CREATE INDEX idx_table_stats_quarterly_quarter
  ON das.table_stats_quarterly (date);

CREATE TABLE das.column_stats_quarterly (
  id                 SERIAL PRIMARY KEY,
  column_id          INTEGER,
  join_count         INTEGER DEFAULT 0,
  filter_count       INTEGER DEFAULT 0,
  aggregation_count  INTEGER DEFAULT 0,
  projection_count   INTEGER DEFAULT 0,
  date               DATE,
  FOREIGN KEY (column_id) REFERENCES columns (id) ON DELETE CASCADE,
  UNIQUE (column_id, date)
);
CREATE INDEX idx_column_stats_quarterly_column_id
  ON das.column_stats_quarterly (column_id);
CREATE INDEX idx_column_stats_quarterly_quarter
  ON das.column_stats_quarterly (date);



/*
 * Table definition for the scheduler audit functionality
 */

CREATE TABLE das.report_scheduler_run_audit (
  id                SERIAL PRIMARY KEY,
  type              VARCHAR(100) NOT NULL,
  read_start_time   TIMESTAMP(0),
  read_end_time     TIMESTAMP(0),
  queries_processed TEXT,
  status            VARCHAR(50),
  failure_reason    TEXT,
  last_try_id       INTEGER,
  retry_count       SMALLINT,
  FOREIGN KEY (last_try_id) REFERENCES das.report_scheduler_run_audit (id) ON DELETE CASCADE
);
CREATE INDEX idx_rsra_status
  ON das.report_scheduler_run_audit (status);
CREATE INDEX idx_rsra_type
  ON das.report_scheduler_run_audit (type);


/*
 * Table definition for the event file read tracker functionality
 */
CREATE TABLE das.file_status (
  id                SERIAL PRIMARY KEY,
  file_type         VARCHAR(10),
  date              DATE,
  file_name         VARCHAR(1024),
  position          BIGINT,
  last_event_time   BIGINT,
  finished          BOOLEAN,
  UNIQUE (file_type, date, file_name)
);

/*
 * Table definition for suggested searches functionality
 */

CREATE TABLE das.searches (
  id        SERIAL PRIMARY KEY,

  name      VARCHAR(512),
  category  VARCHAR(64),    -- Possible values are SUGGEST & SAVED
  type      VARCHAR(64),    -- Possible values are BASIC & ADVANCED
  entity    VARCHAR(64),    -- Entity type of item being searched
  owner     VARCHAR(256),    -- For SUGGEST (pre-populated) queries this would be DEFAULT

  clause    TEXT,           -- Search clause
  facet     JSONB,          -- Facet clause
  columns   JSONB,          -- Visible columns in the displayed order. Marking it a JSON field considering future improvements.
  range     JSONB,          -- Time range to be searched
  sort      JSONB           -- Sort order definition
);

CREATE INDEX idx_up_name ON das.searches (name);
CREATE INDEX idx_up_type ON das.searches (type);
CREATE INDEX idx_up_owner ON das.searches (owner);

INSERT INTO das.searches VALUES (DEFAULT, 'Longest running queries', 'SUGGEST', 'BASIC', 'query', 'DEFAULT', '', null, null, null, '{"sortOrder": "desc", "sortColumnId": "duration"}');
INSERT INTO das.searches VALUES (DEFAULT, 'Most expensive queries', 'SUGGEST', 'BASIC', 'query', 'DEFAULT', '', null, null, null, '{"sortOrder": "desc", "sortColumnId": "cpuTime"}');
INSERT INTO das.searches VALUES (DEFAULT, 'Queries that use LEFT JOIN', 'SUGGEST', 'BASIC', 'query', 'DEFAULT', 'LEFT JOIN', null, null, null, null);
INSERT INTO das.searches VALUES (DEFAULT, 'Failed or cancelled queries', 'SUGGEST', 'BASIC', 'query', 'DEFAULT', '', '{"status": {"in": ["FAILED", "CANCELED"]}}', null, null, null);
INSERT INTO das.searches VALUES (DEFAULT, 'Queries didn''t use a CBO', 'SUGGEST', 'BASIC', 'query', 'DEFAULT', '', '{"usedCBO": {"in": ["No"]}}', null, null, null);

CREATE TABLE db_replication_info (
  id                          SERIAL PRIMARY KEY,
  database_name               VARCHAR(512),
  last_replication_id         VARCHAR(512),
  last_replication_start_time TIMESTAMP,
  last_replication_end_time   TIMESTAMP,
  next_replication_start_time TIMESTAMP
);

CREATE TABLE das.app_properties (
  id                SERIAL PRIMARY KEY,
  property_name     VARCHAR(100),
  property_value    VARCHAR(1000),
  UNIQUE (property_name)
);

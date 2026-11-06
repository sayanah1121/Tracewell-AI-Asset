-- Phase 0 smoke test: Flink side of the shared Iceberg table.
-- Run with: sql-client.sh -f /opt/tracewell/flink-jobs/smoke/smoke_flink.sql
-- S3 credentials come from the AWS_* environment variables of the Flink containers.

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG tracewell WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true'
);

INSERT INTO tracewell.smoke.engine_writes VALUES
  (50, 'flink', 'written by flink', CURRENT_TIMESTAMP),
  (51, 'flink', 'written by flink', CURRENT_TIMESTAMP);

-- Flink reads back everything, including the rows Spark wrote.
SELECT engine, COUNT(*) AS n FROM tracewell.smoke.engine_writes GROUP BY engine;

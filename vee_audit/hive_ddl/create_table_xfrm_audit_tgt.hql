CREATE EXTERNAL TABLE IF NOT EXISTS stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO}(
 aep_usage_dt string, 
 aep_usage_type string, 
 name_register string, 
 aep_data_quality_cd string, 
 aep_no_of_intvl string, 
 intvl_cnt bigint, 
 intvl_usg double, 
 unq_meter_count bigint, 
 hdp_insert_dttm string)
PARTITIONED BY ( 
  run_dt string, 
  aep_opco string, 
  aggr_type string)
STORED AS PARQUET
LOCATION
  's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO}'
TBLPROPERTIES (
  'bucketing_version'='2',
  'parquet.bloom.filter.columns'='aggr_type',
  'parquet.bloom.filter.fpp'='0.05',
  'parquet.compression'='SNAPPY',
  'parquet.create.index'='true' )

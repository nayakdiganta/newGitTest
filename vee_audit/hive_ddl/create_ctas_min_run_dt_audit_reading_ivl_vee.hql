CREATE TABLE stg_vee.ctas_min_run_dt_audit_reading_ivl_vee
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/ctas_min_run_dt_audit_reading_ivl_vee/',
      format = 'TEXTFILE',
      field_delimiter = ',',
	  write_compression = 'NONE'
      )
AS
select min(run_dt) min_run_dt from usage_vee.audit_reading_ivl_vee_parquet ;
CREATE TABLE stg_vee.xfrm_ctas_audit_dates
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_ctas_audit_dates/',
      format = 'TEXTFILE',
      field_delimiter = ',',
	  write_compression = 'NONE'
      )
AS
select array_join(array_agg( concat('''', usage_date ,'''')),',') AEP_USAGE_DT 
from
(
	select distinct usage_date 
	from stg_vee.stg_measurement_audit 
	where aep_usage_type = 'interval' 
	--	and usage_date > '${START_DATE}' 
	and usage_date < '${AUDIT_RUN_DT}'
) A ;
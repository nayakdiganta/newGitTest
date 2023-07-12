--INTERVAL_COUNT aggregation
INSERT INTO stg_vee.xfrm_audit_reading_ivl_vee_src
(
  aep_usage_dt, 
  aep_usage_type, 
  name_register, 
  aep_data_quality_cd , 
  aep_no_of_intvl , 
  intvl_cnt , 
  intvl_usg , 
  unq_meter_count , 
  hdp_insert_dttm ,
  run_dt , 
  aep_opco , 
  aggr_type 
)
select 
  aep_usage_dt , 
  aep_usage_type , 
  name_register ,   
  '' aep_data_quality_cd ,
  aep_no_of_intvl,
  cast( cnt_msrmt_val as bigint) cnt_msrmt_val , 
  cast( tot_msrmt_val as double) tot_msrmt_val,
  unq_srl_nb , 
  cast( current_timestamp as varchar) hdp_insert_dttm , 
  '${AUDIT_RUN_DT}' run_dt , 
  aep_opco ,
  'INTERVAL_COUNT' aggr_type
from 
	stg_vee.xfrm_ctas_audit_intvl_aggr
--order by
--Run_dt,
--aep_opco,
--aep_usage_dt,
--aep_usage_type,
--name_register
;



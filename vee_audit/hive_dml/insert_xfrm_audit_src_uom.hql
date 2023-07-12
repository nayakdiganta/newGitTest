--UOM aggregation
insert INTO stg_vee.xfrm_audit_reading_ivl_vee_src
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
  '' aep_no_of_intvl ,
  cast( src_intvl_cnt as bigint) src_intvl_cnt , 
  cast( src_intvl_usg as double) src_intvl_usg,
  src_unq_meter_count , 
  cast( current_timestamp as varchar) hdp_insert_dttm , 
  run_dt , 
  aep_opco ,
  aggr_type
from 
(
	select 
		usage_date aep_usage_dt,
		aep_usage_type,
		measr_comp_type_cd name_register,
		'' aep_data_quality_cd ,
		'' aep_no_of_intvl ,
		sum(cnt_msrmt_val) src_intvl_cnt,
		sum(tot_msrmt_val) src_intvl_usg,
		COUNT(distinct adhoc_char_val) src_unq_meter_count,
		current_timestamp hdp_insert_dttm , 	
		'${AUDIT_RUN_DT}' Run_dt,
		aep_opco,	
		'UOM' aggr_type
	from 
		stg_vee.stg_measurement_audit
	where 	
		aep_usage_type = 'interval' 
		and usage_date < '${AUDIT_RUN_DT}'
	group by 
		'${AUDIT_RUN_DT}',
		aep_opco,
		usage_date,
		aep_usage_type,
		measr_comp_type_cd 
)aggr_tbl;
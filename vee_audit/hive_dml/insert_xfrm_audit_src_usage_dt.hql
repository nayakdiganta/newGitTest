-- USAGE_DATE
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
	'' name_register ,   
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
		'${AUDIT_RUN_DT}' Run_dt,
		'USAGE_DATE' aggr_type,
		aep_opco,
		usage_date aep_usage_dt,
		aep_usage_type,
		sum(cnt_msrmt_val) src_intvl_cnt,
		sum(tot_msrmt_val) src_intvl_usg,	
		COUNT(distinct adhoc_char_val) src_unq_meter_count
	from 
		stg_vee.stg_measurement_audit
	where 
		aep_usage_type = 'interval' 
		and measr_comp_type_cd in('E-KWH-15-DEL','E-KWH-15-REC')
	--	and usage_date > '${START_DATE}'
		and usage_date < '${AUDIT_RUN_DT}'
	group by 
		'${AUDIT_RUN_DT}',
		aep_opco,
		usage_date,
		aep_usage_type

) aggr_tbl;
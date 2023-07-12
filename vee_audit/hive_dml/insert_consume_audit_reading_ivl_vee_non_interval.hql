--non_interval data
insert into usage_vee.audit_reading_ivl_vee_parquet
(
	aep_usage_dt ,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	src_intvl_cnt,
	tgt_intvl_cnt,
	diff_intvl_cnt,
	per_diff_intvl_cnt,
	src_intvl_usg,
	tgt_intvl_usg,
	diff_intvl_usg,
	per_diff_intvl_usg,
	src_unq_meter_count,
	tgt_unq_meter_count,
	diff_unq_meter_count,
	per_diff_unq_mtr,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
)
SELECT  
	coalesce(src.aep_usage_dt,tgt.aep_usage_dt) aep_usage_dt,  
	coalesce(src.aep_usage_type,tgt.aep_usage_type) aep_usage_type, 
	coalesce(src.name_register,tgt.name_register)  name_register, 
	coalesce(src.aep_data_quality_cd,tgt.aep_data_quality_cd) aep_data_quality_cd, 
	coalesce(src.aep_no_of_intvl,tgt.aep_no_of_intvl) aep_no_of_intvl, 

	coalesce(src.intvl_cnt,0) src_intvl_cnt, 
	coalesce(tgt.intvl_cnt,0) tgt_intvl_cnt,
	(coalesce(src.intvl_cnt,0) - coalesce(tgt.intvl_cnt,0)) diff_intvl_cnt,
	cast(round( (((src.intvl_cnt - tgt.intvl_cnt)/cast(src.intvl_cnt as double) )*100) ,2) as decimal(7,4)) per_diff_intvl_cnt,
	coalesce(src.intvl_usg,0.0) src_intvl_usg, 
	coalesce(tgt.intvl_usg,0.0) tgt_intvl_usg,
	cast(  case when is_nan((src.intvl_usg - coalesce(tgt.intvl_usg,0.0))) then 0.0 else (src.intvl_usg - tgt.intvl_usg) end as decimal(9,2) ) diff_intvl_usg,
	
	cast( case when is_nan(((src.intvl_usg - coalesce(tgt.intvl_usg,0.0) )/src.intvl_usg)*100.0) then 0.0 
	else (( (src.intvl_usg - coalesce(tgt.intvl_usg,0.0) )/src.intvl_usg)*100.0) end as decimal(7,4) ) per_diff_intvl_usg,

	coalesce(src.unq_meter_count,0) src_unq_meter_count, 
	coalesce(tgt.unq_meter_count,0) tgt_unq_meter_count, 
	(coalesce(src.unq_meter_count,0) - coalesce(tgt.unq_meter_count,0)) diff_unq_meter_count,
	cast(round( (((src.unq_meter_count - tgt.unq_meter_count)/cast(src.unq_meter_count as double) )*100) ,2) as decimal(7,4)) per_diff_unq_mtr,
	
	cast( current_timestamp as varchar) hdp_insert_dttm,
	
	coalesce(src.run_dt, tgt.run_dt) run_dt, 
	coalesce(src.aep_opco, tgt.aep_opco) aep_opco,
	coalesce(src.aggr_type, tgt.aggr_type) aggr_type
FROM     
stg_vee.xfrm_audit_reading_ivl_vee_src src
FULL OUTER JOIN stg_vee.xfrm_audit_reading_ivl_vee_tgt tgt 
ON 
src.run_dt=tgt.run_dt
AND src.aep_opco=tgt.aep_opco 
AND src.aggr_type=tgt.aggr_type 
AND src.aep_usage_dt=tgt.aep_usage_dt 
AND src.aep_usage_type=tgt.aep_usage_type 
AND src.name_register=tgt.name_register 
AND src.aep_data_quality_cd=tgt.aep_data_quality_cd 
AND src.aep_no_of_intvl=tgt.aep_no_of_intvl
AND tgt.aggr_type <> 'INTERVAL_COUNT'
AND tgt.run_dt = '${AUDIT_RUN_DT}'
and tgt.aep_usage_dt not in ('${START_DATE}')

where 
src.aggr_type <> 'INTERVAL_COUNT'
and src.run_dt = '${AUDIT_RUN_DT}'
and src.aep_usage_dt not in ('${START_DATE}')

;


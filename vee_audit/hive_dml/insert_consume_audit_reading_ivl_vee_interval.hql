-- interval data
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
select 

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
	cast( (src.intvl_usg - tgt.intvl_usg) as decimal(9,2) ) diff_intvl_usg,
	cast(round( (((src.intvl_usg - tgt.intvl_usg)/cast(src.intvl_usg as double) )*100) ,2) as decimal(7,4)) per_diff_intvl_usg,
	
	src.unq_meter_count src_unq_meter_count,
	tgt.unq_meter_count tgt_unq_meter_count,
	src.unq_meter_count -tgt.unq_meter_count diff_unq_meter_count,
	cast(round( (((src.unq_meter_count - coalesce(tgt.unq_meter_count,0) )/cast(src.unq_meter_count as double) )*100) ,2) as decimal(7,4)) per_diff_unq_mtr,
	
	cast( current_timestamp as varchar) hdp_insert_dttm,

	coalesce(src.run_dt, tgt.run_dt) run_dt, 
	coalesce(src.aep_opco, tgt.aep_opco) aep_opco,
	coalesce(src.aggr_type, tgt.aggr_type) aggr_type
	
from 
(
	Select 
	run_dt,
	aep_opco,
	aggr_type,
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	case when cast(aep_no_of_intvl as int) > 96 then '>96' when cast(aep_no_of_intvl as int) < 96 then '<96' when cast(aep_no_of_intvl as int) = 96 then '96' else null end aep_no_of_intvl,

	sum(intvl_cnt) intvl_cnt,
	sum(intvl_usg) intvl_usg,
	sum(unq_meter_count) unq_meter_count
	from 
	stg_vee.xfrm_audit_reading_ivl_vee_src
	where 
	aggr_type = 'INTERVAL_COUNT'
	AND run_dt = '${AUDIT_RUN_DT}'
	and aep_usage_dt not in ('${START_DATE}')

	Group by
	run_dt,
	aep_opco,
	aggr_type,
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	case when cast(aep_no_of_intvl as int) > 96 then '>96' when cast(aep_no_of_intvl as int) < 96 then '<96' when cast(aep_no_of_intvl as int) = 96 then '96' else null end
	

) src
FULL OUTER JOIN 
(
	Select 
	run_dt,
	aep_opco,
	aggr_type,
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	case when cast(aep_no_of_intvl as int) > 96 then '>96' when cast(aep_no_of_intvl as int) < 96 then '<96' when cast(aep_no_of_intvl as int) = 96 then '96' else null end aep_no_of_intvl,

	sum(intvl_cnt) intvl_cnt,
	sum(intvl_usg) intvl_usg,
	sum(unq_meter_count) unq_meter_count
	from 
	stg_vee.xfrm_audit_reading_ivl_vee_tgt
	where 
	aggr_type = 'INTERVAL_COUNT'
	AND run_dt = '${AUDIT_RUN_DT}'
	and aep_usage_dt not in ('${START_DATE}')


	Group by
	run_dt,
	aep_opco,
	aggr_type,
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	case when cast(aep_no_of_intvl as int) > 96 then '>96' when cast(aep_no_of_intvl as int) < 96 then '<96' when cast(aep_no_of_intvl as int) = 96 then '96' else null end

) tgt on 
src.run_dt = tgt.run_dt 
and src.aep_opco = tgt.aep_opco 
and src.aggr_type = tgt.aggr_type 
and src.aep_usage_dt = tgt.aep_usage_dt 
and src.aep_usage_type = tgt.aep_usage_type 
and src.name_register = tgt.name_register 
and src.aep_data_quality_cd = tgt.aep_data_quality_cd 
and src.aep_no_of_intvl = tgt.aep_no_of_intvl

;

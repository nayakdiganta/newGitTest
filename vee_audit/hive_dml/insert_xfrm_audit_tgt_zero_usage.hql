-- ZERO_USAGE
insert INTO stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO} 
(
	aep_usage_dt , 
	aep_usage_type , 
	name_register , 
	aep_data_quality_cd , 
	aep_no_of_intvl , 
	intvl_cnt , 
	intvl_usg , 
	unq_meter_count, 
	hdp_insert_dttm, 
	run_dt , 
	aep_opco ,
	aggr_type
)
select 
	aep_usage_dt , 
	aep_usage_type , 
	name_register ,   
	aep_data_quality_cd aep_data_quality_cd ,
	'' aep_no_of_intvl ,
	cast( sum(tgt_intvl_cnt) as bigint) tgt_intvl_cnt , 
	cast( sum(tgt_intvl_usg) as double) tgt_intvl_usg , 
	sum(tgt_unq_meter_count) tgt_unq_meter_count , 
	cast( current_timestamp as varchar) hdp_insert_dttm , 	
	run_dt , 
	aep_opco ,
	'ZERO_USAGE' aggr_type
from 
(
	select 
		aep_usage_dt , 
		aep_usage_type , 
		name_register ,   
		aep_data_quality_cd,
		SUM(tgt_intvl_cnt) tgt_intvl_cnt , 
		SUM(tgt_intvl_usg) tgt_intvl_usg, 
		COUNT(serialnumber) tgt_unq_meter_count , 
		run_dt , 
		aep_opco ,
		'ZERO_USAGE' aggr_type
	from 
	(
		Select  
			'${AUDIT_RUN_DT}' Run_dt,
			aep_usage_dt,
			aep_usage_type,
			serialnumber serialnumber,
			name_register,
			aep_data_quality_cd,
			count(1)  tgt_intvl_cnt,
			sum(value) tgt_intvl_usg,		
			aep_opco	
		from 
			${HADOOP_DATA_CONSUME_SCHEMA} 
		where 
			aep_usage_dt in (${INPUT_DATES})
			and aep_opco = '${VAR_OPCO}'
			and name_register in ('E-KWH-15-DEL', 'E-KWH-15-REC')
			and aep_derived_uom = 'KWH'
		group by 
			'${AUDIT_RUN_DT}' ,
			aep_opco,
			aep_usage_dt,
			aep_usage_type,
			serialnumber,
			name_register,
			aep_data_quality_cd

		UNION ALL 

		Select  
			'${AUDIT_RUN_DT}' Run_dt,
			aep_usage_dt,
			case 
			when interval_scalar_flg = 'D1SC' then 'scalar' 
			when interval_scalar_flg = 'D1IN' then 'interval' 
			else 
			'un' 
			end as aep_usage_type, 
			serialnumber serialnumber,	
			measr_comp_type_cd name_register,
			msrmt_cond_flg aep_data_quality_cd,
			count(1)  tgt_intvl_cnt,
			SUM( cast( MSRMT_VAL as integer) ) tgt_intvl_usg,
			aep_opco	
		from 
			stg_vee.data_qlty_reprocess 
		where 
			aep_usage_dt in (${INPUT_DATES})
			and aep_opco = '${VAR_OPCO}'
			and measr_comp_type_cd in ('E-KWH-15-DEL', 'E-KWH-15-REC')
			and interval_scalar_flg = 'D1IN'
		group by 
			'${AUDIT_RUN_DT}' ,
			aep_opco,
			aep_usage_dt,
			case 
			when interval_scalar_flg = 'D1SC' then 'scalar' 
			when interval_scalar_flg = 'D1IN' then 'interval' 
			else 
			'un' 
			end,
			serialnumber,
			measr_comp_type_cd,
			msrmt_cond_flg
		
	)select_rec
	where 
		tgt_intvl_usg = 0
	Group by 
		aep_usage_dt , 
		aep_usage_type , 
		name_register,
		aep_data_quality_cd,
		run_dt , 
		aep_opco ,
		'ZERO_USAGE'
)aggr_tbl
Group BY
	run_dt , 
	aep_opco ,
	'ZERO_USAGE',
	aep_usage_dt, 
	aep_usage_type ,
	name_register,
	aep_data_quality_cd
--order by
--Run_dt,
--aep_opco,
--aep_usage_dt,
--aep_usage_type,
--name_register,
--aep_data_quality_cd
;
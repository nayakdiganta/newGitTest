--UOM
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
	'' aep_data_quality_cd ,
	'' aep_no_of_intvl ,
	cast( sum(tgt_intvl_cnt) as bigint) tgt_intvl_cnt , 
	cast( sum(tgt_intvl_usg) as double) tgt_intvl_usg , 
	sum(tgt_unq_meter_count) tgt_unq_meter_count , 
	cast( current_timestamp as varchar) hdp_insert_dttm , 	
	run_dt , 
	aep_opco ,
	'UOM' aggr_type
from
(
	Select  
		'${AUDIT_RUN_DT}' Run_dt,
		aep_usage_dt,
		aep_usage_type,
		name_register,
		count(1)  tgt_intvl_cnt,
		sum(case when aep_derived_uom in ('KWH','KW') then value else 0 end) tgt_intvl_usg,	
		COUNT(distinct serialnumber) tgt_unq_meter_count,
		aep_opco	
	from 
		${HADOOP_DATA_CONSUME_SCHEMA} 
	where 
		aep_usage_dt in (${INPUT_DATES})
		and aep_opco = '${VAR_OPCO}'
	group by 
		'${AUDIT_RUN_DT}' ,
		aep_opco,
		aep_usage_dt,
		aep_usage_type,
		name_register

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
		measr_comp_type_cd name_register,
		count(1)  tgt_intvl_cnt,
		SUM(case when SUBSTR(measr_comp_type_cd,1,4) = 'E-KW' THEN cast(MSRMT_VAL as integer) ELSE 0 END) tgt_intvl_usg,
		COUNT(distinct serialnumber) tgt_unq_meter_count,
		aep_opco	
	from 
		stg_vee.data_qlty_reprocess 
	where 
		aep_usage_dt in (${INPUT_DATES})
		and aep_opco = '${VAR_OPCO}'
	group by 
		'${AUDIT_RUN_DT}' ,
		aep_opco,
		aep_usage_dt,
		case 
			when interval_scalar_flg = 'D1SC' then 'scalar' 
			when interval_scalar_flg = 'D1IN' then 'interval' 
		else 
			'un' 
		end , 
		measr_comp_type_cd 

) aggr_tbl
Group BY
  run_dt , 
  aep_opco ,
  'UOM',
  aep_usage_dt, 
  aep_usage_type ,
  name_register	
;
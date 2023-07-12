-- DATA_QUALITY
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
	aep_data_quality_cd ,
	'' aep_no_of_intvl ,
	src_intvl_cnt , 
	src_intvl_usg , 
	src_unq_meter_count , 
	cast( current_timestamp as varchar) hdp_insert_dttm ,
	run_dt , 
	aep_opco ,
	aggr_type
from 
(
	select 
		'${AUDIT_RUN_DT}' Run_dt,
		'DATA_QUALITY' aggr_type,
		aep_opco,
		aep_usage_dt,
		aep_usage_type,
		name_register,
		case when aep_data_quality_cd = '501000' then 'REGULAR' Else 'ESTIMATE' END aep_data_quality_cd,
		count(1) src_intvl_cnt,
		sum(case when aep_derived_uom in ('KWH','KW') then value else 0 end) src_intvl_usg,
		COUNT(distinct serialnumber) src_unq_meter_count
	from 
		${HADOOP_DATA_CONSUME_SCHEMA}
	where 	
		aep_usage_dt in (${INPUT_DATES})
		and aep_opco = '${VAR_OPCO}'
		and name_register in ('E-KWH-15-DEL','E-KWH-15-REC')
	group by 
		'${AUDIT_RUN_DT}' ,
		'DATA_QUALITY',
		aep_opco,
		aep_usage_dt ,
		aep_usage_type,
		name_register,
		case when aep_data_quality_cd = '501000' then 'REGULAR' Else 'ESTIMATE' END 
)aggr_tbl
--order by
--Run_dt,
--aep_opco,
--aep_usage_dt,
--aep_usage_type,
--name_register,
--aep_data_quality_cd
;
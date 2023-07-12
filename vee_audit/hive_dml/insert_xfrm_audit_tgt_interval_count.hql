-- INTERVAL_COUNT
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
	cast(aep_no_of_intvl as varchar),
	src_intvl_cnt , 
	cast(src_intvl_usg as double),
	src_unq_meter_count , 
	cast( current_timestamp as varchar) hdp_insert_dttm ,
	run_dt , 
	aep_opco ,
	aggr_type
from
(
	select 
	  run_dt , 
	  aggr_type,
	  aep_opco ,
	  aep_usage_dt , 
	  aep_usage_type , 
	  name_register ,   
	  aep_no_of_intvl, 
	  sum(aep_no_of_intvl) src_intvl_cnt,
	  sum(intvl_usg) src_intvl_usg,
	  COUNT(distinct serialnumber) src_unq_meter_count
	from 
	(
		select 
			'${AUDIT_RUN_DT}' Run_dt,
			'INTERVAL_COUNT' aggr_type,
			aep_opco,
			aep_usage_dt,
			aep_usage_type,
			name_register,
			serialnumber,
			count(1) aep_no_of_intvl,
			sum(case when aep_derived_uom in ('KWH','KW') then value else 0 end) intvl_usg
		from 
			${HADOOP_DATA_CONSUME_SCHEMA} 
		where 	
			aep_usage_dt in (${INPUT_DATES})
			and aep_opco = '${VAR_OPCO}'
			and name_register in('E-KWH-15-DEL','E-KWH-15-REC')
		group by 
			'${AUDIT_RUN_DT}' ,
			'INTERVAL_COUNT',
			aep_opco,
			aep_usage_dt ,
			aep_usage_type,
			name_register,
			serialnumber
	)select_rec
	group by
	  run_dt , 
	  aggr_type,
	  aep_opco ,
	  aep_usage_dt , 
	  aep_usage_type , 
	  name_register ,   
	  aep_no_of_intvl 
) aggr_tbl
--order by
--Run_dt,
--aep_opco,
--aep_usage_dt,
--aep_usage_type,
--name_register
;
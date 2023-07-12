--DATA_QUALITY aggregation
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
	usage_date aep_usage_dt,
	aep_usage_type,
	measr_comp_type_cd name_register,
	case 
		when msrmt_cond_flg IN('501000','501000','501001','501002','501003','501004','501005','501006','501007','501008') then 
			'REGULAR' 
	Else 
		'ESTIMATE' 
	END aep_data_quality_cd,
	'' aep_no_of_intvl ,
	cast( sum(cnt_msrmt_val) as bigint) src_intvl_cnt,
	cast( sum(tot_msrmt_val) as double) src_intvl_usg,
	COUNT(distinct adhoc_char_val) src_unq_meter_count,
	cast( current_timestamp as varchar) hdp_insert_dttm , 

	'${AUDIT_RUN_DT}' Run_dt,
	aep_opco,
	'DATA_QUALITY' aggr_type
from 
	stg_vee.stg_measurement_audit
where 	
	aep_usage_type = 'interval' 
	and measr_comp_type_cd in('E-KWH-15-DEL','E-KWH-15-REC')
	and usage_date < '${AUDIT_RUN_DT}'
group by 
	'${AUDIT_RUN_DT}' ,
	'DATA_QUALITY',
	aep_opco,
	usage_date ,
	aep_usage_type,
	measr_comp_type_cd,
	case when msrmt_cond_flg IN('501000','501000','501001','501002','501003','501004','501005','501006','501007','501008') then 'REGULAR' Else 'ESTIMATE' END ;
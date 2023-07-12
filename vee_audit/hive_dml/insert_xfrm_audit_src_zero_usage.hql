--ZERO_USAGE aggregation
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
  aep_data_quality_cd ,
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
	  run_dt , 
	  aggr_type,
	  aep_opco ,
	  aep_usage_dt , 
	  aep_usage_type , 
	  name_register ,   
	  aep_data_quality_cd ,
	  sum(aep_no_of_intvl) src_intvl_cnt, 
	  count(serialnumber) src_unq_meter_count,
	  sum(intvl_usg) src_intvl_usg
	from 
		(
			select 
				'${AUDIT_RUN_DT}' Run_dt,
				'ZERO_USAGE' aggr_type,
				aep_opco,
				usage_date aep_usage_dt,
				aep_usage_type,
				measr_comp_type_cd name_register,
				split(adhoc_char_val,'-')[1] serialnumber,
				msrmt_cond_flg aep_data_quality_cd,
				sum(cnt_msrmt_val) aep_no_of_intvl,
				sum(tot_msrmt_val) intvl_usg
			from 
				stg_vee.stg_measurement_audit
			where 	
				aep_usage_type = 'interval' 
				and measr_comp_type_cd in('E-KWH-15-DEL','E-KWH-15-REC')
			--	and usage_date > '${START_DATE}'	
				and usage_date < '${AUDIT_RUN_DT}'
			group by 
				'${AUDIT_RUN_DT}' ,
				'ZERO_USAGE',
				aep_opco,
				usage_date ,
				aep_usage_type,
				measr_comp_type_cd,
				split(adhoc_char_val,'-')[1],
				msrmt_cond_flg
		)select_rec
	where
		intvl_usg = 0
	group by
	  run_dt , 
	  aggr_type,
	  aep_opco ,
	  aep_usage_dt , 
	  aep_usage_type , 
	  name_register ,
	  aep_data_quality_cd
)aggr_tbl;
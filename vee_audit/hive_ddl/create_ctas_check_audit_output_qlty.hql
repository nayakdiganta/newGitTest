CREATE TABLE stg_vee.ctas_check_audit_output_qlty
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/ctas_check_audit_output_qlty/',
      format = 'TEXTFILE',
      field_delimiter = ',',
	  write_compression = 'NONE'
      )
AS
select 
	aep_opco,
	aep_usage_dt,

	src_regular as src_regular_cnt,
	tgt_regular as tgt_regular_cnt,
	src_regular - tgt_regular as diff_reg_intvl_cnt,

	src_estimate as src_estimate,
	tgt_estimate as tgt_estimate,
	src_estimate - tgt_estimate as diff_est_intvl_cnt,

	src_total as src_total_cnt,
	tgt_total as tgt_total_cnt,
	src_total - tgt_total as diff_tot_intvl_cnt,


	((src_regular*1.0)/(src_total*1.0))*100 src_reg_cnt_per,
	( (src_estimate*1.0)/(src_total*1.0))*100 src_est_cnt_per,

	((tgt_regular*1.0)/(tgt_total*1.0))*100 tgt_reg_cnt_per,
	( (tgt_estimate*1.0)/(tgt_total*1.0 ))*100 tgt_est_cnt_per,
	

	run_dt as run_dt,
	aggr_type  as aggr_type,
	 
	case 
		when (( (src_estimate*1.0 )/(src_total*1.0) )*100) > ${PERC_DIFF_TO_ALERT} then 'SOURCE NON REGULAR INTEVAL COUNT > ${PERC_DIFF_TO_ALERT}%'
		when (( (tgt_estimate*1.0)/(tgt_total *1.0) )*100) > ${PERC_DIFF_TO_ALERT} then 'TARGET NON REGULAR INTEVAL COUNT > ${PERC_DIFF_TO_ALERT}%'
	else
		'NO ISSUE'
	end as issue_type

from (
		select 
		run_dt,
		aep_opco,
		aggr_type, 
		aep_usage_dt,
		aep_usage_type,
		sum(case when aep_data_quality_cd = 'ESTIMATE' then src_intvl_cnt else 0 end) src_estimate,
		sum(case when aep_data_quality_cd = 'REGULAR' then src_intvl_cnt else 0 end) src_regular,
		sum(src_intvl_cnt) src_total,

		sum(case when aep_data_quality_cd = 'ESTIMATE' then tgt_intvl_cnt else 0 end) tgt_estimate,
		sum(case when aep_data_quality_cd = 'REGULAR' then tgt_intvl_cnt else 0 end) tgt_regular,
		sum(tgt_intvl_cnt) tgt_total
		 
		from 
		usage_vee.audit_reading_ivl_vee_parquet 
		where
		aggr_type = 'DATA_QUALITY'  
		and name_register in ('E-KWH-15-DEL','E-KWH-15-REC')
		and run_dt = '${AUDIT_RUN_DT}' 
		and aep_usage_dt > '${START_DATE}' 
		group by
		run_dt,
		aep_opco,
		aggr_type,
		aep_usage_dt,
		aep_usage_type
	) max_cnt
where 
	(( (tgt_estimate*1.0)/(tgt_total*1.0) )*100) > ${PERC_DIFF_TO_ALERT}
order by 
run_dt,
aep_opco,
aggr_type,
aep_usage_dt
;
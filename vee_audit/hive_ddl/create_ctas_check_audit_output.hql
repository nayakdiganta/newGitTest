CREATE TABLE stg_vee.ctas_check_audit_output
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/ctas_check_audit_output/',
      format = 'TEXTFILE',
      field_delimiter = ',',
	  write_compression = 'NONE'
      )
AS
select 
	calculate_diff.aep_opco as aep_opco,
	calculate_diff.aep_usage_dt as aep_usage_dt,
	calculate_diff.src_intvl_cnt as src_intvl_cnt,
	calculate_diff.tgt_intvl_cnt as tgt_intvl_cnt,
	calculate_diff.src_intvl_usg as src_intvl_usg,
	calculate_diff.tgt_intvl_usg as tgt_intvl_usg,
	calculate_diff.src_unq_meter_count as src_unq_meter_count,
	calculate_diff.tgt_unq_meter_count as tgt_unq_meter_count,

	calculate_diff.diff_intvl_cnt as diff_intvl_cnt,
	calculate_diff.diff_unq_meter_count as diff_unq_meter_count,
	calculate_diff.diff_intvl_usg as diff_intvl_usg,
	calculate_diff.max_tgt_intvl_cnt as max_tgt_intvl_cnt,

	calculate_diff.per_diff_intvl_cnt as per_diff_intvl_cnt,
	calculate_diff.per_diff_unq_mtr as per_diff_unq_mtr,
	calculate_diff.per_diff_intvl_usg as per_diff_intvl_usg,
	calculate_diff.per_max_tgt_intvl_cnt as per_max_tgt_intvl_cnt,

	calculate_diff.run_dt as run_dt,
	calculate_diff.aggr_type as aggr_type,
	case 
		when (per_diff_intvl_cnt > ${PERC_DIFF_TO_ALERT} OR per_diff_unq_mtr > ${PERC_DIFF_TO_ALERT} OR per_diff_intvl_usg > ${PERC_DIFF_TO_ALERT}) and per_max_tgt_intvl_cnt > 5 then 'SOURCE AND TARGET ISSUE'
		when (per_diff_intvl_cnt < ${PERC_DIFF_TO_ALERT} and per_diff_unq_mtr < ${PERC_DIFF_TO_ALERT} and per_diff_intvl_usg < ${PERC_DIFF_TO_ALERT}) and per_max_tgt_intvl_cnt > 5 then 'SOURCE ISSUE'
		when (per_diff_intvl_cnt > ${PERC_DIFF_TO_ALERT} OR per_diff_unq_mtr > ${PERC_DIFF_TO_ALERT} OR per_diff_intvl_usg > ${PERC_DIFF_TO_ALERT})  and per_max_tgt_intvl_cnt < 5 then 'TARGET ISSUE'
	end as issue_type
from 
	(
		select 
			vee.aep_opco,
			aep_usage_dt,
			coalesce(src_intvl_cnt,0) src_intvl_cnt,
			coalesce(tgt_intvl_cnt,0) tgt_intvl_cnt,
			coalesce(src_intvl_usg,0.0) src_intvl_usg,
			coalesce(tgt_intvl_usg,0.0) tgt_intvl_usg,
			coalesce(src_unq_meter_count,0) src_unq_meter_count,
			coalesce(tgt_unq_meter_count,0) tgt_unq_meter_count,
			coalesce(diff_intvl_cnt,0) diff_intvl_cnt,
			coalesce(per_diff_intvl_cnt,0.0) per_diff_intvl_cnt,
			coalesce(diff_unq_meter_count,0) diff_unq_meter_count,
			coalesce(per_diff_unq_mtr,0.0) per_diff_unq_mtr,
			coalesce(diff_intvl_usg,0) diff_intvl_usg,
			coalesce(per_diff_intvl_usg,0.0) per_diff_intvl_usg,
			coalesce(max_cnt.max_tgt_intvl_cnt,0) max_tgt_intvl_cnt,
			case when coalesce(max_cnt.max_tgt_intvl_cnt,0) =0 then 0.0 else  ((( max_cnt.max_tgt_intvl_cnt - src_intvl_cnt)*1.0) /coalesce(max_cnt.max_tgt_intvl_cnt,0))*100 end per_max_tgt_intvl_cnt,
			run_dt,
			aggr_type 
		from 
			usage_vee.audit_reading_ivl_vee_parquet vee
		join 
			(
				select 
					aep_opco,
					max(tgt_intvl_cnt) max_tgt_intvl_cnt
				from 
					usage_vee.audit_reading_ivl_vee_parquet 
				where
					aggr_type = 'USAGE_DATE' 
					and run_dt = '${AUDIT_RUN_DT}' 
				--and run_dt = '2019-03-25'
				Group by aep_opco
			)max_cnt on max_cnt.aep_opco = vee.aep_opco
		where 
			aggr_type = 'USAGE_DATE' 
		--	and run_dt = '2019-03-25'
		and run_dt = '${AUDIT_RUN_DT}' 
		and aep_usage_dt > '${START_DATE}' 
	)calculate_diff
where 
	per_diff_intvl_cnt > ${PERC_DIFF_TO_ALERT} OR per_diff_unq_mtr > ${PERC_DIFF_TO_ALERT} OR per_diff_intvl_usg > ${PERC_DIFF_TO_ALERT} OR per_max_tgt_intvl_cnt > 5
order by 	
calculate_diff.aep_opco,
calculate_diff.aep_usage_dt
;
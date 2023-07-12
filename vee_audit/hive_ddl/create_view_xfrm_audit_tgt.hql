DROP TABLE IF EXISTS stg_vee.xfrm_audit_reading_ivl_vee_tgt;

CREATE OR REPLACE VIEW stg_vee.xfrm_audit_reading_ivl_vee_tgt 
AS 
SELECT 
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	intvl_cnt,
	intvl_usg,
	unq_meter_count,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
FROM stg_vee.xfrm_audit_reading_ivl_vee_tgt_oh 
WHERE aep_opco = 'oh'
UNION ALL 
SELECT 
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	intvl_cnt,
	intvl_usg,
	unq_meter_count,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
FROM stg_vee.xfrm_audit_reading_ivl_vee_tgt_tx
WHERE aep_opco = 'tx'
UNION ALL 
SELECT 
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	intvl_cnt,
	intvl_usg,
	unq_meter_count,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
FROM stg_vee.xfrm_audit_reading_ivl_vee_tgt_ap
WHERE aep_opco = 'ap'
UNION ALL 
SELECT 
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	intvl_cnt,
	intvl_usg,
	unq_meter_count,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
FROM stg_vee.xfrm_audit_reading_ivl_vee_tgt_pso
WHERE aep_opco = 'pso'
UNION ALL 
SELECT 
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	intvl_cnt,
	intvl_usg,
	unq_meter_count,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
FROM stg_vee.xfrm_audit_reading_ivl_vee_tgt_im
WHERE aep_opco = 'im'
UNION ALL 
SELECT 
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_data_quality_cd,
	aep_no_of_intvl,
	intvl_cnt,
	intvl_usg,
	unq_meter_count,
	hdp_insert_dttm,
	run_dt,
	aep_opco,
	aggr_type
FROM stg_vee.xfrm_audit_reading_ivl_vee_tgt_swp
WHERE aep_opco = 'swp';

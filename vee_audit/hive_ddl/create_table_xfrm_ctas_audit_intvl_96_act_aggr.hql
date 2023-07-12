CREATE TABLE stg_vee.xfrm_ctas_audit_intvl_96_act_aggr
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_96_act_aggr/',
      format = 'TEXTFILE',
      field_delimiter = ','
      )
AS 
select  
	aep_opco,
	aep_usage_type,
	aep_usage_dt,
	name_register,
	serialnumber,
	'96' as aep_no_of_intvl,
	tot_msrmt_val,
	avg_msrmt_val,
	cnt_msrmt_val

from stg_vee.xfrm_ctas_audit_intvl_cnt_src 
where  cnt_msrmt_val=96 and aep_data_quality_cd = 'A';


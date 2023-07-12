CREATE TABLE stg_vee.xfrm_ctas_audit_intvl_96_est_aggr
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_96_est_aggr/',
      format = 'TEXTFILE',
      field_delimiter = ','
      )
AS 
  
select  
	intvl.aep_opco,
	intvl.aep_usage_type,
	intvl.aep_usage_dt,
	intvl.name_register,
	intvl.serialnumber,
	'96' as aep_no_of_intvl,
	tot_msrmt_val,
	avg_msrmt_val,
	cnt_msrmt_val
from stg_vee.xfrm_ctas_audit_intvl_cnt_src intvl
where 
not exists 
(
	select 1 from 
	(
		select  distinct aep_opco,aep_usage_type,aep_usage_dt,name_register,serialnumber
		from stg_vee.xfrm_ctas_audit_intvl_96_act_aggr
	) mtr where mtr.aep_opco = intvl.aep_opco 
	and mtr.aep_usage_type = intvl.aep_usage_type and mtr.aep_usage_dt = intvl.aep_usage_dt and mtr.serialnumber = intvl.serialnumber and mtr.name_register = intvl.name_register
) and cnt_msrmt_val = 96 and row_num =1;

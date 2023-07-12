CREATE TABLE stg_vee.xfrm_ctas_audit_intvl_aggr
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_aggr/',
      format = 'TEXTFILE',
      field_delimiter = ','
      )
AS 
select  
	aep_opco,
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_no_of_intvl aep_no_of_intvl,
	sum(cnt_msrmt_val) cnt_msrmt_val,
	sum(tot_msrmt_val) tot_msrmt_val,
	count(distinct serialnumber) unq_srl_nb
from 
	(
		select  
		aep_opco,
		aep_usage_type,
		aep_usage_dt,
		name_register,
		serialnumber,
		aep_no_of_intvl,
		tot_msrmt_val,
		avg_msrmt_val,
		cnt_msrmt_val
		from stg_vee.xfrm_ctas_audit_intvl_96_act_aggr intvl
		union all
		select  
		aep_opco,
		aep_usage_type,
		aep_usage_dt,
		name_register,
		serialnumber,
		aep_no_of_intvl,
		tot_msrmt_val,
		avg_msrmt_val,
		cnt_msrmt_val
		from stg_vee.xfrm_ctas_audit_intvl_96_est_aggr intvl
		UNION ALL
		select  
		aep_opco,
		aep_usage_type,
		aep_usage_dt,
		name_register,
		serialnumber,
		aep_no_of_intvl,
		tot_msrmt_val,
		avg_msrmt_val,
		cnt_msrmt_val 
		from  stg_vee.xfrm_ctas_audit_intvl_not_96_aggr	
	)select_rec
group by
	aep_opco,
	aep_usage_dt,
	aep_usage_type,
	name_register,
	aep_no_of_intvl
order by 
aep_opco,
aep_usage_type,
aep_usage_dt
;
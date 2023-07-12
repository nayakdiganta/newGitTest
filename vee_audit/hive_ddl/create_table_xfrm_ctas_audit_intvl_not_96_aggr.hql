CREATE TABLE stg_vee.xfrm_ctas_audit_intvl_not_96_aggr
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_not_96_aggr/',
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
--case when aep_no_of_intvl = '96' then '96' when aep_no_of_intvl < '96' then '<96' else '>96' end aep_no_of_intvl,
cast(aep_no_of_intvl as varchar) aep_no_of_intvl,
--sum(tot_msrmt_val) tot_msrmt_val,
tot_msrmt_val,
--avg(avg_msrmt_val) avg_msrmt_val,
avg_msrmt_val,
--sum(intvl.aep_no_of_intvl) cnt_msrmt_val
cast(aep_no_of_intvl as INT) cnt_msrmt_val
from 
(
	select  
		intvl.aep_opco,
		intvl.aep_usage_type,
		intvl.aep_usage_dt,
		intvl.name_register,
		intvl.serialnumber,
		sum(tot_msrmt_val) tot_msrmt_val,
		avg(avg_msrmt_val) avg_msrmt_val,
		sum(intvl.cnt_msrmt_val) aep_no_of_intvl
	from stg_vee.xfrm_ctas_audit_intvl_cnt_src intvl

	where cnt_msrmt_val <> 96 and row_num =1

	group by
	aep_opco,
	aep_usage_type,
	aep_usage_dt,
	name_register,
	serialnumber
) intvl;


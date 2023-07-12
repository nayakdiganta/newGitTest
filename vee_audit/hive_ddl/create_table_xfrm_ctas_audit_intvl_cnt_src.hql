CREATE TABLE stg_vee.xfrm_ctas_audit_intvl_cnt_src 
WITH (
         external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_cnt_src/',
      format = 'TEXTFILE',
      field_delimiter = ','
      )
AS 
select  
	device_config_id,
	aep_opco,
	aep_usage_type,
	usage_date aep_usage_dt,
	measr_comp_id  ,
	split(adhoc_char_val,'-')[1] as serialnumber,
	split(adhoc_char_val,'-')[3] as Device_cd,
	split(adhoc_char_val,'-')[4] as prog_id,
	measr_comp_type_cd name_register,
	case when msrmt_cond_flg IN('501000','501000','501001','501002','501003','501004','501005','501006','501007','501008') then 'A' Else 'E' END aep_data_quality_cd,
	tot_msrmt_val,
	avg_msrmt_val,
	cnt_msrmt_val
	,dense_rank() over (partition by aep_opco, aep_usage_type, usage_date, split(adhoc_char_val,'-')[1], measr_comp_type_cd order by device_config_id desc,measr_comp_id desc) as row_num
from 
	stg_vee.stg_measurement_audit
where 	
	aep_usage_type = 'interval' 
	and measr_comp_type_cd in('E-KWH-15-DEL','E-KWH-15-REC')
--	and usage_date > '${START_DATE}'	
	and usage_date < '${AUDIT_RUN_DT}'



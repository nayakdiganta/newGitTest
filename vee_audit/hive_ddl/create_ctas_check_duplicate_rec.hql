CREATE TABLE stg_vee.ctas_check_duplicate_rec
WITH (
      external_location = 's3://aep-datalake-work-${env}/raw/intervals/vee/audit/ctas_check_duplicate_rec/',
      format = 'TEXTFILE',
      field_delimiter = ',',
	  write_compression = 'NONE'
      )
AS
Select rowkey, count(1) _c1
from usage_vee.reading_ivl_vee 
where aep_opco='oh' and aep_derived_uom='KWH' and aep_usage_dt >= '${ARCHIEVE_DATE}' 
group by rowkey having count(1) > 1;
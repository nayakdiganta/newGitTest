#!/bin/bash 

export aws_env=$1
echo "$aws_env"
export aws_region='us-east-1'
echo "$aws_region"
export S3DataLogBucket='aep-dl-log'
echo "$S3DataLogBucket"
export s3_temp_loc="s3://${S3DataLogBucket}-${aws_env}/glueetl/temporary/"
echo "$s3_temp_loc"
export ATHENA_WORK_GROUP="HdpDeveloper"

function fnLog()
{
	echo `date +"%Y-%m-%d %k:%M:%S"` $1
}

function log_error_exit()
{
	date
	ret=1
	exit ${ret}
}

function drop_create_tables()
{
	##===========================================
	## DROP Tables

	

	## 3. stg_meterevents.end_device_event_incr tbl drop
	end_device_event_incr_tbl_drop_qry='DROP TABLE IF EXISTS stg_meterevents.end_device_event_incr'
	echo "end_device_event_incr_tbl_drop_qry: $end_device_event_incr_tbl_drop_qry"
	end_device_event_incr_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$end_device_event_incr_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "end_device_event_incr_tbl_drop_qry_eid: $end_device_event_incr_tbl_drop_qry_eid"
	sleep 5
	end_device_event_incr_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $end_device_event_incr_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${end_device_event_incr_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.end_device_event_incr table drop succeeded."
	else
		fnLog "stg_meterevents.end_device_event_incr table drop failed."
		log_error_exit
	fi

	echo "============================"

	


	##===========================================
	## CREATE Tables

	
	#3. create stg_meterevents.end_device_event_incr table
	end_device_event_incr_tbl_create_qry="CREATE EXTERNAL TABLE stg_meterevents.end_device_event_incr(
	issuertracking_id  string, 
    issuer_id  string, 
  	serialnumber string, 
  	enddeviceeventtypeid string, 
  	aep_timezone_cd string, 
  	valuesinterval string, 
	aep_devicecode string, 
	aep_mtr_pnt_nb string, 
	aep_tarf_pnt_nb string, 
	aep_premise_nb string, 
	aep_service_point string, 
	aep_bill_account_nb string, 
	reason string, 
	user_id string, 
	manufacturer_id string, 
	domain string, 
	eventoraction string, 
	sub_domain string, 
	event_type string, 
	aep_state string, 
	aep_area_cd string, 
	aep_sub_area_cd string, 
	longitude string, 
	latitude string, 
	aep_city string, 
	aep_zip string, 
	hdp_update_user string, 
	hdp_insert_dttm timestamp, 
	hdp_update_dttm timestamp)
	PARTITIONED BY ( 
	run_dt string COMMENT 'run_dt in yyyyMMdd_HHMMSS format',
	aep_opco string COMMENT 'operating company: e.g. ap, oh, pso', 
	aep_event_dt string COMMENT 'event date from stg_valuesinterval')
	ROW FORMAT SERDE 
	'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
	STORED AS INPUTFORMAT 
	'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
	OUTPUTFORMAT 
	'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
	LOCATION
	's3://aep-datalake-work-${aws_env}/util/events/uiq/end_device_event_incr'
	TBLPROPERTIES (
	'bucketing_version'='2', 
	'orc.bloom.filter.columns'='serialnumber', 
	'orc.bloom.filter.fpp'='0.05', 
	'orc.compress'='SNAPPY', 
	'orc.create.index'='true', 
	'orc.row.index.stride'='10000', 
	'orc.stripe.size'='268435456', 
	'transient_lastDdlTime'='1677511877');"
	echo "end_device_event_incr_tbl_create_qry: $end_device_event_incr_tbl_create_qry"
	end_device_event_incr_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$end_device_event_incr_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "end_device_event_incr_tbl_create_qry_eid: $end_device_event_incr_tbl_create_qry_eid"
	sleep 5
	end_device_event_incr_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $end_device_event_incr_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${end_device_event_incr_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.end_device_event_incr tbl creation succeeded."
	else
		fnLog "stg_meterevents.end_device_event_incr tbl creation failed."
		log_error_exit
	fi

	echo "============================"
	
}

function main()
{
    if [ $# -ne 1 ]; then
	    # Wrong number of arguments, exit the script.	    
		echo "Wrong number of arguments. Pls provide env argument, exiting the script."
	    log_error_exit
    fi  

	fnLog "========================== DB AND TABLE DROP/CREATION PROCESS STARTED =========================="
	drop_create_tables
	fnLog "==========================  DB AND TABLE DROP/CREATION PROCESS ENDED  =========================="
}

main "${@}"
exit 0


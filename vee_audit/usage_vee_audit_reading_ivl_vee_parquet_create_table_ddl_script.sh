#!/bin/bash 

##/**------------------------------------------------------------------------------------------**/
##/**          AMERICAN ELECTRIC POWER - usage_vee.audit_reading_ivl_vee_parquet               **/
##/**------------------------------------------------------------------------------------------**/
##/**                               Confidentiality Information:                               **/
##/**                               Copyright 2022 by                                          **/
##/**                               American Electric Power                                    **/
##/**                                                                                          **/
##/** This module is confidential and proprietary information of American Electric             **/
##/** Power, it is not to be copied or reproduced in any form, by any means, in                **/
##/** whole or in part, nor is it to be used for any purpose other than that for               **/
##/** which it is expressly provide without written permission of AEP.                         **/
##/**------------------------------------------------------------------------------------------**/
##/** AEP Custom Changes                                                                       **/
##/**  Version #   Name       Date        Description                                          **/
##/**   V0.1   Diganta     12/20/2022  DDL script for usage_vee.audit_reading_ivl_vee_parquet  **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/

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

function create_db_and_table()
{
	# usage_vee.audit_reading_ivl_vee_parquet table drop
	tbl_create_qry="CREATE EXTERNAL TABLE IF NOT EXISTS usage_vee.audit_reading_ivl_vee_parquet(
	 aep_usage_dt string,
	 aep_usage_type string,
	 name_register string,
	 aep_data_quality_cd string,
	 aep_no_of_intvl string,
	 src_intvl_cnt bigint,
	 tgt_intvl_cnt bigint,
	 diff_intvl_cnt bigint,
	 per_diff_intvl_cnt decimal(7,4),
	 src_intvl_usg double,
	 tgt_intvl_usg double,
	 diff_intvl_usg decimal(9,2),
	 per_diff_intvl_usg decimal(7,4),
	 src_unq_meter_count bigint,
	 tgt_unq_meter_count bigint,
	 diff_unq_meter_count bigint,
	 per_diff_unq_mtr decimal(7,4),
	 hdp_insert_dttm string)
	PARTITIONED BY ( 
	   run_dt string, 
	   aep_opco string, 
	   aggr_type string)
	STORED AS PARQUET
	LOCATION
	 's3://aep-datalake-consume-${aws_env}/util/intervals/audit_reading_ivl_vee_parquet'
	TBLPROPERTIES (
   'bucketing_version'='2',
   'parquet.bloom.filter.columns'='aggr_type',
   'parquet.bloom.filter.fpp'='0.05',
   'parquet.compression'='SNAPPY',
   'parquet.create.index'='true' );"
	echo "tbl_create_qry: $tbl_create_qry"
	tbl_qry_executionid=$(aws athena start-query-execution --region $aws_region --query-string "$tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "tbl_qry_executionid: $tbl_qry_executionid"
	sleep 5
	tbl_qry_status=$(aws athena get-query-execution --query-execution-id $tbl_qry_executionid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${tbl_qry_status}" == "SUCCEEDED" ]; then
		fnLog "usage_vee.audit_reading_ivl_vee_parquet creation succeeded."
	else
		fnLog "usage_vee.audit_reading_ivl_vee_parquet creation failed."
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

	fnLog "========================== DB CREATION PROCESS STARTED =========================="
	create_db_and_table
	fnLog "========================== DB CREATION PROCESS ENDED =========================="
}

main "${@}"
exit 0


#!/bin/bash 

##/**------------------------------------------------------------------------------------------**/
##/**          AMERICAN ELECTRIC POWER - mdm_intvl_vee audit stg                               **/
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
##/**  Version #   Name            Date        Description                                     **/
##/**   V0.1      Diganta        12/20/2022  DDL script for mdm_vee_audit                      **/
##/**                                                                                          **/
##/**------------------------------------------------------------------------------------------**/

export aws_env=$1
echo "$aws_env"
export VAR_OPCO=$2
echo "$VAR_OPCO"
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
	# stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO} create
	stg_tbl_create_qry="CREATE EXTERNAL TABLE IF NOT EXISTS stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO}(
	 aep_usage_dt string, 
	 aep_usage_type string, 
	 name_register string, 
	 aep_data_quality_cd string, 
	 aep_no_of_intvl string, 
	 intvl_cnt bigint, 
	 intvl_usg double, 
	 unq_meter_count bigint, 
	 hdp_insert_dttm string)
	PARTITIONED BY ( 
	  run_dt string, 
	  aep_opco string, 
	  aggr_type string)
	STORED AS PARQUET
	LOCATION
	  's3://aep-datalake-work-${aws_env}/raw/intervals/vee/audit/xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO}'
	TBLPROPERTIES (
	  'bucketing_version'='2',
	  'parquet.bloom.filter.columns'='aggr_type',
	  'parquet.bloom.filter.fpp'='0.05',
	  'parquet.compression'='SNAPPY',
	  'parquet.create.index'='true' );"
	echo "stg_tbl_create_qry: $stg_tbl_create_qry"
	stg_tbl_qry_executionid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_tbl_qry_executionid: $stg_tbl_qry_executionid"
	sleep 5
	stg_tbl_qry_status=$(aws athena get-query-execution --query-execution-id $stg_tbl_qry_executionid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_tbl_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg tbl stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO} creation succeeded."
	else
		fnLog "stg tbl stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO} creation failed."
		log_error_exit
	fi

	echo "============================"


}

function main()
{
    if [ $# -ne 2 ]; then
	    # Wrong number of arguments, exit the script.	    
		echo "Wrong number of arguments. Pls provide env and opco argument, exiting the script."
	    log_error_exit
    fi  

	fnLog "========================== AUDIT STG TABLE CREATION PROCESS STARTED =========================="
	create_db_and_table
	fnLog "========================== AUDIT STG TABLE CREATION PROCESS ENDED =========================="
}

main "${@}"
exit 0


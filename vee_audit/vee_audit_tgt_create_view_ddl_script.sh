#!/bin/bash 

##/**------------------------------------------------------------------------------------------**/
##/**          AMERICAN ELECTRIC POWER - CTIES GIS Meter-Transform Mapping Data                **/
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
##/**   V0.1      Diganta        12/20/2022  DDL script for mdm_vee_audit  view                **/
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
	# stg_vee.xfrm_audit_reading_ivl_vee_tgt table drop
	stg_tbl_drop_qry="DROP TABLE IF EXISTS stg_vee.xfrm_audit_reading_ivl_vee_tgt;"
	echo "stg_tbl_drop_qry: $stg_tbl_drop_qry"
	stg_tbl_qry_executionid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_tbl_qry_executionid: $stg_tbl_qry_executionid"
	sleep 5
	stg_tbl_qry_status=$(aws athena get-query-execution --query-execution-id $stg_tbl_qry_executionid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_tbl_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg tbl stg_vee.xfrm_audit_reading_ivl_vee_tgt drop succeeded."
	else
		fnLog "stg tbl stg_vee.xfrm_audit_reading_ivl_vee_tgt drop failed."
		log_error_exit
	fi

	echo "============================"


	# stg_vw_create
	stg_vw_create_qry="CREATE OR REPLACE VIEW stg_vee.xfrm_audit_reading_ivl_vee_tgt 
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
	WHERE aep_opco = 'swp';"
	echo "stg_vw_create_qry: $stg_vw_create_qry"
	stg_vw_qry_executionid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_vw_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_vw_qry_executionid: $stg_vw_qry_executionid"
	sleep 5
	stg_vw_qry_status=$(aws athena get-query-execution --query-execution-id $stg_vw_qry_executionid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_vw_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg vw stg_vee.xfrm_audit_reading_ivl_vee_tgt creation succeeded."
	else
		fnLog "stg vw stg_vee.xfrm_audit_reading_ivl_vee_tgt creation failed."
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


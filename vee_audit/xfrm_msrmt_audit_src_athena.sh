##                    AMERICAN ELECTRIC POWER - AMI Data Interval                           ##
##------------------------------------------------------------------------------------------##
##                               Confidentiality Information:                               ##
##                               Copyright 2017 by                                          ##
##                               American Electric Power                                    ##
##                                                                                          ##
## This module is confidential and proprietary information of American Electric             ##
## Power, it is not to be copied or reproduced in any form, by any means, in                ##
## whole or in part, nor is it to be used for any purpose other than that for               ##
## which it is expressly provide without written permission of AEP.                         ##
##------------------------------------------------------------------------------------------##
## AEP Custom Changes                                                                       ##
##  Version #   Name                                    Date        Description             ##
##                                                                                          ##
##  V.001    Ramesh Sethuraj                            03/22/2019     Initial              ##
##                                                                                          ##
##------------------------------------------------------------------------------------------##
## This script is to ingest d1_msrmt table incrementally along with other tables as group   ##
## from Oracle MDM database                                                                 ##
## USAGE: sh stg_import_msrmt.sh P[1-10]                                                    ##
##------------------------------------------------------------------------------------------##
#!/bin/bash

. /aep/home/hdpapp/mdm_intvl_vee/scripts/.mdm_intvl_vee_env
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MODULE="xfrm_msrmt_audit_src-$AUTO_JOB_NAME"
script_name="$(basename ${BASH_SOURCE[0]})"
PROCESS_DAY=`date +%Y%m%d%H%M%S`
#LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
env=$AWS_ENV
#echo $LOGFILE

#VAR_OPCO=$1

log_error_exit()
{
  date
  ret=1

mailx -s "MDMVEE - Error Processing during ${MODULE}  - `date`" -a ${LOGFILE} -rHadoopBatchErrors ${MDMVEE_EMAIL} <<!
Error Occured During Processing in ${MODULE}
Please Review the Log File at ${LOGILE} for Errors and Reprocess the Job.
!

  exit ${ret}
}

function initialize_log_file(){
    # Log File initialization
    LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
    ## Logic to Cleanup the Old Log Files 
	## Remove older than 7 days of log files
	find $MDMVEE_LOGS -type f -name ${MODULE}_log_* -mtime +7 -exec rm -f {} \;
	fnLog  "==========  VEE AUDIT SRC SCRIPT BEGIN  =========="
}

function initialize_variables(){
	echo "Start Source Aggregation `date `"

	AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
	START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`

	## This is to limit the records updated after the ingestion cutoff time
	### commented out by Diganta as it is not being used anywhere
	##HDP_LAST_RUN_DT=`cat $MDMVEE_PARMS/last_run_dt.txt`

	echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"
	echo "START_DATE=${START_DATE}"
	##echo "HDP_LAST_RUN_DT=${HDP_LAST_RUN_DT}"

}

function fnLog(){
	echo `date +"%Y-%m-%d %k:%M:%S"` $1 
	echo `date +"%Y-%m-%d %k:%M:%S"` $1 >> ${LOGFILE}
}

create_athena_objects(){

ATHENA_WORK_GROUP="HdpDeveloper"
#ATHENA_WORK_GROUP="AnalyticSvcs"

ATHENA_QUERY_OUTPUT="s3://aep-datalake-user-data-${AWS_ENV}/athena_result_sets/lambda_partitions_update/"



	athena_input_file=$1
	
	echo 'start create_athena_objects'
	echo $athena_input_file
	
	if [[ -z "${athena_input_file}" ]];then
		fnLog "athena_input_file is empty"
		log_error_exit
	fi
	
	if [[ -z "${ATHENA_WORK_GROUP}" ]];then
		fnLog "ATHENA_WORK_GROUP is empty"
		log_error_exit
	fi

	if [[ -z "${ATHENA_QUERY_OUTPUT}" ]];then
		fnLog "ATHENA_QUERY_OUTPUT is empty"
		log_error_exit
	fi
	
	athena_execution_id="${athena_input_file}.execution_id.txt"
	>${athena_execution_id}
	execution_id=""
	ls -l ${athena_input_file}
	
	fnLog "submit file ${athena_input_file} to Athena"
	echo ""
	fnLog "===================================================================="	
	echo ""	
	cat ${athena_input_file}
	echo ""
	fnLog "===================================================================="	
	echo ""
	#--query-execution-context Database=${db_name},,Catalog=${catalog}
	
	fnLog "aws athena start-query-execution --query-string file://${athena_input_file}  --result-configuration OutputLocation="${ATHENA_QUERY_OUTPUT}" --work-group ${ATHENA_WORK_GROUP} --output text"

	execution_id=`aws athena start-query-execution --query-string file://${athena_input_file}  --result-configuration OutputLocation="${ATHENA_QUERY_OUTPUT}" --work-group ${ATHENA_WORK_GROUP} --output text`
	ret=$?
	
	fnLog "Athena Execution id=${execution_id}"
	echo ""
	echo ""
	echo ""		
	if [[ $ret -eq 0 ]];then
		if [[ -z ${execution_id} ]];then
			fnLog "Missing execution_id ${execution_id}"
			log_error_exit
		fi	
		echo ${execution_id} >${athena_execution_id}
		check_athena_query_status
		execution_id=`cat ${athena_input_file}.execution_id.txt`				
	else
		fnLog "Error calling Athena start-query-execution"
		log_error_exit
	fi

	fnLog "===================================================================="
	echo ""
	echo ""
	echo ""		

}

check_athena_query_status(){

	line_cnt=`cat ${athena_execution_id} |wc -l`
	if [[ $line_cnt -gt 0 ]];then
		cp ${athena_execution_id} ${athena_execution_id}.bkup
		loop="TRUE"
		while [[ "${loop}" == "TRUE" ]]
		do
			line_cnt=`cat ${athena_execution_id} |wc -l`
			fnLog "line_cnt=$line_cnt"
			if [[ $line_cnt -eq 0 ]];then
				fnLog "ALL RECORDS PROCESSED"
				loop="FALSE"
				break
			fi
			>${athena_execution_id}.running
			while IFS= read -r line
			do
				if [ ! -z ${line} ];then
	#				fnLog "$line"
					status=`aws athena get-query-execution --query-execution-id "$line" --output json |grep "\"State\":"|tr "\"" " "|cut -d ":" -f2|xargs`
					ret=$?
					if [[ $ret -eq 0 ]];then
						fnLog "Query $line status => ${status}"
						if [[ ${status} == SUCCEEDED* ]];then
							echo "$line" > ${athena_execution_id}.success 
						fi
						if [[ ${status} == FAILED* ]];then
							echo "$line" > ${athena_execution_id}.failed 
							fnLog "Athena Query Failed "
							log_error_exit							
						fi
						if [[ ! ${status} == FAILED* ]] && [[ ! ${status} == SUCCEEDED* ]];then
							fnLog "still running, write id $line to run file => ${athena_execution_id}.running "
							echo "$line" > ${athena_execution_id}.running 
						fi
						sleep 5
					else
						fnLog "Error calling Athena get-query-execution "
						log_error_exit
					fi
				fi
			done < "${athena_execution_id}"
			#fnLog "mv ${athena_execution_id}.running ${athena_execution_id}"
			mv ${athena_execution_id}.running ${athena_execution_id}
		done
	else
		fnLog "No records to process, file might be missing => ${athena_execution_id}"
		ls -l ${athena_execution_id}
		log_error_exit
	fi	

}

function clean_external_location_dir(){
	echo "executing clean_external_location_dir.."
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/clean_external_location_dir.txt
	
	hdfs_extrct_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_audit_reading_ivl_vee_src"

	echo 's3 location to place the exported data from athena'
	echo $hdfs_extrct_path
    echo "start clean_export_dir: $hdfs_extrct_path"
	fnLog "aws s3 rm --recursive ${hdfs_extrct_path}/"
	
	if [[ ! -z ${hdfs_extrct_path} ]];then
		aws s3 rm --recursive ${hdfs_extrct_path}/
	fi
    fnLog "Clean-Up external_location_dir ${hdfs_extrct_path}"
	echo "clean_external_location_dir succeeded.."

}

function drop_create_athena_table(){
	echo "executing drop_create_athena_table.."
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/clean_external_location_dir.txt
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/drop_create_athena_table.txt
	
	##drop the table if exists
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_audit_reading_ivl_vee_src;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	##create the table
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_audit_src.hql ${athena_input_file}
    fnLog "sed  -i \"s/\${env}/${env}/g\" ${athena_input_file}"
	
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}

	create_athena_objects ${athena_input_file}
	echo "drop_create_athena_table succeeded.."
}

function insert_usage_dt()
{
	echo "executing insert_usage_dt.."
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/drop_create_athena_table.txt
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/insert_usage_dt.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "insert_usage_dt;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DML/insert_xfrm_audit_src_usage_dt.hql ${athena_input_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${START_DATE}/${START_DATE}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	echo "insert_usage_dt succeeded.."

}

function insert_uom()
{
	echo "executing insert_uom.."
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/insert_usage_dt.txt
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/insert_uom.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "insert_uom;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DML/insert_xfrm_audit_src_uom.hql ${athena_input_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	

	create_athena_objects ${athena_input_file}	
	echo "insert_uom succeeded.."
	
}

function insert_data_quality()
{
	echo "executing insert_data_quality.."
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/insert_uom.txt
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/insert_data_quality.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "insert_data_quality;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DML/insert_xfrm_audit_src_data_quality.hql ${athena_input_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	

	create_athena_objects ${athena_input_file}	
	echo "insert_data_quality succeeded.."	
	
}

function insert_zero_usage()
{
	echo "executing insert_zero_usage.."
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/insert_data_quality.txt
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/insert_zero_usage.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "insert_zero_usage;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DML/insert_xfrm_audit_src_zero_usage.hql ${athena_input_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	

	create_athena_objects ${athena_input_file}	
	echo "insert_zero_usage succeeded.."
	
}

function insert_interval_count()
{
	echo "executing insert_interval_count.."
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/insert_zero_usage.txt
	echo `date` > $MDMVEE_HIVE_DML/daily_scripts/insert_interval_count.txt
	
	##### ctas tables start ######
	### 1st 
	xfrm_ctas_audit_intvl_cnt_src_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_cnt_src"
	fnLog "aws s3 rm --recursive ${xfrm_ctas_audit_intvl_cnt_src_path}/"	
	if [[ ! -z ${xfrm_ctas_audit_intvl_cnt_src_path} ]];then
		aws s3 rm --recursive ${xfrm_ctas_audit_intvl_cnt_src_path}/
	fi
    fnLog "Clean-Up ctas location ${xfrm_ctas_audit_intvl_cnt_src_path}"
	  ##drop the table
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_ctas_audit_intvl_cnt_src;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	##create the table
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_ctas_audit_intvl_cnt_src.hql ${athena_input_file}
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}
	fnLog "drop_create stg_vee.xfrm_ctas_audit_intvl_cnt_src table succeeded.."
	
	### 2nd
	xfrm_ctas_audit_intvl_96_act_aggr_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_96_act_aggr"
	fnLog "aws s3 rm --recursive ${xfrm_ctas_audit_intvl_96_act_aggr_path}/"	
	if [[ ! -z ${xfrm_ctas_audit_intvl_96_act_aggr_path} ]];then
		aws s3 rm --recursive ${xfrm_ctas_audit_intvl_96_act_aggr_path}/
	fi
    fnLog "Clean-Up ctas location ${xfrm_ctas_audit_intvl_96_act_aggr_path}"
	
	  ##drop the table
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_ctas_audit_intvl_96_act_aggr;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	  ##create the table
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_ctas_audit_intvl_96_act_aggr.hql ${athena_input_file}
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}
	fnLog "drop_create stg_vee.xfrm_ctas_audit_intvl_96_act_aggr table succeeded.."
	
	### 3rd
	xfrm_ctas_audit_intvl_96_est_aggr_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_96_est_aggr"
	fnLog "aws s3 rm --recursive ${xfrm_ctas_audit_intvl_96_est_aggr_path}/"	
	if [[ ! -z ${xfrm_ctas_audit_intvl_96_est_aggr_path} ]];then
		aws s3 rm --recursive ${xfrm_ctas_audit_intvl_96_est_aggr_path}/
	fi
    fnLog "Clean-Up ctas location ${xfrm_ctas_audit_intvl_96_est_aggr_path}"
	
	  ##drop the table
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_ctas_audit_intvl_96_est_aggr;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	  ##create the table
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_ctas_audit_intvl_96_est_aggr.hql ${athena_input_file}
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}
	fnLog "drop_create stg_vee.xfrm_ctas_audit_intvl_96_est_aggr table succeeded.."
	
	### 4th
	xfrm_ctas_audit_intvl_not_96_aggr_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_not_96_aggr"
	fnLog "aws s3 rm --recursive ${xfrm_ctas_audit_intvl_not_96_aggr_path}/"	
	if [[ ! -z ${xfrm_ctas_audit_intvl_not_96_aggr_path} ]];then
		aws s3 rm --recursive ${xfrm_ctas_audit_intvl_not_96_aggr_path}/
	fi
    fnLog "Clean-Up ctas location ${xfrm_ctas_audit_intvl_not_96_aggr_path}"
	
	  ##drop the table
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_ctas_audit_intvl_not_96_aggr;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	  ##create the table
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_ctas_audit_intvl_not_96_aggr.hql ${athena_input_file}
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}
	fnLog "drop_create stg_vee.xfrm_ctas_audit_intvl_not_96_aggr table succeeded.."
	
	### 5th
	xfrm_ctas_audit_intvl_aggr_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_ctas_audit_intvl_aggr"
	fnLog "aws s3 rm --recursive ${xfrm_ctas_audit_intvl_aggr_path}/"	
	if [[ ! -z ${xfrm_ctas_audit_intvl_aggr_path} ]];then
		aws s3 rm --recursive ${xfrm_ctas_audit_intvl_aggr_path}/
	fi
    fnLog "Clean-Up ctas location ${xfrm_ctas_audit_intvl_aggr_path}"
	
	   ##drop the table
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_ctas_audit_intvl_aggr;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	   ##create the table
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_ctas_audit_intvl_aggr.hql ${athena_input_file}
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}
	fnLog "drop_create stg_vee.xfrm_ctas_audit_intvl_aggr table succeeded.."
	
	##### ctas tables end   ######
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "insert_zero_usage;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DML/insert_xfrm_audit_src_interval_count.hql ${athena_input_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	

	create_athena_objects ${athena_input_file}	
	echo "insert_interval_count succeeded.."	
	
}

function main()
{
	initialize_log_file
	initialize_variables
	
	## check if fresh_run or force_start
	
	if [ ! -f $MDMVEE_HIVE_DML/daily_scripts/clean_external_location_dir.txt ] && [ ! -f $MDMVEE_HIVE_DML/daily_scripts/drop_create_athena_table.txt ] && [ ! -f $MDMVEE_HIVE_DML/daily_scripts/insert_usage_dt.txt ] && [ ! -f $MDMVEE_HIVE_DML/daily_scripts/insert_uom.txt ] && [ ! -f $MDMVEE_HIVE_DML/daily_scripts/insert_data_quality.txt ] && [ ! -f $MDMVEE_HIVE_DML/daily_scripts/insert_zero_usage.txt ] && [ ! -f $MDMVEE_HIVE_DML/daily_scripts/insert_interval_count.txt ]
	then
		fnLog "Starting a Fresh Load.."
		clean_external_location_dir
		drop_create_athena_table
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/clean_external_location_dir.txt ] 
	then 
		fnLog "Starting from clean_external_location_dir.."
		clean_external_location_dir
		drop_create_athena_table
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/drop_create_athena_table.txt ] 
	then 
		fnLog "Starting from drop_create_athena_table.."
		drop_create_athena_table
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/insert_usage_dt.txt ] 
	then 
		fnLog "Starting from insert_usage_dt.."
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/insert_uom.txt ] 
	then 
		fnLog "Starting from insert_uom.."
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/insert_data_quality.txt ] 
	then 
		fnLog "Starting from insert_data_quality.."
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/insert_zero_usage.txt ] 
	then 
		fnLog "Starting from insert_zero_usage.."
		insert_zero_usage
		insert_interval_count	
	elif [ -f $MDMVEE_HIVE_DML/daily_scripts/insert_interval_count.txt ] 
	then 
		fnLog "Starting from insert_interval_count.."
		insert_interval_count
	fi
	rm -rf $MDMVEE_HIVE_DML/daily_scripts/insert_interval_count.txt
}

main "${@}"
exit 0
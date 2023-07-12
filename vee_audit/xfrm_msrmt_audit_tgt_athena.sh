##                    AMERICAN ELECTRIC POWER - mdm_intvl_vee audit stg                     ##
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
##  Version #   Name                   Date                   Description                   ##
##                                                                                          ##
##  V.001    Diganta Nayak             12/13/2022             Hive to Athena migration      ##
##------------------------------------------------------------------------------------------##
## This script is to ingest d1_msrmt table incrementally along with other tables as group   ##
## from Oracle MDM database                                                                 ##
## USAGE: sh stg_import_msrmt.sh P[1-10]                                                    ##
##------------------------------------------------------------------------------------------##
#!/bin/bash

. /aep/home/hdpapp/mdm_intvl_vee/scripts/.mdm_intvl_vee_env
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MODULE="xfrm_msrmt_audit_tgt"
script_name="$(basename ${BASH_SOURCE[0]})"
#source "${COMMON_PATH}/scripts/shell_utils.sh" || { echo "ERROR: failed to source /home/hdpapp/common/scripts/shell_utils.sh"; exit 1; }
#MODULE="xfrm_msrmt_audit_tgt_${1}_${2}-$AUTO_JOB_NAME"

PROCESS_DAY=`date +%Y%m%d%H%M%S`
#LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
env=$AWS_ENV

VAR_OPCO=$1
#AUDIT_TYP=$2

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


#### start 

function handle_script_args()
{
	if [ $# -ne 1 ]; then
	    # Wrong number of arguments, exit the script.
	    # usage;
		echo "Wrong number of arguments, exiting the script."
		log_error_exit
	    exit 1
    fi
	
	if [ "$VAR_OPCO" != "pso" ] && [ "$VAR_OPCO" != "oh" ] && [ "$VAR_OPCO" != "tx" ] && [ "$VAR_OPCO" != "im" ] && [ "$VAR_OPCO" != "ap" ] && [ "$VAR_OPCO" != "swp" ];then
	echo "Wrong Input parameter for OPCO. It should be either pso/im/oh/tx/ap/swp"
	exit 1
fi
}
function initialize_log_file()
{
    # Log File initialization
    LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
    ##log_file="${log_dir}/${1}_${common_shell_utils_current_date}.log"    
	find $MDMVEE_LOGS -type f -name ${MODULE}_log_* -mtime +7 -exec rm -f {} \;
	fnLog  "==========  VEE AUDIT SCRIPT BEGIN  =========="
}

function initialize_variables()
{
	echo "Start Target Aggregation `date `"

	#HADOOP_DATA_CONSUME_SCHEMA="usage_vee.reading_ivl_vee_hist"
	HADOOP_DATA_CONSUME_SCHEMA="usage_vee.reading_ivl_vee_${VAR_OPCO}"

	AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
	#START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`

	START_DATE=`date +%Y-%m-%d -d "$NO_OF_DAYS_TO_AUDIT day ago"`

	echo "START_DATE=${START_DATE}"
	echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"

	if [ ! -f $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt ]; then
		echo "Input Date criteria file is missing - $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt  This file is generated in "
		log_error_exit
	fi
	
	INPUT_DATES=`cat $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt|cut -f2`
	echo "${INPUT_DATES}"

	echo "INPUT_DATES:"
	echo "${INPUT_DATES}"|tr "," "\n"|sort
	echo "========================="

	counter=0
	
	#echo ${NO_OF_DAYS_TO_AUDIT}
	
	while [ ${counter} -lt ${NO_OF_DAYS_TO_AUDIT} ]; 
	do
	#	echo $counter
		new_date=`date  -d "${START_DATE} + ${counter} days" '+%Y-%m-%d'`
		INPUT_DATE_LIST="${INPUT_DATE_LIST},'${new_date}'"
	#	echo "${INPUT_DATE_LIST}"
		counter=`expr ${counter} + 1`
	done
	
	echo "INPUT_DATE_LIST"
	echo "${INPUT_DATE_LIST}"|tr "," "\n"|sort
	
	INPUT_DATES=`echo ${INPUT_DATE_LIST}|cut -d "," -f2-`
	
	echo "Using INPUT_DATE_LIST"
	echo ${INPUT_DATES}
	
}

function fnLog()
{
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

function clean_external_location_dir()
{
	echo "executing clean_external_location_dir.."
	touch ${MDMVEE_LOGS}/clean_external_location_dir_${VAR_OPCO}.txt
	hdfs_extrct_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO}"
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

function drop_create_athena_table()
{
	echo "executing drop_create_athena_table.."
	rm -rf ${MDMVEE_LOGS}/clean_external_location_dir_${VAR_OPCO}.txt
	touch ${MDMVEE_LOGS}/drop_create_athena_table_${VAR_OPCO}.txt

	##drop the table if exists
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_${VAR_OPCO}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_audit_reading_ivl_vee_tgt_${VAR_OPCO};" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	##create the table
	cp ${script_dir}/hive_ddl/create_table_xfrm_audit_tgt.hql ${athena_input_file}
    fnLog "sed  -i \"s/\${env}/${env}/g\" ${athena_input_file}"
	
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	sed  -i "s/\${VAR_OPCO}/${VAR_OPCO}/g" ${athena_input_file}
#	sed  -i "s/\${table_name}/${table_name}/g" ${athena_input_file}	
	create_athena_objects ${athena_input_file}
	echo "drop_create_athena_table succeeded.."
}

function insert_usage_dt()
{
	echo "executing insert_usage_dt.."
	rm -rf ${MDMVEE_LOGS}/drop_create_athena_table_${VAR_OPCO}.txt
	touch ${MDMVEE_LOGS}/insert_usage_dt_${VAR_OPCO}.txt

	athena_input_file="${MDMVEE_LOGS}/${MODULE}_${VAR_OPCO}_export_athena.tmp"
	echo "insert_usage_dt;" > ${athena_input_file}
	cp ${script_dir}/hive_dml/insert_xfrm_audit_tgt_usage_dt.hql ${athena_input_file}
	
	sed  -i "s/\${VAR_OPCO}/${VAR_OPCO}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${INPUT_DATES}/${INPUT_DATES}/g" ${athena_input_file}
	sed  -i "s/\${HADOOP_DATA_CONSUME_SCHEMA}/${HADOOP_DATA_CONSUME_SCHEMA}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	echo "insert_usage_dt succeeded.."
}

function insert_uom()
{
	echo "executing insert_uom.."
	rm -rf ${MDMVEE_LOGS}/insert_usage_dt_${VAR_OPCO}.txt
	touch ${MDMVEE_LOGS}/insert_uom_${VAR_OPCO}.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_${VAR_OPCO}_export_athena.tmp"
	echo "insert_uom;" > ${athena_input_file}
	cp ${script_dir}/hive_dml/insert_xfrm_audit_tgt_uom.hql ${athena_input_file}
	
	sed  -i "s/\${VAR_OPCO}/${VAR_OPCO}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${INPUT_DATES}/${INPUT_DATES}/g" ${athena_input_file}
	sed  -i "s/\${HADOOP_DATA_CONSUME_SCHEMA}/${HADOOP_DATA_CONSUME_SCHEMA}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	echo "insert_uom succeeded.."
}

function insert_data_quality()
{
	echo "executing insert_data_quality.."
	rm -rf ${MDMVEE_LOGS}/insert_uom_${VAR_OPCO}.txt
	touch ${MDMVEE_LOGS}/insert_data_quality_${VAR_OPCO}.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_${VAR_OPCO}_export_athena.tmp"
	echo "insert_data_quality;" > ${athena_input_file}
	cp ${script_dir}/hive_dml/insert_xfrm_audit_tgt_data_quality.hql ${athena_input_file}
	
	sed  -i "s/\${VAR_OPCO}/${VAR_OPCO}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${INPUT_DATES}/${INPUT_DATES}/g" ${athena_input_file}
	sed  -i "s/\${HADOOP_DATA_CONSUME_SCHEMA}/${HADOOP_DATA_CONSUME_SCHEMA}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	echo "insert_data_quality succeeded.."
}

function insert_zero_usage()
{
	echo "executing insert_zero_usage.."
	rm -rf ${MDMVEE_LOGS}/insert_data_quality_${VAR_OPCO}.txt
	touch ${MDMVEE_LOGS}/insert_zero_usage_${VAR_OPCO}.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_${VAR_OPCO}_export_athena.tmp"
	echo "insert_zero_usage;" > ${athena_input_file}
	cp ${script_dir}/hive_dml/insert_xfrm_audit_tgt_zero_usage.hql ${athena_input_file}
	
	sed  -i "s/\${VAR_OPCO}/${VAR_OPCO}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${INPUT_DATES}/${INPUT_DATES}/g" ${athena_input_file}
	sed  -i "s/\${HADOOP_DATA_CONSUME_SCHEMA}/${HADOOP_DATA_CONSUME_SCHEMA}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	echo "insert_zero_usage succeeded.."
}

function insert_interval_count()
{

	echo "executing insert_interval_count.."
	rm -rf ${MDMVEE_LOGS}/insert_zero_usage_${VAR_OPCO}.txt
	touch ${MDMVEE_LOGS}/insert_interval_count_${VAR_OPCO}.txt
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_${VAR_OPCO}_export_athena.tmp"
	echo "insert_interval_count;" > ${athena_input_file}
	cp ${script_dir}/hive_dml/insert_xfrm_audit_tgt_interval_count.hql ${athena_input_file}
	
	sed  -i "s/\${VAR_OPCO}/${VAR_OPCO}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${INPUT_DATES}/${INPUT_DATES}/g" ${athena_input_file}
	sed  -i "s/\${HADOOP_DATA_CONSUME_SCHEMA}/${HADOOP_DATA_CONSUME_SCHEMA}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	echo "insert_interval_count succeeded.."
}


function main()
{
	handle_script_args "${@}"
	initialize_log_file
	initialize_variables
	
	## check if fresh_run or force_start
	
	if [ ! -f ${MDMVEE_LOGS}/clean_external_location_dir_${VAR_OPCO}.txt ] && [ ! -f ${MDMVEE_LOGS}/drop_create_athena_table_${VAR_OPCO}.txt ] && [ ! -f ${MDMVEE_LOGS}/insert_usage_dt_${VAR_OPCO}.txt ] && [ ! -f ${MDMVEE_LOGS}/insert_uom_${VAR_OPCO}.txt ] && [ ! -f ${MDMVEE_LOGS}/insert_data_quality_${VAR_OPCO}.txt ] && [ ! -f ${MDMVEE_LOGS}/insert_zero_usage_${VAR_OPCO}.txt ] && [ ! -f ${MDMVEE_LOGS}/insert_interval_count_${VAR_OPCO}.txt ]
	then
		clean_external_location_dir
		drop_create_athena_table
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f ${MDMVEE_LOGS}/clean_external_location_dir_${VAR_OPCO}.txt ] 
	then 
		clean_external_location_dir
		drop_create_athena_table
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f ${MDMVEE_LOGS}/drop_create_athena_table_${VAR_OPCO}.txt ] 
	then 
		drop_create_athena_table
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f ${MDMVEE_LOGS}/insert_usage_dt_${VAR_OPCO}.txt ] 
	then 
		insert_usage_dt
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f ${MDMVEE_LOGS}/insert_uom_${VAR_OPCO}.txt ] 
	then 
		insert_uom
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f ${MDMVEE_LOGS}/insert_data_quality_${VAR_OPCO}.txt ] 
	then 
		insert_data_quality
		insert_zero_usage
		insert_interval_count
	elif [ -f ${MDMVEE_LOGS}/insert_zero_usage_${VAR_OPCO}.txt ] 
	then 
		insert_zero_usage
		insert_interval_count	
	elif [ -f ${MDMVEE_LOGS}/insert_interval_count_${VAR_OPCO}.txt ] 
	then 
		insert_interval_count
	fi
	rm -rf ${MDMVEE_LOGS}/insert_interval_count_${VAR_OPCO}.txt
}

main "${@}"
exit 0
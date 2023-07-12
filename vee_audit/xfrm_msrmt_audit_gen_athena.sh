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
##  V.001    Ramesh Sethuraj                            03/22/2019     Initial 				##
##  V.002    Diganta Nayak                              12/30/2022   Athena Conversion		##
##                                                                                          ##
##------------------------------------------------------------------------------------------##
## This script is to ingest d1_msrmt table incrementally along with other tables as group   ##
## from Oracle MDM database                                                                 ##
## USAGE: sh stg_import_msrmt.sh P[1-10]                                                    ##
##------------------------------------------------------------------------------------------##
#!/bin/bash

. /aep/home/hdpapp/mdm_intvl_vee/scripts/.mdm_intvl_vee_env
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MODULE="xfrm_msrmt_audit_gen_dts-$AUTO_JOB_NAME"
script_name="$(basename ${BASH_SOURCE[0]})"
#PROCESS_DAY=`date +%Y%m%d%H`
PROCESS_DAY=`date +%Y%m%d%H%M%S`
#LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
env=$AWS_ENV
#echo $LOGFILE

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
	fnLog  "==========  VEE AUDIT audit_gen SCRIPT BEGIN  =========="
}

function fnLog()
{
	echo `date +"%Y-%m-%d %k:%M:%S"` $1 
	echo `date +"%Y-%m-%d %k:%M:%S"` $1 >> ${LOGFILE}
}

function initialize_variables(){
	echo "Start Audit Date generation `date `"

	#AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt`
	AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
	START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`
	HDP_LAST_RUN_DT=`cat $MDMVEE_PARMS/last_run_dt.txt`

	echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"
	echo "START_DATE=${START_DATE}"


	if [ -z "$AUDIT_RUN_DT" ] || [ -z "$START_DATE" ]; then
		fnLog "ERROR:AUDIT_RUN_DT Or START_DATE Date is empty, check $MDMVEE_PARMS/audit_run_dt.txt"
		log_error_exit
	fi

}

## Logic to Cleanup the Old Log Files 
## Remove older than 7 days of log files
#find $MDMVEE_LOGS -type f -name ${MODULE}_log_* -mtime +7 -exec rm -f {} \;

#exec > $LOGFILE 2>&1

#echo "Start Audit Date generation `date `"
#
##AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt`
#AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
#START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`
#HDP_LAST_RUN_DT=`cat $MDMVEE_PARMS/last_run_dt.txt`
#
#echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"
#echo "START_DATE=${START_DATE}"


#if [ -z "$AUDIT_RUN_DT" ] || [ -z "$START_DATE" ]; then
#	fnLog "ERROR:AUDIT_RUN_DT Or START_DATE Date is empty, check $MDMVEE_PARMS/audit_run_dt.txt"
#	log_error_exit
#fi
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

gen_audit_dates()
{
	##### ctas tables start ######
	## remove s3 data for stg_vee.xfrm_ctas_audit_dates
	xfrm_ctas_audit_dates_path="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/xfrm_ctas_audit_dates"
	echo "Clean-Up ctas location ${xfrm_ctas_audit_dates_path}"
	fnLog "aws s3 rm --recursive ${xfrm_ctas_audit_dates_path}/"	
	if [[ ! -z ${xfrm_ctas_audit_dates_path} ]];then
		aws s3 rm --recursive ${xfrm_ctas_audit_dates_path}/
	fi
    fnLog "Clean-Up ctas location ${xfrm_ctas_audit_dates_path}"
	##drop the table stg_vee.xfrm_ctas_audit_dates
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.xfrm_ctas_audit_dates;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	##create the table stg_vee.xfrm_ctas_audit_dates
	cp ${MDMVEE_HIVE_DDL}/create_table_xfrm_ctas_audit_dates.hql ${athena_input_file}
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}
	
	## Generate audit_dates_${AUDIT_RUN_DT}.txt
	mkdir ${MDMVEE_DATA}/data_from_s3
	chmod 777 ${MDMVEE_DATA}/data_from_s3
	aws s3 cp --recursive ${xfrm_ctas_audit_dates_path}/ ${MDMVEE_DATA}/data_from_s3/
	#mv data_from_s3/* ${MDMVEE_DATA}/dt.txt
	cat ${MDMVEE_DATA}/data_from_s3/* > ${MDMVEE_DATA}/dt.txt
	echo "cat ${MDMVEE_DATA}/dt.txt "
	cat ${MDMVEE_DATA}/dt.txt
	sed -i '1s/^/test	/' ${MDMVEE_DATA}/dt.txt  ## added 'test' to the audit_dates_${AUDIT_RUN_DT}.txt
	cp ${MDMVEE_DATA}/dt.txt ${MDMVEE_PARMS}/audit_dates_${AUDIT_RUN_DT}.txt
	rm ${MDMVEE_DATA}/dt.txt
	rm -r ${MDMVEE_DATA}/data_from_s3
	fnLog "drop_create stg_vee.xfrm_ctas_audit_dates table succeeded.."
	fnLog "${MDMVEE_PARMS}/audit_dates_${AUDIT_RUN_DT}.txt file creation succeeded.."
}

function main()
{
	initialize_log_file
	initialize_variables
	
	if [ ! -f $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt ]; then
		echo "Generating data file"
		gen_audit_dates
	else
		echo "File Already exist - $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt"
	fi
	
	INPUT_DATES=`cat $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt|cut -f2`

	echo "INPUT_DATES:"
	echo ${INPUT_DATES}

	if [ -z "$INPUT_DATES" ]; then
		fnLog "ERROR:INPUT_DATES is empty, check xfrm_msrmt_audit_gen_dts.sh."
		rm -f $MDMVEE_PARMS/audit_dates_${AUDIT_RUN_DT}.txt
		log_error_exit
	fi
	
	echo "Creating current view audit_reading_ivl_vee_cur_vw"
	echo "
	create or replace view stg_vee.audit_reading_ivl_vee_cur_vw as 
	Select   
	aep_opco, 
	aep_usage_dt,
	name_register,
	src_intvl_cnt,
	tgt_intvl_cnt,
	src_intvl_usg,
	tgt_intvl_usg,
	src_unq_meter_count,
	tgt_unq_meter_count,
	diff_intvl_cnt,
	per_diff_intvl_cnt,
	diff_unq_meter_count,
	per_diff_unq_mtr,
	diff_intvl_usg,
	per_diff_intvl_usg,
	run_dt,
	aggr_type,
	hdp_insert_dttm
	from 
	usage_vee.audit_reading_ivl_vee
	where 
	run_dt = '${AUDIT_RUN_DT}'
	order by 
	aggr_type,
	aep_opco, 
	aep_usage_dt,
	name_register
	;"
	
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "create or replace view stg_vee.audit_reading_ivl_vee_cur_vw as 
	Select   
	aep_opco, 
	aep_usage_dt,
	name_register,
	src_intvl_cnt,
	tgt_intvl_cnt,
	src_intvl_usg,
	tgt_intvl_usg,
	src_unq_meter_count,
	tgt_unq_meter_count,
	diff_intvl_cnt,
	per_diff_intvl_cnt,
	diff_unq_meter_count,
	per_diff_unq_mtr,
	diff_intvl_usg,
	per_diff_intvl_usg,
	run_dt,
	aggr_type ,
	hdp_insert_dttm
	from 
	usage_vee.audit_reading_ivl_vee
	where 
	run_dt = '${AUDIT_RUN_DT}'
	order by 
	aggr_type,
	aep_opco, 
	aep_usage_dt,
	name_register
	;" > ${athena_input_file}

	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	

	create_athena_objects ${athena_input_file}	
	echo "stg_vee.audit_reading_ivl_vee_cur_vw view creation succeeded.."
	fnLog "stg_vee.audit_reading_ivl_vee_cur_vw view creation succeeded.."
}


main "${@}"
exit 0

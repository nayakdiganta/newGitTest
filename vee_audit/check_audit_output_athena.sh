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
MODULE="check_audit_output-$AUTO_JOB_NAME"
#PROCESS_DAY=`date +%Y%m%d%H`
PROCESS_DAY=`date +%Y%m%d%H%M%S`
#LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
env=$AWS_ENV
#AUDIT_ALERT_MDMVEE_EMAIL="rsethuraj@aep.com"

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
	echo $LOGFILE
    ## Logic to Cleanup the Old Log Files 
	## Remove older than 7 days of log files
	find $MDMVEE_LOGS -type f -name ${MODULE}_log* -mtime +7 -exec rm -f {} \;

	find $MDMVEE_HIVE_DML/daily_scripts/ -type f -name "check_audit_output_*" -mtime +10 -exec rm -f {} \;

	find $MDMVEE_HIVE_DML/daily_scripts/ -type f -name "check_duplicate_rec_*" -mtime +10 -exec rm -f {} \;

	find $MDMVEE_HIVE_DML/daily_scripts/ -type f -name "audit_dates_*" -mtime +10 -exec rm -f {} \;

	find $MDMVEE_PARMS/ -type f -name "audit_dates_*" -mtime +10 -exec rm -f {} \;
	fnLog  "==========  VEE AUDIT check_audit_output_athena SCRIPT BEGIN  =========="
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

function clean_external_location_dir()
{
	echo "executing clean_external_location_dir.."
	
	hdfs_extrct_path=$1  ##"s3://aep-datalake-work-${env}/raw/intervals/vee/audit/xfrm_audit_reading_ivl_vee_src"
	echo 's3 location to place the exported data from athena'
	echo $hdfs_extrct_path
    echo "start clean_external_location_dir(hdfs_extrct_path): $hdfs_extrct_path"
	fnLog "aws s3 rm --recursive ${hdfs_extrct_path}/"
	
	if [[ ! -z ${hdfs_extrct_path} ]];then
		aws s3 rm --recursive ${hdfs_extrct_path}/
	fi
    fnLog "Clean-Up external_location_dir ${hdfs_extrct_path}"
	echo "clean_external_location_dir succeeded.."
}

function initialize_variables()
{
	#exec > $LOGFILE 2>&1

	echo "Remvoe the last audit output trigger files to avoid reprocessing"

	rm -f $MDMVEE_PARMS/vee_reprocess_*.txt

	echo "Get audit alret percentage"

	if [ ! -f $MDMVEE_PARMS/audit_alert_percent.txt ];then
		echo "$MDMVEE_PARMS/audit_alert_percent.txt Missing. Creating one with 2"
		echo "2" > $MDMVEE_PARMS/audit_alert_percent.txt
	fi

	audit_email_info_file="${MDMVEE_PARMS}/audit_email_text_${AWS_ENV}.txt"

	if [ ! -f ${audit_email_info_file} ];then
		echo "Audit Email information file is missing - ${audit_email_info_file}"
		exit 1
	fi

	PERC_DIFF_TO_ALERT=`cat $MDMVEE_PARMS/audit_alert_percent.txt`

	echo "PERC_DIFF_TO_ALERT=$PERC_DIFF_TO_ALERT"

	echo "Start Audit Date generation `date `"
	#audit_run_dt.txt is created in stg_import_msrmt_audit.sh
	AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
	START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`

	echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"
	echo "START_DATE=${START_DATE}"
}

function start_process()
{
	echo "executing start_process.."	
	fnLog "executing start_process.."
	
	echo "Generating audit output data file"
	fnLog "Generating audit output data file by creating ctas table"
	## remove external location of the table
	create_check_audit_output="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/ctas_check_audit_output"
	clean_external_location_dir	${create_check_audit_output}
	
	## drop table ctas_check_audit_output
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.ctas_check_audit_output;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	## create table ctas_check_audit_output
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "create_check_audit_output;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DDL/create_ctas_check_audit_output.hql ${athena_input_file}
	
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${START_DATE}/${START_DATE}/g" ${athena_input_file}
	sed  -i "s/\${PERC_DIFF_TO_ALERT}/${PERC_DIFF_TO_ALERT}/g" ${athena_input_file}
	create_athena_objects ${athena_input_file}	
	
	## create check_audit_output_${AUDIT_RUN_DT} file
	mkdir ${MDMVEE_DATA}/get_ctas_check_audit_output_from_s3
	chmod 777 ${MDMVEE_DATA}/get_ctas_check_audit_output_from_s3
	aws s3 cp --recursive ${create_check_audit_output}/ ${MDMVEE_DATA}/get_ctas_check_audit_output_from_s3/
	#mv ${MDMVEE_DATA}/get_ctas_check_audit_output_from_s3/* ${MDMVEE_DATA}/check_audit_output.txt
	cat ${MDMVEE_DATA}/get_ctas_check_audit_output_from_s3/* > ${MDMVEE_DATA}/check_audit_output.txt
	####### add header
	echo "adding header header_audit_output"
	header_audit_output="aep_opco,aep_usage_dt,src_intvl_cnt,tgt_intvl_cnt,src_intvl_usg,tgt_intvl_usg,src_unq_meter_count,tgt_unq_meter_count,diff_intvl_cnt,diff_unq_meter_count,diff_intvl_usg,max_tgt_intvl_cnt,per_diff_intvl_cnt,per_diff_unq_mtr,per_diff_intvl_usg,per_max_tgt_intvl_cnt,run_dt,aggr_type,issue_type"
	sed -i 1i\ ${header_audit_output} ${MDMVEE_DATA}/check_audit_output.txt
	#######header added
	echo "cat ${MDMVEE_DATA}/check_audit_output.txt"
	cat ${MDMVEE_DATA}/check_audit_output.txt
	cp ${MDMVEE_DATA}/check_audit_output.txt ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt
	rm ${MDMVEE_DATA}/check_audit_output.txt
	rm -r ${MDMVEE_DATA}/get_ctas_check_audit_output_from_s3

	echo "${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt file generation succeeded.."
	fnLog "${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt file generation succeeded.."
	
	
	cat ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt

	OUT_PUT_RECORDS=`wc -l ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|cut -d" " -f1`

	echo "OUT_PUT_RECORDS = $OUT_PUT_RECORDS"
	
	if [ $OUT_PUT_RECORDS -gt 1 ];then

		echo "There is a VEE data mismatch, sending email to the team."

		email_group=`grep "AUDIT_ALERT_MISMATCH" ${audit_email_info_file}|cut -d "|" -f5`

		mailx -s "MDM VEE To Hadoop (AWS) AUDIT - MISMATCH > ${PERC_DIFF_TO_ALERT}% For Run Date ${AUDIT_RUN_DT}  - `date`" -a ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt -rVEE_AUDIT_MISMATCH ${email_group} <<!
MDM VEE Completeness audit result shows there is a difference of more than ${PERC_DIFF_TO_ALERT}% for either record count or meter count or usage between MDM to Hadoop (AWS) Or Source interval count is less than 5% of maximum value of last 14 days.
!

		echo "Check whether it is Processing issue or not"
		tgt_issue_records=`grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|wc -l`
		if [ $tgt_issue_records -gt 0 ];then
			echo "Create trigger file for reprocessing for TARGET ISSUES"
			# grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt| tail -n +2|cut  -f1,2|sort -u > $MDMVEE_PARMS/vee_reprocess_dates_trigger.txt

			# grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|tail -n +2|cut  -f1|sort -u > /tmp/vee_reprocess_opcos.txt

			# grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|tail -n +2|cut  -f2|sort -u > $MDMVEE_PARMS/vee_reprocess_dates.txt	
			grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|cut  -f1,2|sort -u > $MDMVEE_PARMS/vee_reprocess_dates_trigger.txt

			grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|cut  -f1|sort -u > /tmp/vee_reprocess_opcos.txt

			grep "TARGET ISSUE"  ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_${AUDIT_RUN_DT}.txt|cut  -f2|sort -u > $MDMVEE_PARMS/vee_reprocess_dates.txt	
		else
			echo "It is not Processing Issue, data missing in source No need for reprocess"
		fi
	
		first_line=0
		
		while read line
		do 
			if [ ${first_line} -eq 0 ]; then
				REPROC_OPCO="'${line}'"
				first_line=1
			else 
				REPROC_OPCO="${REPROC_OPCO} , '${line}'"
			fi
		done < /tmp/vee_reprocess_opcos.txt
		echo "REPROC_OPCO=${REPROC_OPCO}"
		if ! [ -z "$REPROC_OPCO" ] ; then
			echo ${REPROC_OPCO} > $MDMVEE_PARMS/vee_reprocess_opcos.txt
		fi
	else
		echo "There is no data mismatch between MDM and Hadoop, sending email to the team."
	fi

	echo "check_audit_output done - `date "+%Y-%m-%d@%T"`."
	
	#Truncate older than x partitions
	ARCHIEVE_DATE=`date +%Y-%m-%d -d "30 day ago"`
	
	fnLog "Truncate older than ${ARCHIEVE_DATE} partitions by creating ctas table as athena doe not support range partitions"		
	## remove external location of the table
	create_ctas_min_run_dt_audit_reading_ivl_vee="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/ctas_min_run_dt_audit_reading_ivl_vee"
	clean_external_location_dir	${create_ctas_min_run_dt_audit_reading_ivl_vee}
	
	## drop table ctas_min_run_dt_audit_reading_ivl_vee
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.ctas_min_run_dt_audit_reading_ivl_vee;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	## create table ctas_min_run_dt_audit_reading_ivl_vee
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "create_ctas_min_run_dt_audit_reading_ivl_vee;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DDL/create_ctas_min_run_dt_audit_reading_ivl_vee.hql ${athena_input_file}
	
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}	
	create_athena_objects ${athena_input_file}
	
	## get min_run_dt value
	mkdir ${MDMVEE_DATA}/get_min_run_dt_from_s3
	chmod 777 ${MDMVEE_DATA}/get_min_run_dt_from_s3
	aws s3 cp --recursive ${create_ctas_min_run_dt_audit_reading_ivl_vee}/ ${MDMVEE_DATA}/get_min_run_dt_from_s3/
	#mv ${MDMVEE_DATA}/get_min_run_dt_from_s3/* ${MDMVEE_DATA}/min_run_dt_from_s3.txt
	cat ${MDMVEE_DATA}/get_min_run_dt_from_s3/* > ${MDMVEE_DATA}/min_run_dt_from_s3.txt
	echo "cat ${MDMVEE_DATA}/min_run_dt_from_s3.txt"
	cat ${MDMVEE_DATA}/min_run_dt_from_s3.txt	
	min_run_dt=`cat ${MDMVEE_DATA}/min_run_dt_from_s3.txt`
	echo "min_run_dt: $min_run_dt"
	rm ${MDMVEE_DATA}/min_run_dt_from_s3.txt
	rm -r ${MDMVEE_DATA}/get_min_run_dt_from_s3
	##################
	

	start=`echo $min_run_dt`
	end=`echo $ARCHIEVE_DATE`
	#echo "1st start: ${start}"
	#echo "1st end: ${end}"
	start=$(date -d $start +%Y%m%d)
	end=$(date -d $end +%Y%m%d)
	#echo "2nd start: ${start}"
	#echo "2nd end: ${end}"
	
	while [[ $start -lt $end ]]
	do
		run_dt=`echo $(date -d $start +%Y-%m-%d)`
		echo "run_dt: $run_dt"
		athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
		echo "ALTER TABLE usage_vee.audit_reading_ivl_vee_parquet DROP if exists PARTITION (run_dt='"$run_dt"');"
		echo "ALTER TABLE usage_vee.audit_reading_ivl_vee_parquet DROP if exists PARTITION (run_dt='"$run_dt"');" > ${athena_input_file}
		create_athena_objects ${athena_input_file}
		
		start=$(date -d"$start + 1 day" +"%Y%m%d")
	done
	
	fnLog "Truncate older partitions done"
	#ARCHIEVE_DATE="2020-01-01"

	#AUDIT_ALERT_MDMVEE_EMAIL="rsethuraj@aep"
		
	echo "Checking Duplicate records in reading_ivl_vee" 
	fnLog "Checking Duplicate records in reading_ivl_vee by creating ${MDMVEE_HIVE_DML}/daily_scripts/check_duplicate_rec_${AUDIT_RUN_DT}.txt  through ctas table"
	###################
	## remove external location of the table
	create_ctas_check_duplicate_rec="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/ctas_check_duplicate_rec"
	clean_external_location_dir	${create_ctas_check_duplicate_rec}
	
	## drop table ctas_check_duplicate_rec
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.ctas_check_duplicate_rec;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	## create table ctas_check_duplicate_rec
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "create_ctas_check_duplicate_rec;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DDL/create_ctas_check_duplicate_rec.hql ${athena_input_file}
	
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	sed  -i "s/\${ARCHIEVE_DATE}/${ARCHIEVE_DATE}/g" ${athena_input_file}	
	create_athena_objects ${athena_input_file}
	
	## get duplicate_rec_from_s3 value
	mkdir ${MDMVEE_DATA}/get_duplicate_rec_from_s3
	chmod 777 ${MDMVEE_DATA}/get_duplicate_rec_from_s3
	aws s3 cp --recursive ${create_ctas_check_duplicate_rec}/ ${MDMVEE_DATA}/get_duplicate_rec_from_s3/
	#mv ${MDMVEE_DATA}/get_duplicate_rec_from_s3/* ${MDMVEE_DATA}/duplicate_rec_from_s3.txt
	cat ${MDMVEE_DATA}/get_duplicate_rec_from_s3/* > ${MDMVEE_DATA}/duplicate_rec_from_s3.txt
	####### add header
	echo "adding header header_duplicate_rec"
	header_duplicate_rec="rowkey, _c1"
	sed -i 1i\ ${header_duplicate_rec} ${MDMVEE_DATA}/duplicate_rec_from_s3.txt
	#######header added
	echo "cat ${MDMVEE_DATA}/duplicate_rec_from_s3.txt"
	cat ${MDMVEE_DATA}/duplicate_rec_from_s3.txt
	
	cp ${MDMVEE_DATA}/duplicate_rec_from_s3.txt ${MDMVEE_HIVE_DML}/daily_scripts/check_duplicate_rec_${AUDIT_RUN_DT}.txt
	rm ${MDMVEE_DATA}/duplicate_rec_from_s3.txt
	rm -r ${MDMVEE_DATA}/get_duplicate_rec_from_s3
	
	fnLog "${MDMVEE_HIVE_DML}/daily_scripts/check_duplicate_rec_${AUDIT_RUN_DT}.txt creation succeeded."
	###################

	OUT_PUT_RECORDS_dup=`wc -l ${MDMVEE_HIVE_DML}/daily_scripts/check_duplicate_rec_${AUDIT_RUN_DT}.txt|cut -d" " -f1`

	echo "OUT_PUT_RECORDS_dup = $OUT_PUT_RECORDS_dup"
	
	if [ ${OUT_PUT_RECORDS_dup} -gt 1 ];then

		echo "Sending email for duplicate records" 
		email_group=`grep "AUDIT_ALERT_DUPLICATE" ${audit_email_info_file}|cut -d "|" -f5`

		mailx -s "MDM VEE To Hadoop(AWS) Duplicate Record in VEE - `date`" -a ${MDMVEE_HIVE_DML}/daily_scripts/check_duplicate_rec_${AUDIT_RUN_DT}.txt -rVEE_DUPLICATE ${email_group} <<!
Duplicate Records in VEE Reading_ivl_vee table in Prod.
!
	else
		echo "No Duplicates in VEE for the last one month"
	fi
	
	echo "Generating data quality audit output data file for last 7 days"
	fnLog "Generating data quality audit output data file for last 7 days through ctas table"
	START_DATE=`date --date "-8 days" +'%Y-%m-%d'`

	###################
	## remove external location of the table
	create_ctas_check_audit_output_qlty="${HADOOP_DATA_S3_WORK_SRC_STG}/raw/intervals/vee/audit/ctas_check_audit_output_qlty"
	clean_external_location_dir	${create_ctas_check_audit_output_qlty}
	
	## drop table ctas_check_audit_output_qlty
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
    echo "DROP TABLE IF EXISTS stg_vee.ctas_check_audit_output_qlty;" > ${athena_input_file}
    create_athena_objects ${athena_input_file}
	
	## create table ctas_check_audit_output_qlty
	athena_input_file="${MDMVEE_LOGS}/${MODULE}_export_athena.tmp"
	echo "create_ctas_check_audit_output_qlty;" > ${athena_input_file}
	cp $MDMVEE_HIVE_DDL/create_ctas_check_audit_output_qlty.hql ${athena_input_file}
	
	sed  -i "s/\${env}/${env}/g" ${athena_input_file}
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${athena_input_file}	
	sed  -i "s/\${PERC_DIFF_TO_ALERT}/${PERC_DIFF_TO_ALERT}/g" ${athena_input_file}
	sed  -i "s/\${START_DATE}/${START_DATE}/g" ${athena_input_file}	

	create_athena_objects ${athena_input_file}
	
	## create check_audit_output_qlty_${AUDIT_RUN_DT}.txt file
	mkdir ${MDMVEE_DATA}/get_audit_output_qlty_from_s3
	chmod 777 ${MDMVEE_DATA}/get_audit_output_qlty_from_s3
	aws s3 cp --recursive ${create_ctas_check_audit_output_qlty}/ ${MDMVEE_DATA}/get_audit_output_qlty_from_s3/
	#mv ${MDMVEE_DATA}/get_audit_output_qlty_from_s3/* ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt
	cat ${MDMVEE_DATA}/get_audit_output_qlty_from_s3/* > ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt
	####### add header
	echo "adding header header_audit_output_qlty"
	header_audit_output_qlty="aep_opco,aep_usage_dt,src_regular_cnt,tgt_regular_cnt,diff_reg_intvl_cnt,src_estimate,tgt_estimate,diff_est_intvl_cnt,src_total_cnt,tgt_total_cnt,diff_tot_intvl_cnt,src_reg_cnt_per,src_est_cnt_per,tgt_reg_cnt_per,tgt_est_cnt_per,run_dt,aggr_type,issue_type"
	sed -i 1i\ ${header_audit_output_qlty} ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt
	#######header added
	echo "cat ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt"
	cat ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt
	
	cp ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_qlty_${AUDIT_RUN_DT}.txt
	rm ${MDMVEE_DATA}/audit_output_qlty_from_s3.txt
	rm -r ${MDMVEE_DATA}/get_audit_output_qlty_from_s3
	fnLog "Generating data quality audit output data file ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_qlty_${AUDIT_RUN_DT}.txt succeeded."
	###################

	OUT_PUT_RECORDS_qlty=`wc -l ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_qlty_${AUDIT_RUN_DT}.txt|cut -d" " -f1`

	echo "OUT_PUT_RECORDS_qlty = $OUT_PUT_RECORDS_qlty"

	if [ $OUT_PUT_RECORDS_qlty -gt 1 ];then

		echo "There is a VEE data mismatch, sending email to the team."

		email_group=`grep "AUDIT_ALERT_QUALITY" ${audit_email_info_file}|cut -d "|" -f5`
		echo "email_group: ${email_group}"
		mailx -s "VEE DATA QUALITY AUDIT(AWS) - NON REGULAR READ COUNT > ${PERC_DIFF_TO_ALERT}% For Run Date ${AUDIT_RUN_DT}  - `date`" -a ${MDMVEE_HIVE_DML}/daily_scripts/check_audit_output_qlty_${AUDIT_RUN_DT}.txt -rVEE_AUDIT_MISMATCH ${AUDIT_ALERT_MDMVEE_EMAIL} <<!
MDM VEE Completeness audit result shows that there is more than ${PERC_DIFF_TO_ALERT}% NON REGULAR READ count.
!
	else
		echo "No Issue with VEE Data Quality of Records"
	fi
	
	
	echo "start_process completed.."
	fnLog "start_process completed.."
}

function main()
{
	initialize_log_file
	initialize_variables
	start_process
}


main "${@}"
exit 0

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
##  V.001    Ramesh Sethuraj                            03/22/2019     Initial Rel            ##
##                                                                                          ##
##------------------------------------------------------------------------------------------##
## This script is to ingest d1_msrmt table incrementally along with other tables as group   ##
## from Oracle MDM database                                                                 ##
## USAGE: sh stg_import_msrmt.sh P[1-10]                                                    ##
##------------------------------------------------------------------------------------------##
#!/bin/bash

. /aep/home/hdpapp/mdm_intvl_vee/scripts/.mdm_intvl_vee_env
module="audit_reading_ivl_vee-$AUTO_JOB_NAME"
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MODULE="audit_reading_ivl_vee-$AUTO_JOB_NAME"
script_name="$(basename ${BASH_SOURCE[0]})"
source "${script_dir}/.mdm_intvl_vee_env" || { echo "ERROR: failed to source ${script_dir}/.mdm_intvl_vee_env"; exit 1; }
source "${COMMON_PATH}/scripts/shell_utils.sh" || { echo "ERROR: failed to source ${COMMON_PATH}/shell_utils.sh"; exit 1; } 

#PROCESS_DAY=`date +%Y%m%d%H`
PROCESS_DAY=`date +%Y%m%d%H%M%S`
#LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt

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


## Logic to Cleanup the Old Log Files 
## Remove older than 7 days of log files
#find $MDMVEE_LOGS -type f -name ${MODULE}_log_* -mtime +7 -exec rm -f {} \;

#exec > $LOGFILE 2>&1

#echo "Start audit_reading_ivl_vee `date `"

#AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
#START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`


#echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"
#echo "START_DATE=${START_DATE}"

function initialize_log_file(){
    # Log File initialization
    LOGFILE=${MDMVEE_LOGS}/${MODULE}_log_${PROCESS_DAY}.txt
    ## Logic to Cleanup the Old Log Files 
	## Remove older than 7 days of log files
	find $MDMVEE_LOGS -type f -name ${MODULE}_log_* -mtime +7 -exec rm -f {} \;
	log INFO  "==========  VEE AUDIT SRC SCRIPT BEGIN  =========="
}

function initialize_variables(){
	echo "Start audit_reading_ivl_vee `date `"

	AUDIT_RUN_DT=`cat $MDMVEE_PARMS/audit_run_dt.txt|head -1|cut -d"=" -f2`
	START_DATE=`cat $MDMVEE_PARMS/audit_run_dt.txt|tail -1|cut -d"=" -f2`


	echo "AUDIT_RUN_DT=${AUDIT_RUN_DT}"
	echo "START_DATE=${START_DATE}"
	log INFO  "==========  AUDIT_RUN_DT=${AUDIT_RUN_DT}  =========="
	log INFO  "==========  START_DATE=${START_DATE}  =========="

}

function insert_non_interval()
{
	log INFO "================ insert_non_interval through emrsl =================="
	echo `date` > ${MDMVEE_LOGS}/emr_serverless_ip_file_${PROCESS_DAY}.dml
	
	emr_serverless_ip_file="${MDMVEE_LOGS}/emr_serverless_ip_file_${PROCESS_DAY}.dml"

	cp $MDMVEE_HIVE_DML/insert_audit_reading_ivl_vee_non_interval.hql ${emr_serverless_ip_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${emr_serverless_ip_file}	
	sed  -i "s/\${START_DATE}/${START_DATE}/g" ${emr_serverless_ip_file}
	
	log INFO "start_job_run -f \"${emr_serverless_ip_file}\" --hivevar AUDIT_RUN_DT=\"${AUDIT_RUN_DT}\"  --hivevar START_DATE=\"${START_DATE}\""
		
    start_job_run -f "${emr_serverless_ip_file}" --hivevar AUDIT_RUN_DT="${AUDIT_RUN_DT}" --hivevar START_DATE="${START_DATE}"
		
	if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
		echo "ERROR: Error while generating audit_reading_ivl_vee through insert_non_interval - `date "+%Y-%m-%d@%T"`"
		log_error_exit
	fi

	#echo "beehive $HADOOP_BATCH_HIVE -f $MDMVEE_HIVE_DML/insert_audit_reading_ivl_vee_non_interval.hql -hivevar AUDIT_RUN_DT=$AUDIT_RUN_DT  -hivevar START_DATE=$START_DATE"

	#beehive $HADOOP_BATCH_HIVE -f $MDMVEE_HIVE_DML/insert_audit_reading_ivl_vee_non_interval.hql -hivevar AUDIT_RUN_DT=$AUDIT_RUN_DT -hivevar START_DATE=$START_DATE

	#ret=$?
	#if [ $ret -ne 0 ]; then
	#  echo "ERROR: Error while generating audit_reading_ivl_vee - `date "+%Y-%m-%d@%T"`"
	#  log_error_exit
	#fi
	
}

function insert_interval()
{
	log INFO "================ insert_interval through emrsl =================="
	echo `date` > ${MDMVEE_LOGS}/emr_serverless_ip_file_${PROCESS_DAY}.dml
	
	emr_serverless_ip_file="${MDMVEE_LOGS}/emr_serverless_ip_file_${PROCESS_DAY}.dml"

	cp $MDMVEE_HIVE_DML/insert_audit_reading_ivl_vee_interval.hql ${emr_serverless_ip_file}
	
	sed  -i "s/\${AUDIT_RUN_DT}/${AUDIT_RUN_DT}/g" ${emr_serverless_ip_file}	
	sed  -i "s/\${START_DATE}/${START_DATE}/g" ${emr_serverless_ip_file}
	
	log INFO "start_job_run -f \"${emr_serverless_ip_file}\" --hivevar AUDIT_RUN_DT=\"${AUDIT_RUN_DT}\"  --hivevar START_DATE=\"${START_DATE}\""
		
    start_job_run -f "${emr_serverless_ip_file}" --hivevar AUDIT_RUN_DT="${AUDIT_RUN_DT}" --hivevar START_DATE="${START_DATE}"
	
	if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
		echo "ERROR: Error while generating audit_reading_ivl_vee through insert_interval - `date "+%Y-%m-%d@%T"`"
		log_error_exit
	fi
	
	
	#echo "beehive $HADOOP_BATCH_HIVE -f $MDMVEE_HIVE_DML/insert_audit_reading_ivl_vee_interval.hql -hivevar AUDIT_RUN_DT=$AUDIT_RUN_DT  -hivevar START_DATE=$START_DATE"

	#beehive $HADOOP_BATCH_HIVE -f $MDMVEE_HIVE_DML/insert_audit_reading_ivl_vee_interval.hql -hivevar AUDIT_RUN_DT=$AUDIT_RUN_DT -hivevar START_DATE=$START_DATE

	#ret=$?
	#if [ $ret -ne 0 ]; then
	#  echo "ERROR: Error while generating audit_reading_ivl_vee - `date "+%Y-%m-%d@%T"`"
	#  log_error_exit
	#fi
	
}

function main()
{
	echo "main function started.."
	initialize_log_file
	initialize_variables
	if [[ "${RUN_IN_EMR_SERVERLESS}" == "TRUE" ]];then
		insert_non_interval
		insert_interval
	fi	
}


main "${@}"
exit 0

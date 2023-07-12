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

	## 1. stg_meterevents.dovs_outage_fact_stg tbl drop
	dovs_outage_fact_stg_tbl_drop_qry='DROP TABLE IF EXISTS stg_meterevents.dovs_outage_fact_stg'
	echo "dovs_outage_fact_stg_tbl_drop_qry: $dovs_outage_fact_stg_tbl_drop_qry"
	dovs_outage_fact_stg_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$dovs_outage_fact_stg_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "dovs_outage_fact_stg_tbl_drop_qry_eid: $dovs_outage_fact_stg_tbl_drop_qry_eid"
	sleep 5
	dovs_outage_fact_stg_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $dovs_outage_fact_stg_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${dovs_outage_fact_stg_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.dovs_outage_fact_stg table drop succeeded."
	else
		fnLog "stg_meterevents.dovs_outage_fact_stg table drop failed."
		log_error_exit
	fi

	echo "============================"

	## 2. stg_meterevents.dovs_outage_premise_stg tbl drop
	dovs_outage_premise_stg_tbl_drop_qry='DROP TABLE IF EXISTS stg_meterevents.dovs_outage_premise_stg'
	echo "dovs_outage_premise_stg_tbl_drop_qry: $dovs_outage_premise_stg_tbl_drop_qry"
	dovs_outage_premise_stg_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$dovs_outage_premise_stg_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "dovs_outage_premise_stg_tbl_drop_qry_eid: $dovs_outage_premise_stg_tbl_drop_qry_eid"
	sleep 5
	dovs_outage_premise_stg_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $dovs_outage_premise_stg_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${dovs_outage_premise_stg_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.dovs_outage_premise_stg table drop succeeded."
	else
		fnLog "stg_meterevents.dovs_outage_premise_stg table drop failed."
		log_error_exit
	fi

	echo "============================"

	## 3. stg_meterevents.eems_outage_info_stg tbl drop
	eems_outage_info_stg_tbl_drop_qry='DROP TABLE IF EXISTS stg_meterevents.eems_outage_info_stg'
	echo "eems_outage_info_stg_tbl_drop_qry: $eems_outage_info_stg_tbl_drop_qry"
	eems_outage_info_stg_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$eems_outage_info_stg_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "eems_outage_info_stg_tbl_drop_qry_eid: $eems_outage_info_stg_tbl_drop_qry_eid"
	sleep 5
	eems_outage_info_stg_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $eems_outage_info_stg_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${eems_outage_info_stg_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.eems_outage_info_stg table drop succeeded."
	else
		fnLog "stg_meterevents.eems_outage_info_stg table drop failed."
		log_error_exit
	fi

	echo "============================"

	


	##===========================================
	## CREATE Tables

	#1. create stg_meterevents.dovs_outage_fact_stg table
	dovs_outage_fact_stg_tbl_create_qry="CREATE EXTERNAL TABLE stg_meterevents.dovs_outage_fact_stg(
  surr_key_nb double, 
  ci_nb int, 
  cmi_nb int, 
  cm_nb int, 
  cmm_nb int, 
  step_drtn_nb int, 
  dt_on_ts string, 
  dt_off_ts string, 
  outg_rec_nb bigint, 
  device_cd int, 
  equipment_cd int, 
  town_nb int, 
  fdr_nb int, 
  gis_crct_nb string, 
  location_id string, 
  opco_nbr int, 
  originator_id string, 
  district_nb string, 
  outage_nb string, 
  region_nb string, 
  subarea_nb string, 
  srvc_cntr_nb string, 
  mjr_cause_cd string, 
  mnr_cause_cd string, 
  sub_cause_cd string, 
  field_etr_set_ind_cd string, 
  aep_west_loc_id string, 
  state_abbr_tx string, 
  weather_cd string, 
  edw_last_updt_dt string, 
  sub_nb int, 
  jmed_fl string, 
  omed_fl string, 
  amed_fl string, 
  pmed_fl string, 
  opmed_fl string, 
  operating_unit_id string, 
  oprtg_unt_nm string, 
  opco_nm string, 
  district_name string, 
  area_nm string, 
  circuit_nm string, 
  substation_nm string, 
  substation_nb int, 
  aprvd_dt_ts string, 
  approver_id string, 
  atmpt_to_dsptch_dt string, 
  curr_intrpt_stat_cd string, 
  curr_rec_stat_cd string, 
  cust_ovrd_cd string, 
  intrptn_typ_cd string, 
  last_edit_dt_ts string, 
  mjr_evnt_cd string, 
  opco_nb int, 
  po_ordr_extrnl_typ_tx string, 
  po_ordr_typ_tx string, 
  trbl_addr_tx string, 
  intrptn_extnt_tx string, 
  oms_close_time string, 
  dvc_typ_nm string, 
  short_nm_clr_dev string, 
  equip_typ_nm string, 
  short_nm_eqp_typ string, 
  mjr_cause_nm string, 
  mnr_cause_nm string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://aep-datalake-work-${aws_env}/raw/dovs_eems/dovs_outage_fact_stg'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='OUTG_REC_NB,STATE_ABBR_TX,MJR_CAUSE_CD,DEVICE_CD,INTRPTN_TYP_CD,CURR_REC_STAT_CD', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog', 
  'transient_lastDdlTime'='1684756460');"
	echo "dovs_outage_fact_stg_tbl_create_qry: $dovs_outage_fact_stg_tbl_create_qry"
	dovs_outage_fact_stg_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$dovs_outage_fact_stg_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "dovs_outage_fact_stg_tbl_create_qry_eid: $dovs_outage_fact_stg_tbl_create_qry_eid"
	sleep 5
	dovs_outage_fact_stg_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $dovs_outage_fact_stg_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${dovs_outage_fact_stg_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.dovs_outage_fact_stg tbl creation succeeded."
	else
		fnLog "stg_meterevents.dovs_outage_fact_stg tbl creation failed."
		log_error_exit
	fi

	echo "============================"

	#2. create stg_meterevents.dovs_outage_premise_stg table
	dovs_outage_premise_stg_tbl_create_qry="CREATE EXTERNAL TABLE stg_meterevents.dovs_outage_premise_stg(
  outg_rec_nb bigint, 
  off_tm string, 
  rest_tm string, 
  customer_nm string, 
  house_nb string, 
  street_nm string, 
  city_nm string, 
  postal_cd string, 
  oms_area_cd string, 
  latitude_nb double, 
  longitude_nb double, 
  prmse_rgn_cd string, 
  call_xfrm_pole_nm string, 
  call_svc_pole_nm string, 
  nrml_prmse_phs_cd string, 
  station_nm string, 
  circt_nm string, 
  premise_nb string, 
  state_cd string, 
  account_nb string, 
  nrml_circt_nb string, 
  call_mtr_id string, 
  station_nb string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://aep-datalake-work-${aws_env}/raw/dovs_eems/dovs_outage_premise_stg'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='OUTG_REC_NB', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog', 
  'transient_lastDdlTime'='1684756580');"
	echo "dovs_outage_premise_stg_tbl_create_qry: $dovs_outage_premise_stg_tbl_create_qry"
	dovs_outage_premise_stg_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$dovs_outage_premise_stg_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "dovs_outage_premise_stg_tbl_create_qry_eid: $dovs_outage_premise_stg_tbl_create_qry_eid"
	sleep 5
	dovs_outage_premise_stg_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $dovs_outage_premise_stg_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${dovs_outage_premise_stg_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.dovs_outage_premise_stg tbl creation succeeded."
	else
		fnLog "stg_meterevents.dovs_outage_premise_stg tbl creation failed."
		log_error_exit
	fi

	echo "============================"


	#3. create stg_meterevents.eems_outage_info_stg table
	eems_outage_info_stg_tbl_create_qry="CREATE EXTERNAL TABLE stg_meterevents.eems_outage_info_stg(
  eqseq_id int, 
  equip_id string, 
  eqtype_id int, 
  serial_nb string, 
  company_nb string, 
  pcb_cd string, 
  prev_new_stat_in string, 
  graphic_in string, 
  kind_cd string, 
  kind_nm string, 
  eems_in string, 
  pmis_in string, 
  need_in string, 
  transaction_id int, 
  open_dt string, 
  remove_dt string, 
  close_dt string, 
  location_nb string, 
  location_id int, 
  station_nb string, 
  circuit_nb string, 
  gis_seg_nb int, 
  gis_seq_nb int, 
  phase_vl string, 
  install_dt string, 
  reason_ds string, 
  reason_cd string, 
  mfgr_nm string, 
  mfgr_cd string, 
  purch_stat_cd string, 
  seq_nb int, 
  status_ds string, 
  status_cd string, 
  char_nm string, 
  char_vl string, 
  eqtype_ds string, 
  mtrl_cd string, 
  xcoord_nb int, 
  ycoord_nb int, 
  subarea_nb string, 
  subarea_nm string, 
  company_nm string, 
  company_abbr string, 
  region_nb string, 
  region_nm string, 
  district_nb string, 
  district_nm string, 
  state_abbr string, 
  oper_company_nb string, 
  local_com_station_circuit_view_subarea_nb string, 
  local_com_station_circuit_view_subarea_nm string, 
  local_com_station_circuit_view_region_nb string, 
  local_com_station_circuit_view_region_nm string, 
  local_com_station_circuit_view_district_nb string, 
  local_com_station_circuit_view_district_nm string, 
  local_com_station_circuit_view_state_abbr string, 
  station_nm string, 
  circuit_nm string, 
  gis_circuit_nb string, 
  oper_company_nm string, 
  circuit_id int, 
  station_id int, 
  state_nm string, 
  energized string, 
  effective_dt string, 
  discontinue_dt string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://aep-datalake-work-${aws_env}/raw/dovs_eems/eems_outage_info_stg'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='EQSEQ_ID', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog', 
  'transient_lastDdlTime'='1684756398');"
	echo "eems_outage_info_stg_tbl_create_qry: $eems_outage_info_stg_tbl_create_qry"
	eems_outage_info_stg_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$eems_outage_info_stg_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "eems_outage_info_stg_tbl_create_qry_eid: $eems_outage_info_stg_tbl_create_qry_eid"
	sleep 5
	eems_outage_info_stg_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $eems_outage_info_stg_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${eems_outage_info_stg_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_meterevents.eems_outage_info_stg tbl creation succeeded."
	else
		fnLog "stg_meterevents.eems_outage_info_stg tbl creation failed."
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


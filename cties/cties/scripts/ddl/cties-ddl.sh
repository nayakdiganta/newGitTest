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

function drop_create_db_tables()
{
	##===========================================
	## DROP Tables

	## 1. stg_cties.meter_xfmr_mapping_stg tbl drop
	stg_cties_mapping_tbl_drop_qry='DROP TABLE IF EXISTS stg_cties.meter_xfmr_mapping_stg'
	echo "stg_cties_mapping_tbl_drop_qry: $stg_cties_mapping_tbl_drop_qry"
	stg_cties_mapping_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_cties_mapping_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_cties_mapping_tbl_drop_qry_eid: $stg_cties_mapping_tbl_drop_qry_eid"
	sleep 5
	stg_cties_mapping_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $stg_cties_mapping_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_cties_mapping_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_cties.meter_xfmr_mapping_stg table drop succeeded."
	else
		fnLog "stg_cties.meter_xfmr_mapping_stg table drop failed."
		log_error_exit
	fi

	echo "============================"

	## 2. stg_cties.meter_premise_stg tbl drop
	stg_cties_premise_tbl_drop_qry='DROP TABLE IF EXISTS stg_cties.meter_premise_stg'
	echo "stg_cties_premise_tbl_drop_qry: $stg_cties_premise_tbl_drop_qry"
	stg_cties_premise_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_cties_premise_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_cties_premise_tbl_drop_qry_eid: $stg_cties_premise_tbl_drop_qry_eid"
	sleep 5
	stg_cties_premise_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $stg_cties_premise_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_cties_premise_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_cties.meter_premise_stg table drop succeeded."
	else
		fnLog "stg_cties.meter_premise_stg table drop failed."
		log_error_exit
	fi

	echo "============================"

	## 3. cties.meter_xfmr_mapping tbl drop
	cties_mapping_tbl_drop_qry='DROP TABLE IF EXISTS cties.meter_xfmr_mapping'
	echo "cties_mapping_tbl_drop_qry: $cties_mapping_tbl_drop_qry"
	cties_mapping_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_mapping_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "cties_mapping_tbl_drop_qry_eid: $cties_mapping_tbl_drop_qry_eid"
	sleep 5
	cties_mapping_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $cties_mapping_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_mapping_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_mapping table drop succeeded."
	else
		fnLog "cties.meter_xfmr_mapping table drop failed."
		log_error_exit
	fi

	echo "============================"


	## 4. cties.meter_xfmr_voltg_summ tbl drop
	cties_voltg_summ_tbl_drop_qry='DROP TABLE IF EXISTS cties.meter_xfmr_voltg_summ'
	echo "cties_voltg_summ_tbl_drop_qry: $cties_voltg_summ_tbl_drop_qry"
	cties_voltg_summ_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_voltg_summ_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "cties_voltg_summ_tbl_drop_qry_eid: $cties_voltg_summ_tbl_drop_qry_eid"
	sleep 5
	cties_voltg_summ_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $cties_voltg_summ_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_voltg_summ_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_voltg_summ table drop succeeded."
	else
		fnLog "cties.meter_xfmr_voltg_summ table drop failed."
		log_error_exit
	fi

	echo "============================"

	## 5. cties.meter_xfmr_voltg_rf tbl drop
	cties_random_forest_tbl_drop_qry='DROP TABLE IF EXISTS cties.meter_xfmr_voltg_rf'
	echo "cties_random_forest_tbl_drop_qry: $cties_random_forest_tbl_drop_qry"
	cties_random_forest_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_random_forest_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "cties_random_forest_tbl_drop_qry_eid: $cties_random_forest_tbl_drop_qry_eid"
	sleep 5
	cties_random_forest_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $cties_random_forest_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_random_forest_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_voltg_rf table drop succeeded."
	else
		fnLog "cties.meter_xfmr_voltg_rf table drop failed."
		log_error_exit
	fi

	echo "============================"

	## 6. cties.meter_xfmr_voltg_summ_retie tbl drop 
	cties_retie_tbl_drop_qry='DROP TABLE IF EXISTS cties.meter_xfmr_voltg_summ_retie'
	echo "cties_retie_tbl_drop_qry: $cties_retie_tbl_drop_qry"
	cties_retie_tbl_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_retie_tbl_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "cties_retie_tbl_drop_qry_eid: $cties_retie_tbl_drop_qry_eid"
	sleep 5
	cties_retie_tbl_drop_qry_status=$(aws athena get-query-execution --query-execution-id $cties_retie_tbl_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_retie_tbl_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_voltg_summ_retie table drop succeeded."
	else
		fnLog "cties.meter_xfmr_voltg_summ_retie table drop failed."
		log_error_exit
	fi

	echo "============================"
	echo "============================"


	##===========================================
	## DROP and CREATE Databases

	# stg_cties_db_drop
	stg_cties_db_drop_qry='DROP DATABASE IF EXISTS stg_cties'
	echo "stg_cties_db_drop_qry: $stg_cties_db_drop_qry"
	stg_cties_db_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_cties_db_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_cties_db_drop_qry_eid: $stg_cties_db_drop_qry_eid"
	sleep 5
	stg_cties_db_drop_qry_status=$(aws athena get-query-execution --query-execution-id $stg_cties_db_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_cties_db_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_cties db drop succeeded."
	else
		fnLog "stg_cties db drop failed."
		log_error_exit
	fi

	echo "============================"

	# cties_db_drop
	cties_db_drop_qry="DROP DATABASE IF EXISTS cties;"
	echo "cties_db_drop_qry: $cties_db_drop_qry"
	cties_db_drop_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_db_drop_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "cties_db_drop_qry_eid: $cties_db_drop_qry_eid"
	sleep 5
	cties_db_drop_qry_status=$(aws athena get-query-execution --query-execution-id $cties_db_drop_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_db_drop_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties db drop succeeded."
	else
		fnLog "cties db drop failed."
		log_error_exit
	fi

	echo "============================"
	echo "============================"
	##===========================================
	## CREATE Databases

	# stg_cties_db_create
	stg_cties_db_create_qry='CREATE DATABASE stg_cties'
	echo "stg_cties_db_create_qry: $stg_cties_db_create_qry"
	stg_cties_db_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_cties_db_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_cties_db_create_qry_eid: $stg_cties_db_create_qry_eid"
	sleep 5
	stg_cties_db_create_qry_status=$(aws athena get-query-execution --query-execution-id $stg_cties_db_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_cties_db_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_cties db creation succeeded."
	else
		fnLog "stg_cties db creation failed."
		log_error_exit
	fi

	echo "============================"
	
	# cties_db_create
	cties_db_create_qry="CREATE DATABASE cties;"
	echo "cties_db_create_qry: $cties_db_create_qry"
	cties_db_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_db_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "cties_db_create_qry_eid: $cties_db_create_qry_eid"
	sleep 5
	cties_db_create_qry_status=$(aws athena get-query-execution --query-execution-id $cties_db_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_db_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties db creation succeeded."
	else
		fnLog "cties db creation failed."
		log_error_exit
	fi

	echo "============================"
	echo "============================"
	##===========================================
	## CREATE Tables

	# create stg_cties.meter_xfmr_mapping_stg table
	stg_mapping_tbl_create_qry="CREATE EXTERNAL TABLE stg_cties.meter_xfmr_mapping_stg(
  aep_premise_nb string, 
  aep_premise_loc_nb string, 
  aep_premise_long string, 
  aep_premise_lat string, 
  trsf_pole_nb string, 
  xfmr_long string, 
  xfmr_lat string, 
  trsf_mount_cd string, 
  gis_circuit_nb string, 
  gis_circuit_nm string, 
  gis_station_nb string, 
  gis_station_nm string, 
  company_cd string, 
  srvc_entn_cd string, 
  distance_feets double, 
  aws_update_dttm string, 
  run_control_id bigint)
PARTITIONED BY ( 
  aep_opco string, 
  asof_dt string) 
STORED AS PARQUET  
LOCATION
  's3://aep-datalake-work-${aws_env}/util/cties/meter_xfmr_mapping_stg'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='aep_premise_nb,trsf_pole_nb', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog');"
	# echo "stg_mapping_tbl_create_qry: $stg_mapping_tbl_create_qry"
	stg_mapping_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_mapping_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_mapping_tbl_create_qry_eid: $stg_mapping_tbl_create_qry_eid"
	sleep 5
	stg_mapping_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $stg_mapping_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_mapping_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_cties.meter_xfmr_mapping_stg tbl creation succeeded."
	else
		fnLog "stg_cties.meter_xfmr_mapping_stg tbl creation failed."
		log_error_exit
	fi

	echo "============================"

# create stg_cties.meter_premise_stg table
	stg_premise_tbl_create_qry="CREATE EXTERNAL TABLE stg_cties.meter_premise_stg(
  prem_nb string, 
  co_cd_ownr string, 
  state_cd string, 
  srvc_pnt_nm string, 
  mtr_pnt_nb string, 
  tarf_pnt_nb string, 
  premise_id string, 
  curr_bill_acct_nb string, 
  curr_bill_acct_id string, 
  curr_rvn_cls_cd double, 
  rvn_cls_cd_desc string, 
  mfr_devc_ser_nbr string, 
  mtr_stat_cd string, 
  mtr_stat_cd_desc string, 
  mtr_point_location string, 
  devc_stat_cd string, 
  devc_stat_cd_desc string, 
  devc_cd string, 
  devc_cd_desc string, 
  vintage_year string, 
  first_in_srvc_dt date, 
  phys_inst_dt date, 
  rmvl_ts date, 
  dial_cnst double, 
  bill_cnst double, 
  technology_tx string, 
  technology_desc string, 
  comm_cd string, 
  comm_desc string, 
  type_of_srvc_cd string, 
  type_of_srvc_cd_desc string, 
  type_srvc_cd string, 
  type_srvc_cd_desc string, 
  distributed_generation_flag string, 
  interval_data string, 
  intrvl_data_use_cd string, 
  intrvl_data_use_cd_desc string, 
  mtr_kind_cds string, 
  inst_ts timestamp, 
  bill_fl string, 
  mfr_cd string, 
  mfr_cd_desc string, 
  last_fld_test_date decimal(8,0), 
  pgm_id_nm string, 
  aws_update_dttm string)
PARTITIONED BY ( 
  aep_opco string) 
STORED AS PARQUET 
LOCATION
  's3://aep-datalake-work-${aws_env}/util/cties/meter_premise_stg'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='PREM_NB,MFR_DEVC_SER_NBR', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog');"
	# echo "stg_premise_tbl_create_qry: $stg_premise_tbl_create_qry"
	stg_premise_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$stg_premise_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')
	echo "stg_premise_tbl_create_qry_eid: $stg_premise_tbl_create_qry_eid"
	sleep 5
	stg_premise_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $stg_premise_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${stg_premise_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "stg_cties.meter_premise_stg tbl creation succeeded."
	else
		fnLog "stg_cties.meter_premise_stg tbl creation failed."
		log_error_exit
	fi

	echo "============================"

	# cties_map_tbl_create
	cties_mapping_tbl_create_qry="CREATE EXTERNAL TABLE cties.meter_xfmr_mapping(
  aep_premise_nb string, 
  serialnumber string, 
  aep_premise_loc_nb string, 
  aep_premise_long string, 
  aep_premise_lat string, 
  trsf_pole_nb string, 
  xfmr_long string, 
  xfmr_lat string, 
  trsf_mount_cd string, 
  gis_circuit_nb string, 
  gis_circuit_nm string, 
  gis_station_nb string, 
  gis_station_nm string, 
  company_cd string, 
  srvc_entn_cd string, 
  distance_feets double, 
  county_cd string,
  county_nm string,
  district_nb string,
  district_nm string,
  type_srvc_cd string,
  type_srvc_cd_desc string,
  srvc_addr_1_nm string,
  srvc_addr_2_nm string,
  srvc_addr_3_nm string,
  srvc_addr_4_nm string,
  serv_city_ad string,
  state_cd string,
  serv_zip_ad string,
  aws_update_dttm string, 
  run_control_id bigint)
PARTITIONED BY ( 
  aep_opco string, 
  asof_dt string)
STORED AS PARQUET 
LOCATION
  's3://aep-datalake-consume-${aws_env}/cties/meter_xfmr_mapping'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='aep_premise_nb,trsf_pole_nb', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog');"

	# echo "cties_mapping_tbl_create_qry: $cties_mapping_tbl_create_qry"

	cties_mapping_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_mapping_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')

	echo "cties_mapping_tbl_create_qry_eid: $cties_mapping_tbl_create_qry_eid"
	sleep 5
	cties_mapping_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $cties_mapping_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_mapping_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_mapping table creation succeeded."
	else
		fnLog "cties.meter_xfmr_mapping table creation failed."
		log_error_exit
	fi

	echo "============================"
	
	# cties.meter_xfmr_voltg_summ create
	cties_voltg_summ_tbl_create_qry="CREATE EXTERNAL TABLE cties.meter_xfmr_voltg_summ(
  aep_premise_nb string, 
  serialnumber string, 
  trsf_pole_nb string, 
  trsf_mount_cd string,
  gis_circuit_nb string, 
  gis_circuit_nm string, 
  gis_station_nb string, 
  gis_station_nm string, 
  company_cd string, 
  srvc_entn_cd string, 
  distance_feets double, 
  xf_meter_cnt int, 
  aep_derived_uom string, 
  aep_raw_uom string, 
  aep_channel_id string, 
  gis_mapping_asof_dt string, 
  read_array array<float>, 
  xf_read_array array<float>, 
  xf_read_array_whole array<float>, 
  eucl_dist float, 
  cos_sim float, 
  shape_mismatch_cnt int, 
  mean_eucl_dist double, 
  mean_shape_mismatch_cnt double, 
  stddev_eucl_dist double, 
  stddev_shape_mismatch_cnt double, 
  z_score_eucl_dist double, 
  z_score_shape_mismatch_cnt double, 
  median_eucl_dist float, 
  median_shape_mismatch_cnt int, 
  median_z_score_eucl_dist double, 
  aws_update_dttm string, 
  run_control_id bigint)
PARTITIONED BY ( 
  aep_opco string, 
  aep_usage_dt string) 
STORED AS PARQUET 
LOCATION
  's3://aep-datalake-consume-${aws_env}/cties/meter_xfmr_voltg_summ'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.bloom.filter.columns'='aep_premise_nb,serialnumber,trsf_pole_nb', 
  'parquet.bloom.filter.fpp'='0.05', 
  'parquet.compression'='SNAPPY', 
  'parquet.create.index'='true', 
  'spark.sql.partitionProvider'='catalog');"

	# echo "cties_voltg_summ_tbl_create_qry: $cties_voltg_summ_tbl_create_qry"

	cties_voltg_summ_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_voltg_summ_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')

	echo "cties_voltg_summ_tbl_create_qry_eid: $cties_voltg_summ_tbl_create_qry_eid"
	sleep 5
	cties_voltg_summ_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $cties_voltg_summ_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_voltg_summ_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_voltg_summ table creation succeeded."
	else
		fnLog "cties.meter_xfmr_voltg_summ table creation failed."
		log_error_exit
	fi


	echo "============================"
	
	# cties.meter_xfmr_voltg_rf create
	cties_random_forest_tbl_create_qry="CREATE EXTERNAL TABLE cties.meter_xfmr_voltg_rf(
	aep_premise_nb string COMMENT '', 
	serialnumber string COMMENT '', 
	trsf_pole_nb string COMMENT '', 
	trsf_mount_cd string COMMENT '',
	gis_circuit_nb string COMMENT '', 
	gis_circuit_nm string COMMENT '', 
	gis_station_nb string COMMENT '', 
	gis_station_nm string COMMENT '', 
	company_cd string COMMENT '', 
	srvc_entn_cd string COMMENT '', 
	distance_feets double COMMENT '', 
	xf_meter_cnt int COMMENT '', 
	median_eucl_dist float COMMENT '', 
	median_shape_mismatch_cnt int COMMENT '', 
	median_z_score_eucl_dist double COMMENT '', 
	y_pred int COMMENT '', 
	pred_0 double COMMENT '', 
	pred_1 double COMMENT '', 
	run_control_id bigint COMMENT '', 
	aws_update_dttm string COMMENT '')
	PARTITIONED BY ( 
	aep_opco string COMMENT '', 
	aep_run_dt string COMMENT '')
	ROW FORMAT SERDE 
	'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
	STORED AS INPUTFORMAT 
	'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
	OUTPUTFORMAT 
	'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
	LOCATION
	's3://aep-datalake-consume-${aws_env}/cties/meter_xfmr_voltg_rf'
	TBLPROPERTIES (
	'bucketing_version'='2', 
	'parquet.bloom.filter.columns'='aep_premise_nb,serialnumber,trsf_pole_nb', 
	'parquet.bloom.filter.fpp'='0.05', 
	'parquet.compression'='SNAPPY', 
	'parquet.create.index'='true', 
	'spark.sql.partitionProvider'='catalog');"

	# echo "cties_random_forest_tbl_create_qry: $cties_random_forest_tbl_create_qry"

	cties_random_forest_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_random_forest_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')

	echo "cties_random_forest_tbl_create_qry_eid: $cties_random_forest_tbl_create_qry_eid"
	sleep 5
	cties_random_forest_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $cties_random_forest_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_random_forest_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_voltg_rf table creation succeeded."
	else
		fnLog "cties.meter_xfmr_voltg_rf table creation failed."
		log_error_exit
	fi

	echo "============================"
	
	# cties.meter_xfmr_voltg_summ_retie create
	cties_retie_tbl_create_qry="CREATE EXTERNAL TABLE cties.meter_xfmr_voltg_summ_retie(
	serialnumber string, 
	aep_premise_nb string, 
	aep_premise_loc_nb string, 
	aep_premise_long string, 
	aep_premise_lat string, 
	trsf_pole_nb string, 
	xfmr_long string, 
	xfmr_lat string, 
	trsf_mount_cd string, 
	gis_circuit_nb string, 
	gis_circuit_nm string, 
	gis_station_nb string, 
	gis_station_nm string, 
	company_cd string, 
	srvc_entn_cd string, 
	distance_feets double, 
	county_cd string, 
	county_nm string, 
	district_nb string, 
	district_nm string, 
	type_srvc_cd string, 
	type_srvc_cd_desc string, 
	srvc_addr_1_nm string, 
	srvc_addr_2_nm string, 
	srvc_addr_3_nm string, 
	srvc_addr_4_nm string, 
	serv_city_ad string, 
	state_cd string, 
	serv_zip_ad string, 
	run_control_id bigint, 
	gis_mapping_asof_dt string, 
	median_eucl_dist float, 
	median_shape_mismatch_cnt int, 
	median_z_score_eucl_dist double, 
	xf_meter_cnt bigint, 
	curr_trsf_pole_nb string, 
	orig_median_eucl_dist float, 
	orig_median_z_score_eucl_dist double, 
	y_pred int, 
	pred_0 double, 
	pred_1 double, 
	decile string, 
	new_trsf_pole_nb string, 
	pred_eucl_dist float, 
	dist double, 
	tran_rank int, 
	pred_median_eucl_dist float)
	PARTITIONED BY ( 
	aep_opco string, 
	aep_run_dt string)
	ROW FORMAT SERDE 
	'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
	STORED AS INPUTFORMAT 
	'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
	OUTPUTFORMAT 
	'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
	LOCATION
	's3://aep-datalake-consume-${aws_env}/cties/meter_xfmr_voltg_summ_retie'
	TBLPROPERTIES (
	'bucketing_version'='2', 
	'parquet.bloom.filter.fpp'='0.05', 
	'parquet.compression'='SNAPPY', 
	'parquet.create.index'='true', 
	'spark.sql.partitionProvider'='catalog');"

	# echo "cties_retie_tbl_create_qry: $cties_retie_tbl_create_qry"

	cties_retie_tbl_create_qry_eid=$(aws athena start-query-execution --region $aws_region --query-string "$cties_retie_tbl_create_qry" --result-configuration "OutputLocation=$s3_temp_loc" --work-group ${ATHENA_WORK_GROUP} --query "QueryExecutionId" |sed 's/"//g')

	echo "cties_retie_tbl_create_qry_eid: $cties_retie_tbl_create_qry_eid"
	sleep 5
	cties_retie_tbl_create_qry_status=$(aws athena get-query-execution --query-execution-id $cties_retie_tbl_create_qry_eid --query "QueryExecution.Status.State" |sed 's/"//g')
	
	if [ "${cties_retie_tbl_create_qry_status}" == "SUCCEEDED" ]; then
		fnLog "cties.meter_xfmr_voltg_summ_retie table creation succeeded."
	else
		fnLog "cties.meter_xfmr_voltg_summ_retie table creation failed."
		log_error_exit
	fi
}

function main()
{
    if [ $# -ne 1 ]; then
	    # Wrong number of arguments, exit the script.	    
		echo "Wrong number of arguments. Pls provide env argument, exiting the script."
	    log_error_exit
    fi  

	fnLog "========================== DB AND TABLE DROP/CREATION PROCESS STARTED =========================="
	drop_create_db_tables
	fnLog "==========================  DB AND TABLE DROP/CREATION PROCESS ENDED  =========================="
}

main "${@}"
exit 0


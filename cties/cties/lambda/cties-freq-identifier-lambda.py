import json
import boto3
from botocore.config import Config
import sys
import logging
import json
import os
import time
from datetime import datetime
from datetime import datetime, date, timedelta
logger = logging.getLogger()
logger.setLevel(logging.INFO)


## Initialize the boto3 clients
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
ssm_client = boto3.client('ssm')
sns_client = boto3.client("sns")
glue_client = boto3.client('glue')
stepfunc_client=boto3.client('stepfunctions')


## curret timestamp function and sns topic function creation
def current_timestamp():
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')

def snsLog(snsTopicARN, msg_subject, msg_body):
    logger.info(current_timestamp() + msg_body)
    response = sns_client.publish(TargetArn=snsTopicARN, Message=json.dumps({'default': json.dumps(msg_body)}), Subject=msg_subject, MessageStructure='json')
    return

## Lambda Handler
def lambda_handler(event, context):
    print("DEBUG: - Received event: " + str(event))    
    print("DEBUG: - context : " + str(context))    
    print("DEBUG: - Received event: " + json.dumps(event, indent=2))

    logger.info("## Lambda Handler to copy/move the GIS Mapping Data from landing to raw zone ##")
    logger.info("Lambda function ARN: " + str(context.invoked_function_arn))
    logger.info("Lambda Request ID: " + str(context.aws_request_id))
    logger.info("DEBUG: " + str(os.environ))

    ### environment Variables
    ssm_run_interval_name = os.environ['SSM_RUN_INTERVAL_NAME']
    ssm_last_run_aep_usage_dt_to_name=os.environ['SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME']
    
    ### Event Variables
    AWS_ENV =  event['AWS_ENV']
    S3_DATA_RAW=  event['S3_DATA_RAW']
    S3_DATA_WORK = event['S3_DATA_WORK']
    S3_DATA_CONSUME=event['S3_DATA_CONSUME']  
    # VAR_EXTRACT_DT_FROM = event['VAR_EXTRACT_DT_FROM']       
    # VAR_EXTRACT_DT_TO = event['VAR_EXTRACT_DT_TO']
    VAR_OPCO=event['VAR_OPCO']
    VAR_SNS_TOPIC=event['VAR_SNS_TOPIC']

    VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
    VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
    
    # ssm_run_interval_name=SSM_RUN_INTERVAL_NAME
    # ssm_last_run_aep_usage_dt_to_name=SSM_LAST_RUN_AEP_USAGE_DT_TO_NAME
    
    ##### create ssm_last_run_aep_usage_dt_to_name(<OPCO>) if not exists
    try:
        ssm_last_run_aep_usage_dt_to_name=ssm_last_run_aep_usage_dt_to_name.lower() + '_' + VAR_OPCO
        ssm_client.get_parameter(Name=ssm_last_run_aep_usage_dt_to_name, WithDecryption=False)
        parameter_exists=True
    except ssm_client.exceptions.ParameterNotFound:
        parameter_exists=False
    if not parameter_exists:
        ssm_client.put_parameter(Name=ssm_last_run_aep_usage_dt_to_name, Value= ' ' , Type='String', Overwrite=True )
        print(f"successfully created ssm parameter: {ssm_last_run_aep_usage_dt_to_name}")
    else:
        print(f"already exists ssm parameter: {ssm_last_run_aep_usage_dt_to_name}")
    
    try:
        datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')
        ssm_run_interval_resp = ssm_client.get_parameter(Name=ssm_run_interval_name, WithDecryption=False)
        ssm_run_interval_val=int(ssm_run_interval_resp['Parameter']['Value'])  ## default is 14

        voltg_summ_to_process='yes'

        ssm_last_run_aep_usage_dt_to_resp = ssm_client.get_parameter(Name=ssm_last_run_aep_usage_dt_to_name, WithDecryption=False)
        ssm_last_run_aep_usage_dt_to_val = ssm_last_run_aep_usage_dt_to_resp['Parameter']['Value']

        var_today=date.today()
        var_delta=timedelta(days=ssm_run_interval_val) ## 14

        if ssm_last_run_aep_usage_dt_to_val.strip() == '':
            voltg_summ_to_process='yes'
        else:

            date_obj=datetime.strptime(ssm_last_run_aep_usage_dt_to_val, '%Y-%m-%d').date()            
            var_end_date = date_obj + var_delta

            ## To check the schedule is running every alternate saturday
            print('var_today:',str(var_today))
            if (var_today - var_end_date) < (var_delta):                    
                voltg_summ_to_process = 'no'

        print('voltg_summ_to_process:',voltg_summ_to_process)

        output_data = {

                    'AWS_ENV': AWS_ENV,
                    'S3_DATA_RAW':S3_DATA_RAW,
                    'S3_DATA_WORK': S3_DATA_WORK,
                    'S3_DATA_CONSUME': S3_DATA_CONSUME,
                #   'VAR_EXTRACT_DT_FROM': VAR_EXTRACT_DT_FROM,
                    # 'VAR_EXTRACT_DT_TO': VAR_EXTRACT_DT_TO,
                    'VAR_OPCO': VAR_OPCO, 
                    'VAR_SNS_TOPIC': VAR_SNS_TOPIC,
                    'VOLTG_SUMM_TO_PROCESS':voltg_summ_to_process
                }
        # output_json = json.dumps(output_data)
        output_json = output_data
        print('output_json',output_json)

        return output_json

        logger.info("completed")

   
    except Exception as e:
        print(e)
        msg_title=f"Lambda: cties-freq-identifier-lambda failed"
        print(msg_title)
        msg_body=f"## Lambda: cties-freq-identifier-lambda failed while processing.."
        print(msg_body)
        snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
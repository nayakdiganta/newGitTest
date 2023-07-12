import json
import boto3
from botocore.config import Config
import sys
import logging
import json
import os
import time
from datetime import datetime
import urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## Initialize the boto3 clients
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
ssm_client = boto3.client('ssm')
sns_client = boto3.client("sns")
glue_client = boto3.client('glue')
stepfunc_client=boto3.client('stepfunctions')

AWS_ENV = os.environ['AWS_ENV']                    #'dev'
S3_DATA_LANDING = os.environ['S3_DATA_LANDING']    #"aep-dl-landing" 
S3_DATA_RAW = os.environ['S3_DATA_RAW']            #"aep-datalake-raw" 
S3_DATA_WORK = os.environ['S3_DATA_WORK']          #"aep-datalake-work" 
S3_DATA_CONSUME = os.environ['S3_DATA_CONSUME']    #"aep-datalake-consume" 
VAR_SNS_TOPIC = os.environ['VAR_SNS_TOPIC']        #"arn:aws:sns:us-east-1:889415020100:aep-dl-SNS-success-dev,arn:aws:sns:us-east-1:889415020100:aep-dl-SNS-failure-dev"
VAR_STATEMACHINE_ARN = os.environ['VAR_STATEMACHINE_ARN']

VAR_SNS_TOPIC_SUCCESS=VAR_SNS_TOPIC.split(',')[0]
VAR_SNS_TOPIC_FAILURE=VAR_SNS_TOPIC.split(',')[1]
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

    extract_dt=datetime.today().strftime('%Y%m%d')

    try:
        # s3 = boto3.resource('s3') 
        datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')

        source_bucket_name = event['Records'][0]['s3']['bucket']['name']
        print("DEBUG: - source_bucket_name: " + source_bucket_name)
   
        landing_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        print('DEBUG: - landing_key: ',landing_key)
        filename = os.path.basename(landing_key)
        print('DEBUG: - filename: ',str(filename))

        opco_name=str(landing_key).split('/')[2]  ## extract opco_name from landing path
        print('DEBUG: - opco_name:',opco_name)
        
        # source_folder_prefix = f'util/cties/{opco_name}/'
        target_bucket_name = S3_DATA_RAW.lower() + '-' + AWS_ENV.lower()        
                        
        source_file_copy_object={'Bucket':source_bucket_name,'Key':landing_key}
       
        target_prefix_file= f"util/cties/extract_dt={str(extract_dt)}/aep_opco={opco_name}/" + filename

        s3_resource.Object(target_bucket_name,target_prefix_file).copy_from(CopySource=source_file_copy_object)

        ##==============================================================================================##
        ## Start step function            
  
        input_data = {
            'AWS_ENV': AWS_ENV,
            'S3_DATA_RAW': 's3://' + S3_DATA_RAW,
            'S3_DATA_WORK': 's3://' + S3_DATA_WORK,
            'S3_DATA_CONSUME': 's3://' + S3_DATA_CONSUME,
            'VAR_EXTRACT_DT_FROM': str(extract_dt),
            'VAR_EXTRACT_DT_TO': str(extract_dt),
            'VAR_OPCO': opco_name, 
            'VAR_SNS_TOPIC': VAR_SNS_TOPIC
        }

        # Convert the input data to JSON format
        input_json = json.dumps(input_data)

        response = stepfunc_client.start_execution(
                stateMachineArn=VAR_STATEMACHINE_ARN,
                name=opco_name + '_' + str(datetime.now().strftime('%Y%m%d%H%M%S')),
            input=input_json
        )

        print(response)
        
        
        logger.info("Copying files from source bucket  folder to target bucket folder is completed!")
        logger.info("Triggered the step function and execution arn is: " + str(response['executionArn']) )
   
    except Exception as e:
        print(e)
        msg_title=f"Lambda: cties-meter-xfmr-map-landing-raw-lambda failed"
        print(msg_title)
        msg_body=f"## Lambda: cties-meter-xfmr-map-landing-raw-lambda.py failed while processing for OPCO: <{opco_name}> for date: {extract_dt}"
        print(msg_body)
        snsLog(VAR_SNS_TOPIC_FAILURE, msg_title, msg_body)
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
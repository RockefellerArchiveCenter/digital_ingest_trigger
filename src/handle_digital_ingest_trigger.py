#!/usr/bin/env python3

import logging
import traceback
from os import environ

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

START_STATUS = 'START'

full_config_path = f"/{environ.get('ENV')}/{environ.get('APP_CONFIG_PATH')}"


def get_config(ssm_parameter_path):
    """Fetch config values from Parameter Store.

    Args:
        ssm_parameter_path (str): Path to parameters

    Returns:
        configuration (dict): all parameters found at the supplied path.
    """
    configuration = {}
    try:
        ssm_client = boto3.client(
            'ssm',
            region_name=environ.get('AWS_REGION'))

        param_details = ssm_client.get_parameters_by_path(
            Path=ssm_parameter_path,
            Recursive=False,
            WithDecryption=True)

        for param in param_details.get('Parameters', []):
            param_path_array = param.get('Name').split("/")
            section_position = len(param_path_array) - 1
            section_name = param_path_array[section_position]
            configuration[section_name] = param.get('Value')

    except BaseException:
        print("Encountered an error loading config from SSM.")
        traceback.print_exc()
    finally:
        return configuration


def run_task(ecs_client, config, task_definition, environment):
    service_response = ecs_client.run_task(
        cluster=config.get('ECS_CLUSTER'),
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [config.get('ECS_SUBNET')],
                'securityGroups': [config.get('ECS_SECURITY_GROUP')],
                'assignPublicIp': 'DISABLED'
            }
        },
        taskDefinition=task_definition,
        count=1,
        startedBy='lambda/digital_ingest_trigger',
        overrides={
            'containerOverrides': [
                {
                    "name": task_definition,
                    "environment": environment
                }
            ]
        }
    )
    return ", ".join([t['taskArn'] for t in service_response['tasks']])


def lambda_handler(event, context):
    """Triggers ECS task."""

    config = get_config(full_config_path)
    ecs_client = boto3.client(
        'ecs',
        region_name=environ.get('AWS_REGION'))

    if event['Records'][0].get('eventSource') == 'aws:s3':
        """Handles events from S3 buckets."""

        logger.info("Received S3 event")

        event_type = event['Records'][0]['eventName']

        response = 'Nothing to do for S3 event'

        if event_type in ['ObjectCreated:Put',
                          'ObjectCreated:CompleteMultipartUpload']:
            """Handles object creation events."""
            package_id = event['Records'][0]['s3']['object']['key'].split('.')[
                0]
            environment = [
                {
                    "name": "PACKAGE_ID",
                    "value": package_id
                }
            ]
            task_id = run_task(ecs_client,
                               config,
                               'digital_ingest_discovery',
                               environment)
            response = f"Task {task_id} with definition digital_ingest_discovery started for package {package_id}."

    elif event['Records'][0].get('eventSource') == 'aws:sqs':
        """Handles events from SQS."""

        logger.info("Message batch received.")

        for record in event['Records']:
            attributes = record['messageAttributes']

            response = 'Nothing to do for SQS event'

            package_id = attributes.get('package_id', {}).get('stringValue')

            environment = [
                {
                    "name": "PACKAGE_ID",
                    "value": package_id
                }
            ]

            if attributes.get('requested_status', {}).get(
                    'stringValue') == START_STATUS:
                task_id = run_task(ecs_client,
                                   config,
                                   attributes['service']['stringValue'],
                                   environment)
                response = f"Task {task_id} with definition {attributes['service']['stringValue']} started for package {package_id}."
    else:
        raise Exception('Unsure how to parse message')

    logger.info(response)

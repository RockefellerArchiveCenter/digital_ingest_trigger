#!/usr/bin/env python3

import json
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
            region_name=environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

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
    return ecs_client.run_task(
        cluster=config.get('ECS_CLUSTER'),
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [config.get('ECS_SUBNET')],
                'securityGroups': [],
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


def lambda_handler(event, context):
    """Triggers ECS task."""

    config = get_config(full_config_path)
    ecs_client = boto3.client(
        'ecs',
        region_name=environ.get('AWS_DEFAULT_REGION', 'us-east-1'))

    if event['Records'][0].get('s3'):
        """Handles events from S3 buckets."""

        logger.info(f"Received S3 event {event}")

        event_type = event['Records'][0]['eventName']

        response = f'Nothing to do for S3 event: {event}'

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
            response = run_task(ecs_client,
                                config,
                                'ursa_major',
                                environment)

    elif event['Records'][0].get('Sns'):
        """Handles events from SNS."""

        logger.info(f"Received SNS event {event}")

        attributes = event['Records'][0]['Sns']['MessageAttributes']

        response = f'Nothing to do for SNS event: {event}'

        package_id = attributes.get('package_id', {}).get('Value')

        environment = [
            {
                "name": "PACKAGE_ID",
                "value": package_id
            }
        ]

        if attributes.get('requested_status', {}).get('Value') == START_STATUS:
            response = run_task(ecs_client,
                                config,
                                attributes['service']['Value'],
                                environment)
    else:
        raise Exception('Unsure how to parse message')

    logger.info(response)
    return json.dumps(response, default=str)

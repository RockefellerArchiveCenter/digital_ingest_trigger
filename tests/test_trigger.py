#!/usr/bin/env python3

import json
from pathlib import Path
from unittest.mock import patch

import boto3
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from src.handle_digital_ingest_trigger import get_config, lambda_handler


@mock_aws
@patch('src.handle_digital_ingest_trigger.get_config')
def test_s3_args(mock_config):
    test_cluster_name = "default"
    mock_config.return_value = {
        "AWS_REGION": "us-east-1",
        "ECS_CLUSTER": test_cluster_name,
        "ECS_SUBNET": "subnet",
        "ECS_SECURITY_GROUP": "sg-123456789"}
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=test_cluster_name)
    client.register_task_definition(
        family="digital_ingest_discovery",
        containerDefinitions=[
            {
                "name": "digital_ingest_discovery",
                "image": "docker/hello-world:latest",
                "cpu": 1024,
                "memory": 400,
            }
        ],
    )

    with open(Path('fixtures', 's3_put.json'), 'r') as df:
        message = json.load(df)
        lambda_handler(message, None)

        tasks = client.list_tasks(cluster=test_cluster_name)
        assert len(tasks['taskArns']) == 1

        task_response = client.describe_tasks(
            cluster=test_cluster_name,
            tasks=[tasks['taskArns'][0]])

        assert task_response['tasks'][0]['startedBy'] == 'lambda/digital_ingest_trigger'
        assert task_response['tasks'][0][
            'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digital_ingest_discovery:1"
        with open(Path('fixtures', 's3_args.json'), 'r') as af:
            args = json.load(af)
            assert task_response['tasks'][0]['overrides'] == args


@mock_aws
@patch('src.handle_digital_ingest_trigger.get_config')
def test_sqs_args(mock_config):
    test_cluster_name = "default"
    mock_config.return_value = {
        "AWS_REGION": "us-east-1",
        "ECS_CLUSTER": test_cluster_name,
        "ECS_SUBNET": "subnet",
        "ECS_SECURITY_GROUP": "sg-123456789"}
    client = boto3.client("ecs", region_name="us-east-1")
    client.create_cluster(clusterName=test_cluster_name)
    client.register_task_definition(
        family="digital_ingest_assembly",
        containerDefinitions=[
            {
                "name": "digital_ingest_assembly",
                "image": "docker/hello-world:latest",
                "cpu": 1024,
                "memory": 400,
            }
        ],
    )

    with open(Path('fixtures', 'sqs.json'), 'r') as df:
        message = json.load(df)
        lambda_handler(message, None)

        tasks = client.list_tasks(cluster=test_cluster_name)
        assert len(tasks['taskArns']) == 1

        task_response = client.describe_tasks(
            cluster=test_cluster_name,
            tasks=[tasks['taskArns'][0]])

        assert task_response['tasks'][0]['startedBy'] == 'lambda/digital_ingest_trigger'
        assert task_response['tasks'][0][
            'taskDefinitionArn'] == f"arn:aws:ecs:us-east-1:{DEFAULT_ACCOUNT_ID}:task-definition/digital_ingest_assembly:1"
        with open(Path('fixtures', 'sqs_args.json'), 'r') as af:
            args = json.load(af)
            assert task_response['tasks'][0]['overrides'] == args

    """No new task started."""
    with open(Path('fixtures', 'sqs_idle.json'), 'r') as df:
        message = json.load(df)
        lambda_handler(message, None)

        tasks = client.list_tasks(cluster=test_cluster_name)
        assert len(tasks['taskArns']) == 1


@mock_aws
def test_config():
    ssm = boto3.client('ssm', region_name='us-east-1')
    path = "/dev/digital_ingest_trigger"
    for name, value in [("foo", "bar"), ("baz", "buzz")]:
        ssm.put_parameter(
            Name=f"{path}/{name}",
            Value=value,
            Type="SecureString",
        )
    config = get_config(path)
    assert config == {'foo': 'bar', 'baz': 'buzz'}

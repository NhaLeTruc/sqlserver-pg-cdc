"""
Contract tests for Kafka Connect REST API using Pact.

Ensures API compatibility between versions using consumer-driven contracts.

These tests define the expectations that the reconciliation service (consumer)
has for the Kafka Connect API (provider).
"""

import pytest
import requests
import os
from pathlib import Path

# Import only if pact-python is available
try:
    from pact import Consumer, Provider, Like, EachLike, Term
    PACT_AVAILABLE = True
except ImportError:
    PACT_AVAILABLE = False
    pytest.skip("pact-python not installed", allow_module_level=True)


# Pact directory for storing contracts
PACT_DIR = Path(__file__).parent.parent.parent / "pacts"
PACT_DIR.mkdir(exist_ok=True)


@pytest.fixture(scope='module')
def kafka_connect_pact():
    """
    Setup Pact consumer/provider relationship.

    Creates a mock Kafka Connect API server for testing.
    """
    if not PACT_AVAILABLE:
        pytest.skip("pact-python not available")

    pact = Consumer('reconciliation-service').has_pact_with(
        Provider('kafka-connect-api'),
        host_name='localhost',
        port=1234,  # Mock server port
        pact_dir=str(PACT_DIR)
    )

    pact.start_service()
    yield pact
    pact.stop_service()


def test_get_connectors_list(kafka_connect_pact):
    """
    Test GET /connectors returns list of connector names.

    Contract: GET /connectors should return array of strings.
    """
    expected_response = EachLike('sqlserver-source', minimum=1)

    (kafka_connect_pact
     .given('connectors exist in the system')
     .upon_receiving('a request for all connector names')
     .with_request('GET', '/connectors')
     .will_respond_with(200, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(f'{kafka_connect_pact.uri}/connectors')

        assert response.status_code == 200
        assert isinstance(response.json(), list)
        assert len(response.json()) >= 1
        assert all(isinstance(name, str) for name in response.json())


def test_get_connector_status_running(kafka_connect_pact):
    """
    Test GET /connectors/{name}/status returns connector status.

    Contract: Status endpoint should return connector and task states.
    """
    expected_response = {
        'name': Like('sqlserver-source'),
        'connector': {
            'state': Like('RUNNING'),
            'worker_id': Like('kafka-connect:8083')
        },
        'tasks': EachLike({
            'id': Like(0),
            'state': Like('RUNNING'),
            'worker_id': Like('kafka-connect:8083')
        }, minimum=1)
    }

    (kafka_connect_pact
     .given('connector sqlserver-source exists and is running')
     .upon_receiving('a request for connector status')
     .with_request('GET', '/connectors/sqlserver-source/status')
     .will_respond_with(200, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(
            f'{kafka_connect_pact.uri}/connectors/sqlserver-source/status'
        )

        assert response.status_code == 200
        data = response.json()
        assert 'name' in data
        assert 'connector' in data
        assert 'tasks' in data
        assert data['connector']['state'] in ['RUNNING', 'PAUSED', 'FAILED']
        assert isinstance(data['tasks'], list)


def test_get_connector_status_not_found(kafka_connect_pact):
    """
    Test GET /connectors/{name}/status returns 404 for non-existent connector.

    Contract: Non-existent connector should return 404.
    """
    expected_response = {
        'error_code': Like(404),
        'message': Term(r'.*not found.*', 'Connector not found')
    }

    (kafka_connect_pact
     .given('connector nonexistent-connector does not exist')
     .upon_receiving('a request for non-existent connector status')
     .with_request('GET', '/connectors/nonexistent-connector/status')
     .will_respond_with(404, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(
            f'{kafka_connect_pact.uri}/connectors/nonexistent-connector/status'
        )

        assert response.status_code == 404
        data = response.json()
        assert 'error_code' in data or 'message' in data


def test_get_connector_config(kafka_connect_pact):
    """
    Test GET /connectors/{name}/config returns connector configuration.

    Contract: Config endpoint should return configuration object.
    """
    expected_response = {
        'name': Like('sqlserver-source'),
        'connector.class': Term(
            r'io\.debezium\..+',
            'io.debezium.connector.sqlserver.SqlServerConnector'
        ),
        'tasks.max': Like('1'),
        'database.hostname': Like('localhost'),
        'database.port': Like('1433')
    }

    (kafka_connect_pact
     .given('connector sqlserver-source exists')
     .upon_receiving('a request for connector configuration')
     .with_request('GET', '/connectors/sqlserver-source/config')
     .will_respond_with(200, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(
            f'{kafka_connect_pact.uri}/connectors/sqlserver-source/config'
        )

        assert response.status_code == 200
        data = response.json()
        assert 'name' in data
        assert 'connector.class' in data
        assert 'tasks.max' in data


def test_get_connector_tasks(kafka_connect_pact):
    """
    Test GET /connectors/{name}/tasks returns task list.

    Contract: Tasks endpoint should return array of task info.
    """
    expected_response = EachLike({
        'id': Like({'connector': 'sqlserver-source', 'task': 0}),
        'config': Like({
            'task.class': Term(
                r'io\.debezium\..+',
                'io.debezium.connector.sqlserver.SqlServerConnectorTask'
            )
        })
    }, minimum=1)

    (kafka_connect_pact
     .given('connector sqlserver-source exists with tasks')
     .upon_receiving('a request for connector tasks')
     .with_request('GET', '/connectors/sqlserver-source/tasks')
     .will_respond_with(200, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(
            f'{kafka_connect_pact.uri}/connectors/sqlserver-source/tasks'
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1


def test_get_connector_plugins(kafka_connect_pact):
    """
    Test GET /connector-plugins returns available plugins.

    Contract: Plugins endpoint should return array of plugin info.
    """
    expected_response = EachLike({
        'class': Term(r'[\w\.]+Connector', 'io.debezium.connector.sqlserver.SqlServerConnector'),
        'type': Like('source'),
        'version': Term(r'\d+\.\d+\.\d+', '2.4.0.Final')
    }, minimum=1)

    (kafka_connect_pact
     .given('connector plugins are installed')
     .upon_receiving('a request for available connector plugins')
     .with_request('GET', '/connector-plugins')
     .will_respond_with(200, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(f'{kafka_connect_pact.uri}/connector-plugins')

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        for plugin in data:
            assert 'class' in plugin
            assert 'type' in plugin
            assert plugin['type'] in ['source', 'sink', 'transformation']


def test_post_connector_restart(kafka_connect_pact):
    """
    Test POST /connectors/{name}/restart triggers restart.

    Contract: Restart endpoint should accept POST and return 204 or 202.
    """
    (kafka_connect_pact
     .given('connector sqlserver-source exists')
     .upon_receiving('a request to restart connector')
     .with_request('POST', '/connectors/sqlserver-source/restart')
     .will_respond_with(204))

    with kafka_connect_pact:
        response = requests.post(
            f'{kafka_connect_pact.uri}/connectors/sqlserver-source/restart'
        )

        assert response.status_code in [204, 202]


def test_get_root_info(kafka_connect_pact):
    """
    Test GET / returns Kafka Connect server info.

    Contract: Root endpoint should return version and commit info.
    """
    expected_response = {
        'version': Term(r'\d+\.\d+\.\d+', '3.5.1'),
        'commit': Like('1c42a0c37ed62eda'),
        'kafka_cluster_id': Like('lkc-12345')
    }

    (kafka_connect_pact
     .given('Kafka Connect is running')
     .upon_receiving('a request for server info')
     .with_request('GET', '/')
     .will_respond_with(200, body=expected_response))

    with kafka_connect_pact:
        response = requests.get(f'{kafka_connect_pact.uri}/')

        assert response.status_code == 200
        data = response.json()
        assert 'version' in data
        assert 'commit' in data


@pytest.mark.skipif(not PACT_AVAILABLE, reason="pact-python not installed")
def test_pact_verification_instructions():
    """
    Placeholder test to show how to verify contracts.

    To verify these contracts on the provider side:

    1. Publish pacts to Pact Broker (optional):
       ```
       pact-broker publish pacts/ --consumer-app-version=1.0.0 \\
           --broker-base-url=http://pact-broker:9292
       ```

    2. Verify on provider side:
       ```
       pytest tests/provider/test_kafka_connect_provider.py
       ```

    3. Or use pact-verifier CLI:
       ```
       pact-verifier --provider-base-url=http://localhost:8083 \\
           --pact-url=pacts/reconciliation-service-kafka-connect-api.json
       ```
    """
    assert PACT_DIR.exists()
    print(f"Pact contracts will be written to: {PACT_DIR}")

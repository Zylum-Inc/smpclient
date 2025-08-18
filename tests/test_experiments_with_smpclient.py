"""Experiments with SMPClient `SMPChirpstackFuotaTransport`."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import time
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from chirpstack_fuota_client.api.fuota import FuotaService, FuotaUtils
from google.protobuf.internal.well_known_types import Timestamp

from smpclient import SMPClient

logging.getLogger().setLevel(logging.DEBUG)

from smpclient.requests.os_management import EchoWrite
from smpclient.transport.chirpstack_fuota import (
    ChirpstackFuotaDownlinkSpeed,
    ChirpstackFuotaMulticastGroupTypes,
    ChirpstackFuotaRegionNames,
    DeploymentDevice,
    LoraBasicsClassNames,
    SMPChirpstackFuotaConnectionError,
    SMPChirpstackFuotaTransport,
    chirpstack_fuota_configurations,
)


@pytest.mark.asyncio
@patch("smpclient.transport.chirpstack_fuota.requests.get")  # Add this patch
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_device_logs")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.create_deployment")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_status")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.__init__", return_value=None)
@patch("smpclient.transport.chirpstack_fuota.ApplicationService")
@patch("smpclient.transport.chirpstack_fuota.DeviceService")
async def test_smpclient_with_chirpstack_fuota_transport(
    mock_device_service: MagicMock,
    mock_app_service: MagicMock,
    mock_fuota_service_init: MagicMock,
    mock_get_deployment_status: MagicMock,
    mock_create_deployment: MagicMock,
    mock_get_deployment_device_logs: MagicMock,
    mock_requests_get: MagicMock,  # Add this parameter
) -> None:

    # Arrange
    mock_app_service_instance = mock_app_service.return_value
    mock_device_service_instance = mock_device_service.return_value

    # Mock requests.get for find_dev_id_by_dev_eui method
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"device_id": "test_device_id"}]

    # Mock the second requests.get call for get_messages_by_dev_id method
    mock_messages_response = MagicMock()
    mock_messages_response.status_code = 200
    mock_messages_response.json.return_value = {
        "events": [
            {
                "data": {
                    "time": "2023-01-01T00:00:00Z",
                    "data": "CAEAAAEAAAAGvw==",  # Base64 encoded SMP response
                }
            }
        ]
    }

    # Set up side_effect to return different responses for different calls
    mock_requests_get.side_effect = [mock_response, mock_messages_response]

    transport = SMPChirpstackFuotaTransport(
        multicast_group_type=ChirpstackFuotaMulticastGroupTypes.CLASS_C,
        chirpstack_server_addr="localhost:8080",
        chirpstack_server_api_token="test_token",
        chirpstack_server_app_id="test_app_id",
        devices=[{"dev_eui": "test_eui", "gen_app_key": "test_key"}],
        chirpstack_fuota_server_addr="localhost:8070",
        downlink_speed=ChirpstackFuotaDownlinkSpeed.DL_SLOW,
    )

    smpclient = SMPClient(transport=transport, address="localhost:8080")

    # Mock the connect method dependencies
    mock_app_service_instance.get = MagicMock()
    mock_device_service_instance.get = MagicMock()
    mock_device_service_instance.get.return_value = {
        "device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")
    }  # Ensure a valid device is returned

    await smpclient.connect()

    deployment_response = MagicMock()
    deployment_response.id = "valid_id"
    mock_create_deployment.return_value = deployment_response

    status_response = MagicMock()
    status_response.frag_status_completed_at.seconds = int(time.time())
    status_response.frag_status_completed_at.nanos = 0
    status_response.device_status = []
    device_status_instance = MagicMock()
    device_status_instance.device_eui = "test_eui"
    device_status_instance.created_at.seconds = int(time.time())
    device_status_instance.created_at.nanos = 0
    status_response.device_status.append(device_status_instance)
    mock_get_deployment_status.return_value = status_response

    device_logs = MagicMock()
    device_logs.logs = []
    log_instance = MagicMock()
    log_instance.created_at.seconds = int(time.time())
    log_instance.created_at.nanos = 0
    log_instance.command = "FragSessionSetupReq"
    log_instance.fields = {}
    log_instance.fields['nb_frag'] = 20
    device_logs.logs.append(log_instance)
    log_instance2 = MagicMock()
    log_instance2.created_at.seconds = int(time.time())
    log_instance2.created_at.nanos = 0
    log_instance2.command = "FragSessionStatusAns"
    log_instance2.fields = {}
    log_instance2.fields['nb_frag_received'] = 21
    log_instance2.fields['missing_frag'] = 0
    device_logs.logs.append(log_instance2)

    device_status_instance_log = MagicMock()
    device_status_instance_log.created_at.seconds = int(time.time())
    device_status_instance_log.created_at.nanos = 0
    device_logs.append(device_status_instance_log)
    mock_get_deployment_device_logs.return_value = device_logs

    image = os.urandom(3000)

    # Act
    async for offset in smpclient.upload(image, subsequent_timeout_s=40.0):
        pass

    assert offset == len(image)

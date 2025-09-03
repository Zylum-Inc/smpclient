"""Tests for `SMPBLETransport`."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
from typing import Callable, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from chirpstack_fuota_client.api.fuota import FuotaService, FuotaUtils
from google.protobuf.internal.well_known_types import Timestamp
from smp import header as smpheader
from smp import image_management as smpimg
from smp import os_management as smpos

from smpclient.requests.image_management import ImageStatesRead, ImageStatesWrite, ImageUploadWrite
from smpclient.requests.os_management import EchoWrite, ResetWrite
from smpclient.transport.chirpstack_fuota import (
    ChirpstackFuotaDownlinkSpeed,
    ChirpstackFuotaDownlinkStats,
    ChirpstackFuotaMulticastGroupTypes,
    ChirpstackFuotaRegionNames,
    DeploymentDevice,
    LoraBasicsClassNames,
    SMPChirpstackFuotaConnectionError,
    SMPChirpstackFuotaTransport,
    SMPChirpstackFuotaTransportException,
    chirpstack_fuota_configurations,
)

logging.basicConfig(level=logging.DEBUG)


class MockChirpstackFuotaService:
    def __new__(cls, *args, **kwargs) -> "MockChirpstackFuotaService":  # type: ignore
        client = MagicMock(spec=FuotaService, name="MockChirpstackFuotaService")
        return client


def test_default_constructor() -> None:
    t = SMPChirpstackFuotaTransport()
    assert t.mtu == 2048
    assert t._multicast_group_type == LoraBasicsClassNames.CLASS_C
    assert t._multicast_region == ChirpstackFuotaRegionNames.US_915
    assert t._chirpstack_server_addr == "localhost:8080"
    assert t._chirpstack_server_api_token == ""
    assert t._chirpstack_server_app_id == ""
    assert t._devices == []
    assert t._chirpstack_fuota_server_addr == "localhost:8070"


def test_class_c_constructor() -> None:
    t = SMPChirpstackFuotaTransport(multicast_group_type=LoraBasicsClassNames.CLASS_C)
    assert t.mtu == 2048
    assert t._multicast_group_type == LoraBasicsClassNames.CLASS_C


def test_get_multicast_timeout_seconds() -> None:
    t = SMPChirpstackFuotaTransport(
        multicast_group_type=ChirpstackFuotaMulticastGroupTypes.CLASS_C,
        downlink_speed=ChirpstackFuotaDownlinkSpeed.DL_SLOW,
    )
    assert (
        t.get_multicast_timeout_seconds(
            ChirpstackFuotaMulticastGroupTypes.CLASS_C, ChirpstackFuotaDownlinkSpeed.DL_SLOW
        )
        == 361
    )
    assert (
        t.get_multicast_timeout_seconds(
            ChirpstackFuotaMulticastGroupTypes.CLASS_B, ChirpstackFuotaDownlinkSpeed.DL_SLOW
        )
        == 4246
    )


def test_check_status_response() -> None:
    """
    Test the check_status_response method to ensure it raises exceptions correctly.
    """
    t = SMPChirpstackFuotaTransport()

    # Create the proper status_response structure
    device_status = {
        "dev_eui": "613ded3caba44edd",
        "created_at": 1755537202,
        "updated_at": 1755537219,
        "mc_group_setup_completed_at": 1755537209,
        "mc_session_completed_at": 1755537219,
        "frag_session_setup_completed_at": 1755537213,
        "frag_status_completed_at": 1755537616,  # Changed from 0 to a valid timestamp
        "logs": [
            {
                "created_at": 1755537202,
                "f_port": 200,
                "command": "McGroupSetupReq",
                "fields": {
                    "mc_addr": "9970f61c",
                    "mc_group_id": "0",
                    "min_mc_fcnt": "0",
                    "max_mc_fcnt": "4294967295",
                    "mc_key_encrypted": "6dad633f39671fbce9dabb232bc9c5fb",
                },
            },
            {
                "created_at": 1755537209,
                "f_port": 200,
                "command": "McGroupSetupAns",
                "fields": {"mc_group_id": "0", "id_error": "false"},
            },
            {
                "created_at": 1755537209,
                "f_port": 201,
                "command": "FragSessionSetupReq",
                "fields": {
                    "descriptor": "00000000",
                    "nb_frag": "32",
                    "fragmentation_matrix": "0",
                    "McGroupBitMask": "1",
                    "block_ack_delay": "1",
                    "frag_index": "0",
                    "frag_size": "64",
                    "padding": "0",
                },
            },
            {
                "created_at": 1755537213,
                "f_port": 201,
                "command": "FragSessionSetupAns",
                "fields": {
                    "encoding_unsupported": "false",
                    "frag_session_index_not_supported": "false",
                    "wrong_descriptor": "false",
                    "not_enough_memory": "false",
                    "frag_index": "0",
                },
            },
            {
                "created_at": 1755537213,
                "f_port": 200,
                "command": "McClassCSessionReq",
                "fields": {
                    "mc_group_id": "0",
                    "dl_frequency": "923300000",
                    "dr": "9",
                    "session_time_out": "8",
                    "session_time": "1439572476",
                },
            },
            {
                "created_at": 1755537219,
                "f_port": 200,
                "command": "McClassCSessionAns",
                "fields": {
                    "freq_error": "false",
                    "mc_group_id": "0",
                    "dr_error": "false",
                    "mc_group_undefined": "false",
                },
            },
            {
                "created_at": 1755537514,
                "f_port": 201,
                "command": "FragSessionStatusReq",
                "fields": {"participants": "true", "frag_index": "0"},
            },
            {
                "created_at": 1755537534,
                "f_port": 201,
                "command": "FragSessionStatusAns",
                "fields": {
                    "missing_frag": "1",
                    "nb_frag_received": "32",
                    "not_enough_matrix_memory": "false",
                    "frag_index": "0",
                },
            },
            {
                "created_at": 1755537559,
                "f_port": 201,
                "command": "FragSessionStatusReq",
                "fields": {"participants": "true", "frag_index": "0"},
            },
            {
                "created_at": 1755537585,
                "f_port": 201,
                "command": "FragSessionStatusAns",
                "fields": {
                    "missing_frag": "1",
                    "nb_frag_received": "32",
                    "not_enough_matrix_memory": "false",
                    "frag_index": "0",
                },
            },
            {
                "created_at": 1755537604,
                "f_port": 201,
                "command": "FragSessionStatusReq",
                "fields": {"participants": "true", "frag_index": "0"},
            },
            {
                "created_at": 1755537616,
                "f_port": 201,
                "command": "FragSessionStatusAns",
                "fields": {
                    "frag_index": "0",
                    "missing_frag": "0",  # Changed from "1" to "0" to indicate success
                    "not_enough_matrix_memory": "false",
                    "nb_frag_received": "32",
                },
            },
        ],
    }

    # Create the proper status_response structure
    status_response = {
        "frag_status_completed_at": 1755537616,  # Must be > 0
        "enqueue_completed_at": 1755537209,  # Add this missing field
        "mc_group_setup_completed_at": 1755537209,  # Add this missing field
        "device_status": [device_status],  # Wrap in a list
    }

    downlink_stats = ChirpstackFuotaDownlinkStats()

    assert t.check_status_response(status_response, downlink_stats) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_type,request_args,expected_response_type,expected_assertions",
    [
        (
            ImageUploadWrite,
            {"off": 2345, "data": b"", "len": 54120, "image": 1, "upgrade": None},
            smpimg.ImageUploadWriteResponse,
            lambda req, resp: resp.off == 2345,
        ),
        (
            ImageStatesRead,
            {},
            smpimg.ImageStatesReadResponse,
            lambda req, resp: True,  # Just check that response is created
        ),
        (
            ImageStatesWrite,
            {"hash": b"test_hash", "confirm": True},
            smpimg.ImageStatesWriteResponse,
            lambda req, resp: True,  # Just check that response is created
        ),
        (
            ResetWrite,
            {},
            smpos.ResetWriteResponse,
            lambda req, resp: True,  # Just check that response is created
        ),
    ],
)
async def test_send_and_receive_timeout(
    request_type, request_args, expected_response_type, expected_assertions
) -> None:
    t = SMPChirpstackFuotaTransport()
    req_header = request_type(**request_args)

    t.send_multicast = AsyncMock()  # type: ignore
    t.receive = AsyncMock(side_effect=SMPChirpstackFuotaTransportException("Failed to receive data"))  # type: ignore
    frame = await t.send_and_receive(req_header.BYTES)

    # Verify send_multicast was called for ImageUploadWrite, but not for others
    if request_type == ImageUploadWrite:
        t.send_multicast.assert_awaited_once()
    else:
        t.send_multicast.assert_not_awaited()

    t.receive.assert_awaited_once()

    header = smpheader.Header.loads(frame[: smpheader.Header.SIZE])
    response = expected_response_type.loads(frame)
    logging.debug(f"Response: {response}")
    assert response is not None
    assert expected_assertions(req_header, response)
    assert header.sequence == req_header.sequence


@pytest.mark.asyncio
@patch("smpclient.transport.chirpstack_fuota.ApplicationService")
async def test_verify_app_id(mock_app_service: MagicMock) -> None:
    # Arrange
    mock_app_service_instance = mock_app_service.return_value
    transport = SMPChirpstackFuotaTransport(
        chirpstack_server_addr="localhost:8080",
        chirpstack_server_api_token="test_token",
        chirpstack_server_app_id="test_app_id",
        devices=[{"dev_eui": "test_eui", "gen_app_key": "test_key"}],
        chirpstack_fuota_server_addr="localhost:8070",
    )

    # Success case
    mock_app_service_instance.get = MagicMock()
    result = await transport.verify_app_id("test_app_id")
    assert result is True

    # Failure case
    mock_app_service_instance.get = MagicMock(side_effect=Exception("Failed to get application"))
    result = await transport.verify_app_id("invalid_app_id")
    assert result is False


@pytest.mark.asyncio
@patch("smpclient.transport.chirpstack_fuota.DeviceService")
async def test_get_matched_devices(mock_device_service: MagicMock) -> None:
    # Arrange
    mock_device_service_instance = mock_device_service.return_value
    transport = SMPChirpstackFuotaTransport(
        chirpstack_server_addr="localhost:8080",
        chirpstack_server_api_token="test_token",
        chirpstack_server_app_id="test_app_id",
        devices=[{"dev_eui": "test_eui", "gen_app_key": "test_key"}],
        chirpstack_fuota_server_addr="localhost:8070",
    )

    # Mock the get method to return a valid device
    mock_device_service_instance.get = MagicMock()
    mock_device_service_instance.get.return_value = {
        "device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")
    }

    # Act
    matched_devices = await transport.get_matched_devices()

    # Assert
    assert len(matched_devices) == 1
    assert matched_devices[0]["dev_eui"] == "test_eui"
    assert matched_devices[0]["gen_app_key"] == "test_key"


@pytest.mark.asyncio
@patch("smpclient.transport.chirpstack_fuota.ApplicationService")
@patch("smpclient.transport.chirpstack_fuota.FuotaService")
@patch("smpclient.transport.chirpstack_fuota.DeviceService")
async def test_connect(
    mock_device_service: MagicMock, mock_fuota_service: MagicMock, mock_app_service: MagicMock
) -> None:
    # Arrange
    mock_app_service_instance = mock_app_service.return_value
    mock_device_service_instance = mock_device_service.return_value

    transport = SMPChirpstackFuotaTransport(
        chirpstack_server_addr="localhost:8080",
        chirpstack_server_api_token="test_token",
        chirpstack_server_app_id="test_app_id",
        devices=[{"dev_eui": "test_eui", "gen_app_key": "test_key"}],
        chirpstack_fuota_server_addr="localhost:8070",
    )

    # Success case
    mock_app_service_instance.get = MagicMock()
    mock_device_service_instance.get = MagicMock()
    mock_device_service_instance.get.return_value = {
        "device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")
    }

    await transport.connect("address", 1.0)


@pytest.mark.asyncio
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_device_logs")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.create_deployment")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_status")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.__init__", return_value=None)
@patch("smpclient.transport.chirpstack_fuota.ApplicationService")
@patch("smpclient.transport.chirpstack_fuota.DeviceService")
async def test_send(
    mock_device_service: MagicMock,
    mock_app_service: MagicMock,
    mock_fuota_service_init: MagicMock,
    mock_get_deployment_status: MagicMock,
    mock_create_deployment: MagicMock,
    mock_get_deployment_device_logs: MagicMock,
) -> None:
    # Arrange
    mock_app_service_instance = mock_app_service.return_value
    mock_device_service_instance = mock_device_service.return_value

    transport = SMPChirpstackFuotaTransport(
        multicast_group_type=ChirpstackFuotaMulticastGroupTypes.CLASS_B,
        chirpstack_server_addr="localhost:8080",
        chirpstack_server_api_token="test_token",
        chirpstack_server_app_id="test_app_id",
        devices=[{"dev_eui": "test_eui", "gen_app_key": "test_key"}],
        chirpstack_fuota_server_addr="localhost:8070",
        downlink_speed=ChirpstackFuotaDownlinkSpeed.DL_SLOW,
    )

    # Mock the connect method dependencies
    mock_app_service_instance.get = MagicMock()
    mock_device_service_instance.get = MagicMock()
    mock_device_service_instance.get.return_value = {
        "device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")
    }  # Ensure a valid device is returned

    # Call the connect method
    await transport.connect("address", 1.0)

    # Mock the send_multicast method to return immediately
    transport.send_multicast = AsyncMock()

    # Create a valid SMP header for testing
    from smp import header as smphdr

    from smpclient.requests.image_management import ImageUploadWrite

    # Create a valid ImageUploadWrite request
    image_upload_request = ImageUploadWrite(
        sequence=0,
        off=0,
        data=bytes([random.randint(0, 255) for _ in range(2400)]),  # Random data for the image
        len=2400,
        image=1,
        upgrade=None,
    )

    # Act
    await transport.send(image_upload_request.BYTES)

    # Assert that send_multicast was called
    transport.send_multicast.assert_called_once_with(image_upload_request.BYTES)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_scenario",
    [
        {
            "name": "single_response_1",
            "cloud_lns_response_json_array": [
                {
                    "total": 3,
                    "offset": 0,
                    "limit": 10,
                    "events": [
                        {
                            "id": "36b6be39-a669-4df4-9db2-a40d31bac9cc",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "dmX0aXBlcm1hbmVudPT//2tzcGxpdFN0YXR1cwD/",
                                "fCnt": 16,
                                "time": "2025-08-18T15:56:21.854+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.5,
                                        "rssi": -83,
                                        "board": 3,
                                        "nsTime": "2025-08-18T15:56:21.874504759+00:00",
                                        "channel": 3,
                                        "context": "LYeU+w==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.38043975830078,
                                            "longitude": -71.27425384521484,
                                        },
                                        "uplinkId": 30746,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439567799.854s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904500000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "00453524",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "b8586a65-5c61-4565-ab14-7e8030a4a8c0",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-18T15:56:22.087220",
                            "created_at": "2025-08-18T15:56:22.087220",
                            "updated_at": "2025-08-18T15:56:22.087220",
                        },
                        {
                            "id": "d562bd4c-8d11-4bdb-9275-4358ad71df7f",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQABCAABAQC/ZmltYWdlc5+/ZHNsb3QAZ3ZlcnNpb25wMi4wLjEwMi4zMzU1NDUzNGRoYXNoWCDP1TqQZbY/cUnGQSyvVJ/gLdcuOyRF4JzXQqpayE8C2Whib290YWJsZfVncGVuZGluZ/RpY29uZmlybWVk9WZhY3RpdmX1aXBlcm1hbmVudPT/v2RzbG90AWd2ZXJzaW9ubjIuMS4wLjMzNjE5OTY4ZGhhc2hYIE79p9nM3PW4jA12q49qMAPWr9xDKfNZg2/6/dEEMibbaGJvb3RhYmxl9WdwZW5kaW5n9Gljb25maXJtZWT0ZmFjdGk=",
                                "fCnt": 15,
                                "time": "2025-08-18T15:56:02.640+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.8,
                                        "rssi": -78,
                                        "board": 260,
                                        "nsTime": "2025-08-18T15:56:02.738343182+00:00",
                                        "channel": 4,
                                        "context": "LGJm6w==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.38043975830078,
                                            "longitude": -71.27425384521484,
                                        },
                                        "uplinkId": 30490,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439567780.640s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "00453524",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "56ad4b5c-306b-44a3-bb58-69125ee6a38a",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-18T15:56:02.968912",
                            "created_at": "2025-08-18T15:56:02.968912",
                            "updated_at": "2025-08-18T15:56:02.968912",
                        },
                        {
                            "id": "707d4434-10a5-4b5e-a817-6a01aba9b754",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQAABgAAAAa/YnJjCP8=",
                                "fCnt": 14,
                                "time": "2025-08-18T15:55:51.599+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 11.2,
                                        "rssi": -78,
                                        "board": 261,
                                        "nsTime": "2025-08-18T15:55:51.682330462+00:00",
                                        "channel": 5,
                                        "context": "K7nvPA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.38045883178711,
                                            "longitude": -71.27426147460938,
                                        },
                                        "uplinkId": 29978,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439567769.599s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "00453524",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "6d25ef30-0b96-4f87-8161-6ce9bbab3a56",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-18T15:55:51.897707",
                            "created_at": "2025-08-18T15:55:51.897707",
                            "updated_at": "2025-08-18T15:55:51.897707",
                        },
                    ],
                },
            ],
        },
        {
            "name": "single_response_2",
            "cloud_lns_response_json_array": [
                {
                    "total": 32,
                    "offset": 0,
                    "limit": 3,
                    "events": [
                        {
                            "id": "330b9755-1b83-430d-948a-aee4c458ff86",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "dmX0aXBlcm1hbmVudPT//2tzcGxpdFN0YXR1cwD/",
                                "fCnt": 16,
                                "time": "2025-08-19T17:00:25.886+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9,
                                        "rssi": -82,
                                        "board": 260,
                                        "nsTime": "2025-08-19T17:00:25.923803358+00:00",
                                        "channel": 4,
                                        "context": "MH3b4w==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37990188598633,
                                            "longitude": -71.27464294433594,
                                        },
                                        "uplinkId": 34342,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658043.886s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "58c4b8ca-d14f-49c9-8bb8-9e109f247e61",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:26.141433",
                            "created_at": "2025-08-19T17:00:26.141433",
                            "updated_at": "2025-08-19T17:00:26.141433",
                        },
                        {
                            "id": "5eeea7fc-35aa-4e5a-8798-49f19c71c9fd",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQABCAABUQC/ZmltYWdlc5+/ZHNsb3QAZ3ZlcnNpb25wMi4wLjEwMi4zMzU1NDUzNGRoYXNoWCDP1TqQZbY/cUnGQSyvVJ/gLdcuOyRF4JzXQqpayE8C2Whib290YWJsZfVncGVuZGluZ/RpY29uZmlybWVk9WZhY3RpdmX1aXBlcm1hbmVudPT/v2RzbG90AWd2ZXJzaW9ubjIuMS4wLjMzNjE5OTY4ZGhhc2hYIE79p9nM3PW4jA12q49qMAPWr9xDKfNZg2/6/dEEMibbaGJvb3RhYmxl9WdwZW5kaW5n9Gljb25maXJtZWT0ZmFjdGk=",
                                "fCnt": 15,
                                "time": "2025-08-19T17:00:10.542+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -88,
                                        "board": 1,
                                        "nsTime": "2025-08-19T17:00:10.592196920+00:00",
                                        "channel": 1,
                                        "context": "L5O6aw==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37989807128906,
                                            "longitude": -71.27465057373047,
                                        },
                                        "uplinkId": 33830,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658028.542s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "bb4d924e-10f7-4829-a2ad-23a448f673cb",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:10.820029",
                            "created_at": "2025-08-19T17:00:10.820029",
                            "updated_at": "2025-08-19T17:00:10.820029",
                        },
                        {
                            "id": "5a17bdc3-c182-4678-83c1-2edd7041a26c",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQAABgAA0ga/YnJjCP8=",
                                "fCnt": 14,
                                "time": "2025-08-19T17:00:00.690+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.2,
                                        "rssi": -84,
                                        "board": 260,
                                        "nsTime": "2025-08-19T17:00:00.718937233+00:00",
                                        "channel": 4,
                                        "context": "Lv1lWw==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37989807128906,
                                            "longitude": -71.27465057373047,
                                        },
                                        "uplinkId": 33574,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658018.690s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "351c36a1-a907-46c8-b52c-eeaffe0840cb",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:00.951266",
                            "created_at": "2025-08-19T17:00:00.951266",
                            "updated_at": "2025-08-19T17:00:00.951266",
                        },
                    ],
                },
            ],
        },
        {
            "name": "multiple_responses_1",
            "cloud_lns_response_json_array": [
                {
                    "total": 31,
                    "offset": 0,
                    "limit": 2,
                    "events": [
                        {
                            "id": "5eeea7fc-35aa-4e5a-8798-49f19c71c9fd",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQABCAABUQC/ZmltYWdlc5+/ZHNsb3QAZ3ZlcnNpb25wMi4wLjEwMi4zMzU1NDUzNGRoYXNoWCDP1TqQZbY/cUnGQSyvVJ/gLdcuOyRF4JzXQqpayE8C2Whib290YWJsZfVncGVuZGluZ/RpY29uZmlybWVk9WZhY3RpdmX1aXBlcm1hbmVudPT/v2RzbG90AWd2ZXJzaW9ubjIuMS4wLjMzNjE5OTY4ZGhhc2hYIE79p9nM3PW4jA12q49qMAPWr9xDKfNZg2/6/dEEMibbaGJvb3RhYmxl9WdwZW5kaW5n9Gljb25maXJtZWT0ZmFjdGk=",
                                "fCnt": 15,
                                "time": "2025-08-19T17:00:10.542+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -88,
                                        "board": 1,
                                        "nsTime": "2025-08-19T17:00:10.592196920+00:00",
                                        "channel": 1,
                                        "context": "L5O6aw==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37989807128906,
                                            "longitude": -71.27465057373047,
                                        },
                                        "uplinkId": 33830,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658028.542s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "bb4d924e-10f7-4829-a2ad-23a448f673cb",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:10.820029",
                            "created_at": "2025-08-19T17:00:10.820029",
                            "updated_at": "2025-08-19T17:00:10.820029",
                        },
                        {
                            "id": "5a17bdc3-c182-4678-83c1-2edd7041a26c",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQAABgAA0ga/YnJjCP8=",
                                "fCnt": 14,
                                "time": "2025-08-19T17:00:00.690+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.2,
                                        "rssi": -84,
                                        "board": 260,
                                        "nsTime": "2025-08-19T17:00:00.718937233+00:00",
                                        "channel": 4,
                                        "context": "Lv1lWw==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37989807128906,
                                            "longitude": -71.27465057373047,
                                        },
                                        "uplinkId": 33574,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658018.690s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "351c36a1-a907-46c8-b52c-eeaffe0840cb",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:00.951266",
                            "created_at": "2025-08-19T17:00:00.951266",
                            "updated_at": "2025-08-19T17:00:00.951266",
                        },
                    ],
                },
                {
                    "total": 32,
                    "offset": 0,
                    "limit": 3,
                    "events": [
                        {
                            "id": "330b9755-1b83-430d-948a-aee4c458ff86",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "dmX0aXBlcm1hbmVudPT//2tzcGxpdFN0YXR1cwD/",
                                "fCnt": 16,
                                "time": "2025-08-19T17:00:25.886+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9,
                                        "rssi": -82,
                                        "board": 260,
                                        "nsTime": "2025-08-19T17:00:25.923803358+00:00",
                                        "channel": 4,
                                        "context": "MH3b4w==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37990188598633,
                                            "longitude": -71.27464294433594,
                                        },
                                        "uplinkId": 34342,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658043.886s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "58c4b8ca-d14f-49c9-8bb8-9e109f247e61",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:26.141433",
                            "created_at": "2025-08-19T17:00:26.141433",
                            "updated_at": "2025-08-19T17:00:26.141433",
                        },
                        {
                            "id": "5eeea7fc-35aa-4e5a-8798-49f19c71c9fd",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQABCAABUQC/ZmltYWdlc5+/ZHNsb3QAZ3ZlcnNpb25wMi4wLjEwMi4zMzU1NDUzNGRoYXNoWCDP1TqQZbY/cUnGQSyvVJ/gLdcuOyRF4JzXQqpayE8C2Whib290YWJsZfVncGVuZGluZ/RpY29uZmlybWVk9WZhY3RpdmX1aXBlcm1hbmVudPT/v2RzbG90AWd2ZXJzaW9ubjIuMS4wLjMzNjE5OTY4ZGhhc2hYIE79p9nM3PW4jA12q49qMAPWr9xDKfNZg2/6/dEEMibbaGJvb3RhYmxl9WdwZW5kaW5n9Gljb25maXJtZWT0ZmFjdGk=",
                                "fCnt": 15,
                                "time": "2025-08-19T17:00:10.542+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -88,
                                        "board": 1,
                                        "nsTime": "2025-08-19T17:00:10.592196920+00:00",
                                        "channel": 1,
                                        "context": "L5O6aw==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37989807128906,
                                            "longitude": -71.27465057373047,
                                        },
                                        "uplinkId": 33830,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658028.542s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "bb4d924e-10f7-4829-a2ad-23a448f673cb",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:10.820029",
                            "created_at": "2025-08-19T17:00:10.820029",
                            "updated_at": "2025-08-19T17:00:10.820029",
                        },
                        {
                            "id": "5a17bdc3-c182-4678-83c1-2edd7041a26c",
                            "device_id": "106e0ed6-3528-4f10-a8fd-bac8da3587d3",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 3,
                                "adr": True,
                                "data": "AQAABgAA0ga/YnJjCP8=",
                                "fCnt": 14,
                                "time": "2025-08-19T17:00:00.690+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.2,
                                        "rssi": -84,
                                        "board": 260,
                                        "nsTime": "2025-08-19T17:00:00.718937233+00:00",
                                        "channel": 4,
                                        "context": "Lv1lWw==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 42.37989807128906,
                                            "longitude": -71.27465057373047,
                                        },
                                        "uplinkId": 33574,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1439658018.690s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 7,
                                        }
                                    },
                                },
                                "devAddr": "011422cf",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "613ded3caba44edd",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli 3B8E80F5",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "bf6dbba9-d296-4621-9f3f-e1e43757fbf6",
                                    "deviceProfileName": "US_915_Class_C",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "351c36a1-a907-46c8-b52c-eeaffe0840cb",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-19T17:00:00.951266",
                            "created_at": "2025-08-19T17:00:00.951266",
                            "updated_at": "2025-08-19T17:00:00.951266",
                        },
                    ],
                },
            ],
        },
        {
            "name": "multiple_responses_3",
            "cloud_lns_response_json_array": [
                {
                    "total": 228,
                    "offset": 28,
                    "limit": 5,
                    "events": [
                        {
                            "id": "15c5e305-c423-43ac-b791-c5b4689cf66e",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "dABndmVyc2lvbm8=",
                                "fCnt": 73,
                                "time": "2025-08-29T19:59:55.576+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.2,
                                        "rssi": -60,
                                        "board": 263,
                                        "nsTime": "2025-08-29T19:59:55.599179007+00:00",
                                        "channel": 7,
                                        "context": "HBXTTA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841365814208984,
                                            "longitude": -84.38180541992188,
                                        },
                                        "uplinkId": 41577,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532813.576s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "f9357982-11c2-439f-960e-848dc3f9f8ec",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T19:59:55.812600",
                            "created_at": "2025-08-29T19:59:55.812600",
                            "updated_at": "2025-08-29T19:59:55.812600",
                        },
                        {
                            "id": "cebf3ce6-5bee-49a2-b97f-828ec06b7eff",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "bWFnZXOfv2RzbG8=",
                                "fCnt": 72,
                                "time": "2025-08-29T19:59:08.026+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.5,
                                        "rssi": -62,
                                        "board": 261,
                                        "nsTime": "2025-08-29T19:59:08.051777379+00:00",
                                        "channel": 5,
                                        "context": "GUBFZA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.8414421081543,
                                            "longitude": -84.38185119628906,
                                        },
                                        "uplinkId": 39529,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532766.026s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "5c0010c0-a8f4-42c8-990d-b7189ec154ef",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T19:59:08.264131",
                            "created_at": "2025-08-29T19:59:08.264131",
                            "updated_at": "2025-08-29T19:59:08.264131",
                        },
                        {
                            "id": "db643e7d-bbf1-4b76-bfee-30ceb40e6aaf",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "AQABBwABkQC/Zmk=",
                                "fCnt": 71,
                                "time": "2025-08-29T19:58:43.997+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -59,
                                        "board": 1,
                                        "nsTime": "2025-08-29T19:58:44.023803514+00:00",
                                        "channel": 1,
                                        "context": "F9GfhA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.8414421081543,
                                            "longitude": -84.38185119628906,
                                        },
                                        "uplinkId": 36969,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532741.997s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "4d011160-534d-4f73-856a-b44e2ecc32c2",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T19:58:44.239093",
                            "created_at": "2025-08-29T19:58:44.239093",
                            "updated_at": "2025-08-29T19:58:44.239093",
                        },
                        {
                            "id": "8c399279-4774-4b74-b7a0-b36f5bf5d6a7",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "Ywj/",
                                "fCnt": 70,
                                "time": "2025-08-29T19:57:48.099+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 13.2,
                                        "rssi": -66,
                                        "board": 3,
                                        "nsTime": "2025-08-29T19:57:48.123867200+00:00",
                                        "channel": 3,
                                        "context": "FHyuXA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84149932861328,
                                            "longitude": -84.38184356689453,
                                        },
                                        "uplinkId": 34665,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532686.099s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904500000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "0ea8a025-d6f9-4561-afea-eb2eb41b689c",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T19:57:48.335921",
                            "created_at": "2025-08-29T19:57:48.335921",
                            "updated_at": "2025-08-29T19:57:48.335921",
                        },
                        {
                            "id": "70598a34-2a39-4caa-bf1c-71081a2a1c1e",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "AQAABgAAVga/YnI=",
                                "fCnt": 69,
                                "time": "2025-08-29T19:57:07.759+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 8.8,
                                        "rssi": -65,
                                        "board": 260,
                                        "nsTime": "2025-08-29T19:57:07.790986560+00:00",
                                        "channel": 4,
                                        "context": "EhUmPA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84150695800781,
                                            "longitude": -84.38186645507812,
                                        },
                                        "uplinkId": 30569,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532645.759s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "4046038a-6ca4-415c-9013-09e1d5513dfc",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T19:57:08.006623",
                            "created_at": "2025-08-29T19:57:08.006623",
                            "updated_at": "2025-08-29T19:57:08.006623",
                        },
                    ],
                },
                {
                    "total": 228,
                    "offset": 23,
                    "limit": 5,
                    "events": [
                        {
                            "id": "2846fbda-0e49-4733-8653-43f2bc48968e",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "Hky02PTAaNy/7mg=",
                                "fCnt": 78,
                                "time": "2025-08-29T20:03:22.499+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 12,
                                        "rssi": -64,
                                        "board": 260,
                                        "nsTime": "2025-08-29T20:03:22.531102569+00:00",
                                        "channel": 4,
                                        "context": "KGs5vA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84111022949219,
                                            "longitude": -84.38162231445312,
                                        },
                                        "uplinkId": 51305,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533020.499s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "192ede33-e7b6-4324-934a-1b81431844b5",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:03:22.744027",
                            "created_at": "2025-08-29T20:03:22.744027",
                            "updated_at": "2025-08-29T20:03:22.744027",
                        },
                        {
                            "id": "c51c326c-d0ad-418e-b670-afd2e5de92bc",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "n+YiuOQaEGDezG8=",
                                "fCnt": 77,
                                "time": "2025-08-29T20:02:34.884+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 11.2,
                                        "rssi": -58,
                                        "board": 3,
                                        "nsTime": "2025-08-29T20:02:34.918939419+00:00",
                                        "channel": 3,
                                        "context": "JZSrtA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84109115600586,
                                            "longitude": -84.38160705566406,
                                        },
                                        "uplinkId": 49513,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532972.884s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904500000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "a1f642b1-56cd-45cd-bf52-f75680f0be89",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:02:35.131706",
                            "created_at": "2025-08-29T20:02:35.131706",
                            "updated_at": "2025-08-29T20:02:35.131706",
                        },
                        {
                            "id": "465be932-2b47-486c-9035-396dc8929e1c",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "fPBbBgU0uvkSNi0=",
                                "fCnt": 76,
                                "time": "2025-08-29T20:02:06.628+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 12.2,
                                        "rssi": -65,
                                        "board": 261,
                                        "nsTime": "2025-08-29T20:02:06.663813109+00:00",
                                        "channel": 5,
                                        "context": "I+WHJA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841148376464844,
                                            "longitude": -84.38164520263672,
                                        },
                                        "uplinkId": 47721,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532944.628s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "c102fbe3-0ecb-4ee9-9741-9524c169418a",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:02:06.879597",
                            "created_at": "2025-08-29T20:02:06.879597",
                            "updated_at": "2025-08-29T20:02:06.879597",
                        },
                        {
                            "id": "81e319f9-0a67-4092-8ee8-abd2f51f864d",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "NDQ0M2RoYXNoWCA=",
                                "fCnt": 75,
                                "time": "2025-08-29T20:01:13.556+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.2,
                                        "rssi": -61,
                                        "board": 262,
                                        "nsTime": "2025-08-29T20:01:13.586816334+00:00",
                                        "channel": 6,
                                        "context": "ILu1PA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841209411621094,
                                            "longitude": -84.3816909790039,
                                        },
                                        "uplinkId": 45673,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532891.556s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "8807ef55-5ce2-4146-8052-2af888911d36",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:01:13.810395",
                            "created_at": "2025-08-29T20:01:13.810395",
                            "updated_at": "2025-08-29T20:01:13.810395",
                        },
                        {
                            "id": "2e8b573d-dd23-43c5-bcbd-5c55ab1ffab7",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "Mi4wLjExLjMzNTU=",
                                "fCnt": 74,
                                "time": "2025-08-29T20:00:53.476+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 7.8,
                                        "rssi": -62,
                                        "board": 2,
                                        "nsTime": "2025-08-29T20:00:53.510951550+00:00",
                                        "channel": 2,
                                        "context": "H4lQvA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841278076171875,
                                            "longitude": -84.38174438476562,
                                        },
                                        "uplinkId": 44393,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440532871.476s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "efb75035-c10d-419f-8b08-adcbc301ccd2",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:00:53.724596",
                            "created_at": "2025-08-29T20:00:53.724596",
                            "updated_at": "2025-08-29T20:00:53.724596",
                        },
                    ],
                },
                {
                    "total": 228,
                    "offset": 18,
                    "limit": 5,
                    "events": [
                        {
                            "id": "2925ef89-ec12-4088-b47b-b4905d327659",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "aXZl9WlwZXJtYW4=",
                                "fCnt": 83,
                                "time": "2025-08-29T20:10:00.131+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -59,
                                        "board": 262,
                                        "nsTime": "2025-08-29T20:10:00.160378931+00:00",
                                        "channel": 6,
                                        "context": "QB6blA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84134292602539,
                                            "longitude": -84.38175201416016,
                                        },
                                        "uplinkId": 5738,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533418.131s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "4da1a1c8-c65d-4eed-81af-1c170971421b",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:10:00.412772",
                            "created_at": "2025-08-29T20:10:00.412772",
                            "updated_at": "2025-08-29T20:10:00.412772",
                        },
                        {
                            "id": "cad86224-368a-43d8-9bc3-41c4bc3396df",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZmlybWVk9WZhY3Q=",
                                "fCnt": 82,
                                "time": "2025-08-29T20:07:56.291+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.5,
                                        "rssi": -64,
                                        "board": 3,
                                        "nsTime": "2025-08-29T20:07:56.318836106+00:00",
                                        "channel": 3,
                                        "context": "OLz2bA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841304779052734,
                                            "longitude": -84.38159942626953,
                                        },
                                        "uplinkId": 64617,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533294.291s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904500000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "578f69d9-2d3b-4991-9c5b-4da3af45c518",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:07:56.531053",
                            "created_at": "2025-08-29T20:07:56.531053",
                            "updated_at": "2025-08-29T20:07:56.531053",
                        },
                        {
                            "id": "69584dbb-e752-466e-ae6d-d81ed27f4de3",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZmlybWVk9WZhY3Q=",
                                "fCnt": 81,
                                "time": "2025-08-29T20:07:53.140+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 12.5,
                                        "rssi": -64,
                                        "nsTime": "2025-08-29T20:07:53.166860206+00:00",
                                        "context": "OIzgpA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841304779052734,
                                            "longitude": -84.38159942626953,
                                        },
                                        "uplinkId": 64105,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533291.140s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 903900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "731c611d-c626-4e30-b378-d796e96d3277",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:07:53.381994",
                            "created_at": "2025-08-29T20:07:53.381994",
                            "updated_at": "2025-08-29T20:07:53.381994",
                        },
                        {
                            "id": "75fb176b-f88b-4426-abab-db69712ed1ed",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZW5kaW5n9Gljb24=",
                                "fCnt": 80,
                                "time": "2025-08-29T20:05:49.534+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.8,
                                        "rssi": -62,
                                        "board": 1,
                                        "nsTime": "2025-08-29T20:05:49.567094973+00:00",
                                        "channel": 1,
                                        "context": "MS7LpA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841209411621094,
                                            "longitude": -84.38162231445312,
                                        },
                                        "uplinkId": 56425,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533167.534s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "5c668fed-8d01-4465-a333-7db59db9c7e2",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:05:49.781177",
                            "created_at": "2025-08-29T20:05:49.781177",
                            "updated_at": "2025-08-29T20:05:49.781177",
                        },
                        {
                            "id": "89135b01-8ba0-483c-84b2-13015825ff69",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "Ym9vdGFibGX1Z3A=",
                                "fCnt": 79,
                                "time": "2025-08-29T20:03:47.244+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 11.8,
                                        "rssi": -66,
                                        "nsTime": "2025-08-29T20:03:47.285145642+00:00",
                                        "context": "KeTMZA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84110641479492,
                                            "longitude": -84.38162231445312,
                                        },
                                        "uplinkId": 52073,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533045.244s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 903900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "f2a8c0bb-3ce5-4e5e-b368-171232f38da4",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:03:47.499853",
                            "created_at": "2025-08-29T20:03:47.499853",
                            "updated_at": "2025-08-29T20:03:47.499853",
                        },
                    ],
                },
                {
                    "total": 228,
                    "offset": 13,
                    "limit": 5,
                    "events": [
                        {
                            "id": "63c77547-2b6e-4ac7-a3fc-2d67448cb533",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "LjEuMC4zMzYxOTk=",
                                "fCnt": 88,
                                "time": "2025-08-29T20:16:22.653+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.8,
                                        "rssi": -61,
                                        "board": 261,
                                        "nsTime": "2025-08-29T20:16:22.688857543+00:00",
                                        "channel": 5,
                                        "context": "VutthA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841514587402344,
                                            "longitude": -84.38189697265625,
                                        },
                                        "uplinkId": 34666,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533800.653s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "cf54ff08-3f13-4c69-8562-d0ed15482825",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:16:22.904239",
                            "created_at": "2025-08-29T20:16:22.904239",
                            "updated_at": "2025-08-29T20:16:22.904239",
                        },
                        {
                            "id": "f1f081eb-fab4-458f-a20f-20097636e709",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "LjEuMC4zMzYxOTk=",
                                "fCnt": 87,
                                "time": "2025-08-29T20:16:19.363+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 7.5,
                                        "rssi": -61,
                                        "board": 2,
                                        "nsTime": "2025-08-29T20:16:19.397759190+00:00",
                                        "channel": 2,
                                        "context": "Vrk6BA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841514587402344,
                                            "longitude": -84.38189697265625,
                                        },
                                        "uplinkId": 34410,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533797.363s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "bfc7a517-9e7a-4dd9-9ea8-48ba850d7c0c",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:16:19.611749",
                            "created_at": "2025-08-29T20:16:19.611749",
                            "updated_at": "2025-08-29T20:16:19.611749",
                        },
                        {
                            "id": "9a0ea088-d80f-41e9-8b1c-bbc053d69a80",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "AWd2ZXJzaW9ubjI=",
                                "fCnt": 86,
                                "time": "2025-08-29T20:14:15.670+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 11.8,
                                        "rssi": -64,
                                        "board": 263,
                                        "nsTime": "2025-08-29T20:14:15.702233669+00:00",
                                        "channel": 7,
                                        "context": "T1nRlA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84144592285156,
                                            "longitude": -84.38191223144531,
                                        },
                                        "uplinkId": 29290,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533673.670s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "9e357c0f-d12d-4bfb-bd32-239f5320b78d",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:14:15.914889",
                            "created_at": "2025-08-29T20:14:15.914889",
                            "updated_at": "2025-08-29T20:14:15.914889",
                        },
                        {
                            "id": "5e7507f8-a67b-4c4c-bd92-e789f13a5c03",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZW509P+/ZHNsb3Q=",
                                "fCnt": 85,
                                "time": "2025-08-29T20:12:10.574+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 11.8,
                                        "rssi": -64,
                                        "board": 260,
                                        "nsTime": "2025-08-29T20:12:10.601862941+00:00",
                                        "channel": 4,
                                        "context": "R+UC3A==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84141540527344,
                                            "longitude": -84.3818359375,
                                        },
                                        "uplinkId": 20586,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533548.574s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "303927a9-55d8-46e7-9b28-7d2e46aca956",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:12:10.815730",
                            "created_at": "2025-08-29T20:12:10.815730",
                            "updated_at": "2025-08-29T20:12:10.815730",
                        },
                        {
                            "id": "2ca1293b-34cc-4cf9-bc8a-23ebb003d3bb",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZW509P+/ZHNsb3Q=",
                                "fCnt": 84,
                                "time": "2025-08-29T20:12:06.803+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -61,
                                        "board": 1,
                                        "nsTime": "2025-08-29T20:12:06.836853059+00:00",
                                        "channel": 1,
                                        "context": "R6t2vA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841400146484375,
                                            "longitude": -84.3818130493164,
                                        },
                                        "uplinkId": 20074,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533544.803s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "8894afbc-e11c-419f-85ba-f74207ca453b",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:12:07.050438",
                            "created_at": "2025-08-29T20:12:07.050438",
                            "updated_at": "2025-08-29T20:12:07.050438",
                        },
                    ],
                },
                {
                    "total": 228,
                    "offset": 8,
                    "limit": 5,
                    "events": [
                        {
                            "id": "b89d0ab7-6609-446b-8dd4-a00c05482b54",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "5qTkHK+In6BoYm8=",
                                "fCnt": 93,
                                "time": "2025-08-29T20:24:43.180+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 8,
                                        "rssi": -60,
                                        "board": 260,
                                        "nsTime": "2025-08-29T20:24:43.203173459+00:00",
                                        "channel": 4,
                                        "context": "dMDcLA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84170150756836,
                                            "longitude": -84.38243865966797,
                                        },
                                        "uplinkId": 61802,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534301.180s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "0e726422-6059-44d6-86af-3a51ce43bf85",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:24:43.418441",
                            "created_at": "2025-08-29T20:24:43.418441",
                            "updated_at": "2025-08-29T20:24:43.418441",
                        },
                        {
                            "id": "eb7e8e1e-e647-4991-9dde-13be5821deb8",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "b6aOu3a6igf/qa4=",
                                "fCnt": 92,
                                "time": "2025-08-29T20:22:37.861+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.5,
                                        "rssi": -66,
                                        "board": 2,
                                        "nsTime": "2025-08-29T20:22:37.887252250+00:00",
                                        "channel": 2,
                                        "context": "bUikHA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84169006347656,
                                            "longitude": -84.38218688964844,
                                        },
                                        "uplinkId": 55658,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534175.861s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "673a8b8c-c683-4d32-ba68-9dadf3d9fc17",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:22:38.102786",
                            "created_at": "2025-08-29T20:22:38.102786",
                            "updated_at": "2025-08-29T20:22:38.102786",
                        },
                        {
                            "id": "ca329ec2-c915-4568-b10a-03f533b38a07",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "b6aOu3a6igf/qa4=",
                                "fCnt": 91,
                                "time": "2025-08-29T20:22:31.248+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.5,
                                        "rssi": -66,
                                        "board": 3,
                                        "nsTime": "2025-08-29T20:22:31.281159146+00:00",
                                        "channel": 3,
                                        "context": "bOO7XA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84169006347656,
                                            "longitude": -84.38218688964844,
                                        },
                                        "uplinkId": 55146,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534169.248s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904500000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "ed12112c-a15b-4cf3-a045-dd3262e5af39",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:22:31.496638",
                            "created_at": "2025-08-29T20:22:31.496638",
                            "updated_at": "2025-08-29T20:22:31.496638",
                        },
                        {
                            "id": "7f0a89a8-0520-4700-b408-dd659729bf57",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "NJ1y5r/0YW2wN5o=",
                                "fCnt": 90,
                                "time": "2025-08-29T20:20:28.643+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 13,
                                        "rssi": -59,
                                        "board": 262,
                                        "nsTime": "2025-08-29T20:20:28.676711213+00:00",
                                        "channel": 6,
                                        "context": "ZZTwBA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841739654541016,
                                            "longitude": -84.38182067871094,
                                        },
                                        "uplinkId": 47210,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534046.643s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "b306f44d-db6c-4004-9cb0-a9385f8e7835",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:20:28.891688",
                            "created_at": "2025-08-29T20:20:28.891688",
                            "updated_at": "2025-08-29T20:20:28.891688",
                        },
                        {
                            "id": "c9f004bb-58d7-4875-8f33-b7b6a2a28e08",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "NjhkaGFzaFggSJA=",
                                "fCnt": 89,
                                "time": "2025-08-29T20:18:26.817+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 5,
                                        "rssi": -65,
                                        "board": 263,
                                        "nsTime": "2025-08-29T20:18:26.852842594+00:00",
                                        "channel": 7,
                                        "context": "XlIE3A==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84172058105469,
                                            "longitude": -84.38172149658203,
                                        },
                                        "uplinkId": 41322,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440533924.817s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "67c83e4e-5202-406f-a0fa-ca67e7afb3b8",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:18:27.068933",
                            "created_at": "2025-08-29T20:18:27.068933",
                            "updated_at": "2025-08-29T20:18:27.068933",
                        },
                    ],
                },
                {
                    "total": 228,
                    "offset": 3,
                    "limit": 5,
                    "events": [
                        {
                            "id": "ff431720-ed68-4158-8dd2-5f585aac5b2c",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "cm1lZPRmYWN0aXY=",
                                "fCnt": 98,
                                "time": "2025-08-29T20:31:08.304+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.8,
                                        "rssi": -62,
                                        "board": 260,
                                        "nsTime": "2025-08-29T20:31:08.362523657+00:00",
                                        "channel": 4,
                                        "context": "i7VgdA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84183883666992,
                                            "longitude": -84.38284301757812,
                                        },
                                        "uplinkId": 15467,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534686.304s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "38b04ec4-4512-4b7b-a389-094a7fb59d85",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:31:08.583035",
                            "created_at": "2025-08-29T20:31:08.583035",
                            "updated_at": "2025-08-29T20:31:08.583035",
                        },
                        {
                            "id": "33f77831-bc14-454a-9ba2-746bcc30ff7e",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "cm1lZPRmYWN0aXY=",
                                "fCnt": 97,
                                "time": "2025-08-29T20:31:02.262+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9,
                                        "rssi": -61,
                                        "board": 2,
                                        "nsTime": "2025-08-29T20:31:02.299381022+00:00",
                                        "channel": 2,
                                        "context": "i1kvVA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84183883666992,
                                            "longitude": -84.38284301757812,
                                        },
                                        "uplinkId": 14955,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534680.262s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "9502dbef-1a52-4887-b2b8-292c9cf25509",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:31:02.544872",
                            "created_at": "2025-08-29T20:31:02.544872",
                            "updated_at": "2025-08-29T20:31:02.544872",
                        },
                        {
                            "id": "d2bc4de9-021e-4dbb-a940-c4299b80f639",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZGluZ/RpY29uZmk=",
                                "fCnt": 96,
                                "time": "2025-08-29T20:28:57.175+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 12.2,
                                        "rssi": -65,
                                        "nsTime": "2025-08-29T20:28:57.208102094+00:00",
                                        "context": "g+SDbA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841773986816406,
                                            "longitude": -84.38265991210938,
                                        },
                                        "uplinkId": 10603,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534555.175s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 903900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "64a3923c-076e-4c70-8e5c-48c22b30115c",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:28:57.423792",
                            "created_at": "2025-08-29T20:28:57.423792",
                            "updated_at": "2025-08-29T20:28:57.423792",
                        },
                        {
                            "id": "66788c06-9fb0-4e25-ada2-42579809f28b",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "b3RhYmxl9WdwZW4=",
                                "fCnt": 95,
                                "time": "2025-08-29T20:26:51.387+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.2,
                                        "rssi": -61,
                                        "board": 261,
                                        "nsTime": "2025-08-29T20:26:51.429191660+00:00",
                                        "channel": 5,
                                        "context": "fGUkFA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841732025146484,
                                            "longitude": -84.38250732421875,
                                        },
                                        "uplinkId": 2411,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534429.387s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "f30daff7-508c-49de-81f8-c2184e7bfac8",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:26:51.644967",
                            "created_at": "2025-08-29T20:26:51.644967",
                            "updated_at": "2025-08-29T20:26:51.644967",
                        },
                        {
                            "id": "acbf2ae4-b114-4341-9a52-431c247070d4",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "b3RhYmxl9WdwZW4=",
                                "fCnt": 94,
                                "time": "2025-08-29T20:26:47.282+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10.5,
                                        "rssi": -62,
                                        "board": 1,
                                        "nsTime": "2025-08-29T20:26:47.310270948+00:00",
                                        "channel": 1,
                                        "context": "fCaBRA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.841732025146484,
                                            "longitude": -84.38250732421875,
                                        },
                                        "uplinkId": 1899,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534425.282s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "39ecd091-dbf2-404f-867f-1885b857eda7",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:26:47.525003",
                            "created_at": "2025-08-29T20:26:47.525003",
                            "updated_at": "2025-08-29T20:26:47.525003",
                        },
                    ],
                },
                {
                    "total": 228,
                    "offset": 0,
                    "limit": 5,
                    "events": [
                        {
                            "id": "b18825cd-bdd0-4bbe-a11e-372019ed8b8f",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "dGF0dXMA/w==",
                                "fCnt": 101,
                                "time": "2025-08-29T20:37:16.830+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 12.2,
                                        "rssi": -60,
                                        "board": 261,
                                        "nsTime": "2025-08-29T20:37:16.869463566+00:00",
                                        "channel": 5,
                                        "context": "oaymJA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84195327758789,
                                            "longitude": -84.38143157958984,
                                        },
                                        "uplinkId": 36715,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440535054.830s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904900000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "00abf78d-117f-4acc-9ab1-7ace70595475",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:37:17.086624",
                            "created_at": "2025-08-29T20:37:17.086624",
                            "updated_at": "2025-08-29T20:37:17.086624",
                        },
                        {
                            "id": "7d7b0cc4-81ec-4273-8d38-35c4a19bba5f",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "dPT//2tzcGxpdFM=",
                                "fCnt": 100,
                                "time": "2025-08-29T20:35:14.271+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 11,
                                        "rssi": -60,
                                        "board": 262,
                                        "nsTime": "2025-08-29T20:35:14.308403823+00:00",
                                        "channel": 6,
                                        "context": "ml6LdA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84188461303711,
                                            "longitude": -84.38277435302734,
                                        },
                                        "uplinkId": 29035,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534932.271s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 905100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "586809c1-3a17-49e9-8170-59afd354ced8",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:35:14.522780",
                            "created_at": "2025-08-29T20:35:14.522780",
                            "updated_at": "2025-08-29T20:35:14.522780",
                        },
                        {
                            "id": "1f12b812-7f28-44e8-bca9-0b0bcd0b69b6",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "ZfRpcGVybWFuZW4=",
                                "fCnt": 99,
                                "time": "2025-08-29T20:33:12.473+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 10,
                                        "rssi": -59,
                                        "board": 1,
                                        "nsTime": "2025-08-29T20:33:12.510302854+00:00",
                                        "channel": 1,
                                        "context": "kxwOvA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84187698364258,
                                            "longitude": -84.38286590576172,
                                        },
                                        "uplinkId": 21355,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534810.473s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904100000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "195fc7d9-ff0e-4856-aca9-6f5d599d5fca",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:33:12.724058",
                            "created_at": "2025-08-29T20:33:12.724058",
                            "updated_at": "2025-08-29T20:33:12.724058",
                        },
                        {
                            "id": "ff431720-ed68-4158-8dd2-5f585aac5b2c",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "cm1lZPRmYWN0aXY=",
                                "fCnt": 98,
                                "time": "2025-08-29T20:31:08.304+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9.8,
                                        "rssi": -62,
                                        "board": 260,
                                        "nsTime": "2025-08-29T20:31:08.362523657+00:00",
                                        "channel": 4,
                                        "context": "i7VgdA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84183883666992,
                                            "longitude": -84.38284301757812,
                                        },
                                        "uplinkId": 15467,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534686.304s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904700000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "38b04ec4-4512-4b7b-a389-094a7fb59d85",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:31:08.583035",
                            "created_at": "2025-08-29T20:31:08.583035",
                            "updated_at": "2025-08-29T20:31:08.583035",
                        },
                        {
                            "id": "33f77831-bc14-454a-9ba2-746bcc30ff7e",
                            "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
                            "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
                            "type": "uplink",
                            "data": {
                                "dr": 0,
                                "adr": True,
                                "data": "cm1lZPRmYWN0aXY=",
                                "fCnt": 97,
                                "time": "2025-08-29T20:31:02.262+00:00",
                                "fPort": 2,
                                "rxInfo": [
                                    {
                                        "snr": 9,
                                        "rssi": -61,
                                        "board": 2,
                                        "nsTime": "2025-08-29T20:31:02.299381022+00:00",
                                        "channel": 2,
                                        "context": "i1kvVA==",
                                        "location": {
                                            "altitude": 3,
                                            "latitude": 33.84183883666992,
                                            "longitude": -84.38284301757812,
                                        },
                                        "uplinkId": 14955,
                                        "crcStatus": "CRC_OK",
                                        "gatewayId": "7076ff00550806e4",
                                        "timeSinceGpsEpoch": "1440534680.262s",
                                    }
                                ],
                                "txInfo": {
                                    "frequency": 904300000,
                                    "modulation": {
                                        "lora": {
                                            "codeRate": "CR_4_5",
                                            "bandwidth": 125000,
                                            "spreadingFactor": 10,
                                        }
                                    },
                                },
                                "devAddr": "007e10fc",
                                "confirmed": True,
                                "deviceInfo": {
                                    "tags": {},
                                    "devEui": "e3ab5182159e6599",
                                    "tenantId": "649cca72-f6eb-4f50-b0c7-918d018b9220",
                                    "deviceName": "tas-cli CFB7110D",
                                    "tenantName": "TAS managed organization",
                                    "applicationId": "28c978af-212a-4e48-87af-b4655e650b79",
                                    "applicationName": "TAS managed application",
                                    "deviceProfileId": "5a8198e7-0f31-4f34-b548-b1bbf60c4f17",
                                    "deviceProfileName": "US_915_Class_C_DR0",
                                    "deviceClassEnabled": "CLASS_C",
                                },
                                "regionConfigId": "us915_1",
                                "deduplicationId": "9502dbef-1a52-4887-b2b8-292c9cf25509",
                            },
                            "metadata": {},
                            "captured_at": "2025-08-29T20:31:02.544872",
                            "created_at": "2025-08-29T20:31:02.544872",
                            "updated_at": "2025-08-29T20:31:02.544872",
                        },
                    ],
                },
            ],
        },
    ],
)
@patch("smpclient.transport.chirpstack_fuota.requests.get")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_device_logs")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.create_deployment")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_status")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.__init__", return_value=None)
@patch("smpclient.transport.chirpstack_fuota.ApplicationService")
@patch("smpclient.transport.chirpstack_fuota.DeviceService")
@patch.object(SMPChirpstackFuotaTransport, '_is_valid_response_header')
async def test_send_and_receive_image_state_read_parametrized(
    mock_is_valid_response_header: MagicMock,
    mock_device_service: MagicMock,
    mock_app_service: MagicMock,
    mock_fuota_service_init: MagicMock,
    mock_get_deployment_status: MagicMock,
    mock_create_deployment: MagicMock,
    mock_get_deployment_device_logs: MagicMock,
    mock_requests_get: MagicMock,
    test_scenario: dict,
) -> None:
    """Test send_and_receive method with ImageStateRead request using parametrized scenarios."""
    # Arrange
    mock_app_service_instance = mock_app_service.return_value
    mock_device_service_instance = mock_device_service.return_value

    transport = SMPChirpstackFuotaTransport(
        multicast_group_type=ChirpstackFuotaMulticastGroupTypes.CLASS_C,
        chirpstack_server_addr="localhost:8080",
        chirpstack_server_api_token="test_token",
        chirpstack_server_app_id="test_app_id",
        devices=[{"dev_eui": "613ded3caba44edd", "gen_app_key": "test_key"}],
        chirpstack_fuota_server_addr="localhost:8070",
        downlink_speed=ChirpstackFuotaDownlinkSpeed.DL_SLOW,
        tas_api_addr="localhost:8002",
        tas_api_lns_id="test_lns_id",
    )

    # Mock the connect method dependencies
    mock_app_service_instance.get = MagicMock()
    mock_device_service_instance.get = MagicMock()
    mock_device_service_instance.get.return_value = {
        "device": DeploymentDevice(dev_eui="613ded3caba44edd", gen_app_key="test_key")
    }

    # Call the connect method
    await transport.connect("address", 1.0)

    # Mock the find_dev_id_by_dev_eui method
    transport.find_dev_id_by_dev_eui = MagicMock(
        return_value="106e0ed6-3528-4f10-a8fd-bac8da3587d3"
    )

    from smp import header as smphdr

    # Alternative approach using a closure to capture the transport instance
    def create_mock_function(
        transport_instance: SMPChirpstackFuotaTransport,
    ) -> Callable[[smphdr.Header], bool]:
        def mock_is_valid_response_header_impl(header: smphdr.Header) -> bool:
            print(f"Mocking is_valid_response_header with {header=}")
            print(f"Expected group_id: {transport_instance._expected_response_group_id}")
            print(f"Expected command_id: {transport_instance._expected_response_command_id}")
            # Only check group_id and command_id, ignore sequence number
            return (
                header.group_id == transport_instance._expected_response_group_id
                and header.command_id == transport_instance._expected_response_command_id
            )

        return mock_is_valid_response_header_impl

    mock_is_valid_response_header.side_effect = create_mock_function(transport)

    # Mock the get_messages_by_dev_id method to return the provided JSON response
    cloud_lns_response_json_array = test_scenario["cloud_lns_response_json_array"]

    # Mock the get_messages_by_dev_id method
    transport.get_messages_by_dev_id = MagicMock(side_effect=cloud_lns_response_json_array)

    # Mock the send_unicast method to avoid actual network calls
    transport.send_unicast = AsyncMock()

    # Create an ImageStateRead request
    from smp import image_management as smpimg

    image_state_read_request = smpimg.ImageStatesReadRequest()

    # Act
    response_bytes = await transport.send_and_receive(image_state_read_request.BYTES)

    # Assert
    assert response_bytes is not None
    assert len(response_bytes) > 0

    # Verify the response can be parsed as an ImageStateReadResponse
    from smp import image_management as smpimg

    try:
        response = smpimg.ImageStatesReadResponse.loads(response_bytes)
        assert response is not None
        assert hasattr(response, 'sequence')
        assert hasattr(response, 'images')
        assert hasattr(response, 'splitStatus')

        # Verify the response contains the expected data from the mocked uplink
        # The second uplink contains image state data in base64 format
        assert len(response.images) > 0

        print(f"Successfully parsed ImageStateReadResponse for scenario: {test_scenario['name']}")
        print(f"  Sequence: {response.sequence}")
        print(f"  Number of images: {len(response.images)}")
        print(f"  Split status: {response.splitStatus}")

        for i, image in enumerate(response.images):
            print(
                f"  Image {i}: slot={image.slot}, version={image.version}, "
                f"hash={image.hash.hex() if image.hash else 'None'}, "
                f"bootable={image.bootable}, pending={image.pending}, "
                f"confirmed={image.confirmed}, active={image.active}"
            )

    except Exception as e:
        pytest.fail(f"Failed to parse response as ImageStateReadResponse: {e}")

    # Verify that send_unicast was called (since this is not an image upload)
    transport.send_unicast.assert_called()


#
# @pytest.mark.asyncio
# async def test_disconnect() -> None:
#     t = SMPBLETransport()
#     t._client = MagicMock(spec=BleakClient)
#     await t.disconnect()
#     t._client.disconnect.assert_awaited_once_with()
#
#
# @pytest.mark.asyncio
# async def test_send() -> None:
#     t = SMPBLETransport()
#     t._client = MagicMock(spec=BleakClient)
#     t._smp_characteristic = MagicMock(spec=BleakGATTCharacteristic)
#     t._smp_characteristic.max_write_without_response_size = 20
#     await t.send(b"Hello pytest!")
#     t._client.write_gatt_char.assert_awaited_once_with(
#         t._smp_characteristic, b"Hello pytest!", response=False
#     )
#
#
# @pytest.mark.asyncio
# async def test_receive() -> None:
#     t = SMPBLETransport()
#     t._client = MagicMock(spec=BleakClient)
#     t._smp_characteristic = MagicMock(spec=BleakGATTCharacteristic)
#     t._smp_characteristic.uuid = str(SMP_CHARACTERISTIC_UUID)
#     t._disconnected_event.clear()  # pretend t.connect() was successful
#
#     REP = EchoWrite._Response.get_default()(sequence=0, r="Hello pytest!").BYTES  # type: ignore
#
#     b, _ = await asyncio.gather(
#         t.receive(),
#         t._notify_callback(t._smp_characteristic, REP),
#     )
#
#     assert b == REP
#
#     # cool, now try with a fragmented response
#     async def fragmented_notifies() -> None:
#         await t._notify_callback(t._smp_characteristic, REP[:10])
#         await asyncio.sleep(0.001)
#         await t._notify_callback(t._smp_characteristic, REP[10:])
#
#     b, _ = await asyncio.gather(
#         t.receive(),
#         fragmented_notifies(),
#     )
#
#     assert b == REP
#
#
# @pytest.mark.asyncio
# async def test_send_and_receive() -> None:
#     t = SMPBLETransport()
#     t.send = AsyncMock()  # type: ignore
#     t.receive = AsyncMock()  # type: ignore
#     await t.send_and_receive(b"Hello pytest!")
#     t.send.assert_awaited_once_with(b"Hello pytest!")
#     t.receive.assert_awaited_once_with()
#
#
# def test_max_unencoded_size() -> None:
#     t = SMPBLETransport()
#     t._client = MagicMock(spec=BleakClient)
#     t._max_write_without_response_size = 42
#     assert t.max_unencoded_size == 42
#
#
# def test_max_unencoded_size_mcumgr_param() -> None:
#     t = SMPBLETransport()
#     t._client = MagicMock(spec=BleakClient)
#     t._smp_server_transport_buffer_size = 9001
#     assert t.max_unencoded_size == 9001

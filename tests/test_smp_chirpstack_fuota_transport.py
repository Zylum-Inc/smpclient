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

from smpclient.requests.image_management import ImageUploadWrite
from smpclient.requests.os_management import EchoWrite
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
async def test_send_and_receive_timeout() -> None:
    t = SMPChirpstackFuotaTransport()
    req_header = ImageUploadWrite(off=2345, data=b"", len=54120, image=1, upgrade=None)

    t.send_multicast = AsyncMock()  # type: ignore
    t.receive = AsyncMock(side_effect=SMPChirpstackFuotaTransportException("Failed to receive data"))  # type: ignore
    frame = await t.send_and_receive(req_header.BYTES)
    t.send_multicast.assert_awaited_once()
    t.receive.assert_awaited_once()

    header = smpheader.Header.loads(frame[: smpheader.Header.SIZE])
    response = smpimg.ImageUploadWriteResponse.loads(frame)
    logging.debug(f"Response: {response}")
    assert response is not None
    assert response.off == 2345
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

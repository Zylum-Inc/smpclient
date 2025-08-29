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
        {
            "name": "multiple_responses_3",
            "cloud_lns_response_json_array": [
                {
  "total": 195,
  "offset": 20,
  "limit": 5,
  "events": [
    {
      "id": "71d6aa67-07e6-4b07-afe5-a2c883dad33d",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZW5kaW5n9Gljb24=",
        "fCnt": 47,
        "time": "2025-08-29T18:40:24.579+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13.2,
            "rssi": -67,
            "board": 263,
            "nsTime": "2025-08-29T18:40:24.607567389+00:00",
            "channel": 7,
            "context": "/7Y3hA==",
            "location": {
              "altitude": 3,
              "latitude": 33.8415641784668,
              "longitude": -84.37882995605469
            },
            "uplinkId": 26725,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528042.579s"
          }
        ],
        "txInfo": {
          "frequency": 905300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "0d3d3663-2364-45db-b719-ba0623a4e005"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:40:24.820045",
      "created_at": "2025-08-29T18:40:24.820045",
      "updated_at": "2025-08-29T18:40:24.820045"
    },
    {
      "id": "dcaeeaaf-7ae0-401e-9286-8381a4b82696",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "Ym9vdGFibGX1Z3A=",
        "fCnt": 46,
        "time": "2025-08-29T18:38:18.593+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 11.5,
            "rssi": -63,
            "board": 1,
            "nsTime": "2025-08-29T18:38:18.628882761+00:00",
            "channel": 1,
            "context": "+DPSRA==",
            "location": {
              "altitude": 3,
              "latitude": 33.841712951660156,
              "longitude": -84.37907409667969
            },
            "uplinkId": 18277,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440527916.593s"
          }
        ],
        "txInfo": {
          "frequency": 904100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "bdf49d6d-dc34-4a6e-9578-a69024e7bdea"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:38:18.841287",
      "created_at": "2025-08-29T18:38:18.841287",
      "updated_at": "2025-08-29T18:38:18.841287"
    },
    {
      "id": "eac8c35e-f75a-4e77-a8b6-cc1f7e8e9016",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "Hky02PTAaNy/7mg=",
        "fCnt": 45,
        "time": "2025-08-29T18:37:55.767+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 12.2,
            "rssi": -63,
            "board": 262,
            "nsTime": "2025-08-29T18:37:55.797860316+00:00",
            "channel": 6,
            "context": "9teHDA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84172821044922,
              "longitude": -84.37911987304688
            },
            "uplinkId": 16485,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440527893.767s"
          }
        ],
        "txInfo": {
          "frequency": 905100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "d52a21b3-4b4e-44af-9ba5-d92049633f02"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:37:56.032147",
      "created_at": "2025-08-29T18:37:56.032147",
      "updated_at": "2025-08-29T18:37:56.032147"
    },
    {
      "id": "725bc04d-26ce-4ca8-9694-80f70c865f2f",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "n+YiuOQaEGDezG8=",
        "fCnt": 44,
        "time": "2025-08-29T18:37:00.716+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 11.5,
            "rssi": -65,
            "board": 3,
            "nsTime": "2025-08-29T18:37:00.746162910+00:00",
            "channel": 3,
            "context": "84+A3A==",
            "location": {
              "altitude": 3,
              "latitude": 33.841796875,
              "longitude": -84.37914276123047
            },
            "uplinkId": 13669,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440527838.716s"
          }
        ],
        "txInfo": {
          "frequency": 904500000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "72c57797-be4d-4f39-a6fa-883291875870"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:37:00.963277",
      "created_at": "2025-08-29T18:37:00.963277",
      "updated_at": "2025-08-29T18:37:00.963277"
    },
    {
      "id": "2f622c30-7158-4c90-a378-c3ba4abc4323",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "fPBbBgU0uvkSNi0=",
        "fCnt": 43,
        "time": "2025-08-29T18:36:37.960+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13.8,
            "rssi": -66,
            "board": 260,
            "nsTime": "2025-08-29T18:36:37.994154533+00:00",
            "channel": 4,
            "context": "8jRIFA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84182357788086,
              "longitude": -84.3791275024414
            },
            "uplinkId": 12645,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440527815.960s"
          }
        ],
        "txInfo": {
          "frequency": 904700000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "d397d213-0665-4d69-a25f-9c5553136b12"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:36:38.208739",
      "created_at": "2025-08-29T18:36:38.208739",
      "updated_at": "2025-08-29T18:36:38.208739"
    }
  ]
},
{
  "total": 195,
  "offset": 15,
  "limit": 5,
  "events": [
    {
      "id": "b00cae21-55cc-4eb3-917e-9f60a4df47ae",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZW509P+/ZHNsb3Q=",
        "fCnt": 52,
        "time": "2025-08-29T18:46:55.106+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13.2,
            "rssi": -67,
            "nsTime": "2025-08-29T18:46:55.136732070+00:00",
            "context": "Fv0uTA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840675354003906,
              "longitude": -84.3825912475586
            },
            "uplinkId": 53349,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528433.106s"
          }
        ],
        "txInfo": {
          "frequency": 903900000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "7ffc66c2-2f1c-46ec-aec5-459769f499a7"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:46:55.353449",
      "created_at": "2025-08-29T18:46:55.353449",
      "updated_at": "2025-08-29T18:46:55.353449"
    },
    {
      "id": "422e2435-ee85-4d77-8ac7-e418b64f7370",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZW509P+/ZHNsb3Q=",
        "fCnt": 51,
        "time": "2025-08-29T18:46:48.395+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 9,
            "rssi": -64,
            "board": 2,
            "nsTime": "2025-08-29T18:46:48.431564578+00:00",
            "channel": 2,
            "context": "FpbIJA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840675354003906,
              "longitude": -84.3825912475586
            },
            "uplinkId": 52837,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528426.395s"
          }
        ],
        "txInfo": {
          "frequency": 904300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "88b43a64-8b34-4cc6-a3ea-8494dfa36734"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:46:48.645683",
      "created_at": "2025-08-29T18:46:48.645683",
      "updated_at": "2025-08-29T18:46:48.645683"
    },
    {
      "id": "6b5d887a-d554-49d9-b3ec-5753cc1e2e6e",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "aXZl9WlwZXJtYW4=",
        "fCnt": 50,
        "time": "2025-08-29T18:44:42.989+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13,
            "rssi": -65,
            "board": 263,
            "nsTime": "2025-08-29T18:44:43.022573352+00:00",
            "channel": 7,
            "context": "Dx08lA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84074783325195,
              "longitude": -84.38262176513672
            },
            "uplinkId": 46437,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528300.989s"
          }
        ],
        "txInfo": {
          "frequency": 905300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "11937bc2-a0de-4650-bf7d-1b63284a35b2"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:44:43.237130",
      "created_at": "2025-08-29T18:44:43.237130",
      "updated_at": "2025-08-29T18:44:43.237130"
    },
    {
      "id": "c9c297b2-da5d-4ec4-b322-ea4443c26859",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZmlybWVk9WZhY3Q=",
        "fCnt": 49,
        "time": "2025-08-29T18:42:37.040+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13.2,
            "rssi": -67,
            "board": 1,
            "nsTime": "2025-08-29T18:42:37.072969341+00:00",
            "channel": 1,
            "context": "B5toPA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84148406982422,
              "longitude": -84.37897491455078
            },
            "uplinkId": 36453,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528175.040s"
          }
        ],
        "txInfo": {
          "frequency": 904100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "fc47ea34-67c8-4a76-9802-e2396c08d14b"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:42:37.286372",
      "created_at": "2025-08-29T18:42:37.286372",
      "updated_at": "2025-08-29T18:42:37.286372"
    },
    {
      "id": "e4a8aaae-1d0d-4c11-92de-3bc8c12b58ac",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZmlybWVk9WZhY3Q=",
        "fCnt": 48,
        "time": "2025-08-29T18:42:30.091+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 9.5,
            "rssi": -68,
            "nsTime": "2025-08-29T18:42:30.116888749+00:00",
            "context": "BzFfbA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84148406982422,
              "longitude": -84.37897491455078
            },
            "uplinkId": 36197,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528168.091s"
          }
        ],
        "txInfo": {
          "frequency": 903900000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "bd1da0db-0efc-4126-a03a-f63ae8e3c950"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:42:30.333240",
      "created_at": "2025-08-29T18:42:30.333240",
      "updated_at": "2025-08-29T18:42:30.333240"
    }
  ]
},
{
  "total": 195,
  "offset": 10,
  "limit": 5,
  "events": [
    {
      "id": "bf52adbe-b1a4-4bf9-8f46-4b50aa9a7cad",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "NJ1y5r/0YW2wN5o=",
        "fCnt": 57,
        "time": "2025-08-29T18:55:13.352+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 8.8,
            "rssi": -62,
            "board": 2,
            "nsTime": "2025-08-29T18:55:13.387266849+00:00",
            "channel": 2,
            "context": "NK/OBA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84073257446289,
              "longitude": -84.38249969482422
            },
            "uplinkId": 19302,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528931.352s"
          }
        ],
        "txInfo": {
          "frequency": 904300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "68c285b9-678a-4b51-9439-084694cbb57a"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:55:13.601285",
      "created_at": "2025-08-29T18:55:13.601285",
      "updated_at": "2025-08-29T18:55:13.601285"
    },
    {
      "id": "38b1afde-70a6-478d-95dc-4f748b13395b",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "NjhkaGFzaFggSJA=",
        "fCnt": 56,
        "time": "2025-08-29T18:53:11.273+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 9.5,
            "rssi": -62,
            "board": 3,
            "nsTime": "2025-08-29T18:53:11.309479002+00:00",
            "channel": 3,
            "context": "LWkHFA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84074783325195,
              "longitude": -84.38245391845703
            },
            "uplinkId": 11622,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528809.273s"
          }
        ],
        "txInfo": {
          "frequency": 904500000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "d1204c43-97d3-4df3-8e98-f0eb6008fe7a"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:53:11.523891",
      "created_at": "2025-08-29T18:53:11.523891",
      "updated_at": "2025-08-29T18:53:11.523891"
    },
    {
      "id": "d950ecc2-19bf-42ef-9491-09c1246f9154",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "LjEuMC4zMzYxOTk=",
        "fCnt": 55,
        "time": "2025-08-29T18:51:09.734+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 9,
            "rssi": -59,
            "board": 260,
            "nsTime": "2025-08-29T18:51:09.768696955+00:00",
            "channel": 4,
            "context": "Jip9dA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840728759765625,
              "longitude": -84.3824234008789
            },
            "uplinkId": 1638,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528687.734s"
          }
        ],
        "txInfo": {
          "frequency": 904700000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "8bf88984-2ed0-4eb9-9bc8-f79a6e01c4ae"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:51:09.982220",
      "created_at": "2025-08-29T18:51:09.982220",
      "updated_at": "2025-08-29T18:51:09.982220"
    },
    {
      "id": "3d644bbc-9fbb-49a4-935d-6345d1c69dd9",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "LjEuMC4zMzYxOTk=",
        "fCnt": 54,
        "time": "2025-08-29T18:51:04.981+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 12,
            "rssi": -63,
            "board": 261,
            "nsTime": "2025-08-29T18:51:05.015292266+00:00",
            "channel": 5,
            "context": "JeH3nA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840721130371094,
              "longitude": -84.3824234008789
            },
            "uplinkId": 1126,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528682.981s"
          }
        ],
        "txInfo": {
          "frequency": 904900000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "11bd85a6-33b7-46a6-a71c-869e454e9860"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:51:05.229362",
      "created_at": "2025-08-29T18:51:05.229362",
      "updated_at": "2025-08-29T18:51:05.229362"
    },
    {
      "id": "a7b51502-0728-46ad-ab0f-c932a637d329",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "AWd2ZXJzaW9ubjI=",
        "fCnt": 53,
        "time": "2025-08-29T18:49:00.577+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 9.5,
            "rssi": -60,
            "board": 262,
            "nsTime": "2025-08-29T18:49:00.604044030+00:00",
            "channel": 6,
            "context": "Hne1pA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84063720703125,
              "longitude": -84.38245391845703
            },
            "uplinkId": 58981,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440528558.577s"
          }
        ],
        "txInfo": {
          "frequency": 905100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "581302d9-4760-4728-ab49-419e87af83cc"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:49:00.817744",
      "created_at": "2025-08-29T18:49:00.817744",
      "updated_at": "2025-08-29T18:49:00.817744"
    }
  ]
},
{
  "total": 195,
  "offset": 5,
  "limit": 5,
  "events": [
    {
      "id": "237eaafb-0b19-4471-98e6-0aa3c3cd5116",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZGluZ/RpY29uZmk=",
        "fCnt": 62,
        "time": "2025-08-29T19:03:36.361+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13,
            "rssi": -64,
            "board": 261,
            "nsTime": "2025-08-29T19:03:36.397349484+00:00",
            "channel": 5,
            "context": "UqsdzA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840694427490234,
              "longitude": -84.38259887695312
            },
            "uplinkId": 47462,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529434.361s"
          }
        ],
        "txInfo": {
          "frequency": 904900000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "15ef9501-b90f-4046-9cac-a567f7f91a9e"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:03:36.611654",
      "created_at": "2025-08-29T19:03:36.611654",
      "updated_at": "2025-08-29T19:03:36.611654"
    },
    {
      "id": "283b1ecb-128a-41b4-86b1-737805a3d82f",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "b3RhYmxl9WdwZW4=",
        "fCnt": 61,
        "time": "2025-08-29T19:01:33.096+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 11.8,
            "rssi": -64,
            "board": 263,
            "nsTime": "2025-08-29T19:01:33.132322931+00:00",
            "channel": 7,
            "context": "S1I7jA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840702056884766,
              "longitude": -84.382568359375
            },
            "uplinkId": 38502,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529311.096s"
          }
        ],
        "txInfo": {
          "frequency": 905300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "69fd52e3-2c98-499a-ba86-c1cab7418ad9"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:01:33.348233",
      "created_at": "2025-08-29T19:01:33.348233",
      "updated_at": "2025-08-29T19:01:33.348233"
    },
    {
      "id": "4f4940ea-65dc-47f7-91e9-203fe6ef8708",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "5qTkHK+In6BoYm8=",
        "fCnt": 60,
        "time": "2025-08-29T18:59:28.186+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13,
            "rssi": -65,
            "nsTime": "2025-08-29T18:59:28.227159789+00:00",
            "context": "Q+BDFA==",
            "location": {
              "altitude": 3,
              "latitude": 33.8406867980957,
              "longitude": -84.38256072998047
            },
            "uplinkId": 34662,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529186.186s"
          }
        ],
        "txInfo": {
          "frequency": 903900000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "854a1842-9aac-477f-9cea-7e1c5e064539"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:59:28.443807",
      "created_at": "2025-08-29T18:59:28.443807",
      "updated_at": "2025-08-29T18:59:28.443807"
    },
    {
      "id": "5a5664df-8aa4-4cb9-9177-4c99e5541062",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "5qTkHK+In6BoYm8=",
        "fCnt": 59,
        "time": "2025-08-29T18:59:21.748+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 10,
            "rssi": -61,
            "board": 262,
            "nsTime": "2025-08-29T18:59:21.780219517+00:00",
            "channel": 6,
            "context": "Q34H1A==",
            "location": {
              "altitude": 3,
              "latitude": 33.8406867980957,
              "longitude": -84.38256072998047
            },
            "uplinkId": 33894,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529179.748s"
          }
        ],
        "txInfo": {
          "frequency": 905100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "1df87e28-0c3b-4d05-9b87-26e429acc4e2"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:59:21.995571",
      "created_at": "2025-08-29T18:59:21.995571",
      "updated_at": "2025-08-29T18:59:21.995571"
    },
    {
      "id": "b8631fc9-5a43-482e-b061-37c5435cefd1",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "b6aOu3a6igf/qa4=",
        "fCnt": 58,
        "time": "2025-08-29T18:57:16.474+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 12.2,
            "rssi": -64,
            "board": 3,
            "nsTime": "2025-08-29T18:57:16.502228033+00:00",
            "channel": 3,
            "context": "PAZ/JA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840694427490234,
              "longitude": -84.38250732421875
            },
            "uplinkId": 25446,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529054.474s"
          }
        ],
        "txInfo": {
          "frequency": 904500000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "bb1fba52-ee7e-4e03-bf5c-fe559491d4b4"
      },
      "metadata": {},
      "captured_at": "2025-08-29T18:57:16.716668",
      "created_at": "2025-08-29T18:57:16.716668",
      "updated_at": "2025-08-29T18:57:16.716668"
    }
  ]
},
{
  "total": 195,
  "offset": 0,
  "limit": 5,
  "events": [
    {
      "id": "f6fb318f-208b-47c6-b5ae-671264a21bf6",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "dGF0dXMA/w==",
        "fCnt": 68,
        "time": "2025-08-29T19:12:01.954+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 13.2,
            "rssi": -66,
            "board": 263,
            "nsTime": "2025-08-29T19:12:01.987581626+00:00",
            "channel": 7,
            "context": "cM3ZdA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840763092041016,
              "longitude": -84.3827133178711
            },
            "uplinkId": 21351,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529939.954s"
          }
        ],
        "txInfo": {
          "frequency": 905300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "01b29898-9f94-4345-b6f9-a9d1bc9a3912"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:12:02.228857",
      "created_at": "2025-08-29T19:12:02.228857",
      "updated_at": "2025-08-29T19:12:02.228857"
    },
    {
      "id": "f1c92080-9eb8-4947-b48f-c079cdb6799f",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "dGF0dXMA/w==",
        "fCnt": 67,
        "time": "2025-08-29T19:11:58.194+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 10,
            "rssi": -65,
            "nsTime": "2025-08-29T19:11:58.229749065+00:00",
            "context": "cJR7HA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840763092041016,
              "longitude": -84.3827133178711
            },
            "uplinkId": 21095,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529936.194s"
          }
        ],
        "txInfo": {
          "frequency": 903900000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "68ba5625-1c67-4d66-aece-04d696a2844b"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:11:58.444494",
      "created_at": "2025-08-29T19:11:58.444494",
      "updated_at": "2025-08-29T19:11:58.444494"
    },
    {
      "id": "57b6fe9d-a557-47b6-bb12-0bb417c9d531",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "dPT//2tzcGxpdFM=",
        "fCnt": 66,
        "time": "2025-08-29T19:09:51.527+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 11.2,
            "rssi": -64,
            "board": 262,
            "nsTime": "2025-08-29T19:09:51.556858288+00:00",
            "channel": 6,
            "context": "aQewFA==",
            "location": {
              "altitude": 3,
              "latitude": 33.840728759765625,
              "longitude": -84.38270568847656
            },
            "uplinkId": 6759,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529809.527s"
          }
        ],
        "txInfo": {
          "frequency": 905100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "87447d63-24a1-4715-847b-826d5256f684"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:09:51.768546",
      "created_at": "2025-08-29T19:09:51.768546",
      "updated_at": "2025-08-29T19:09:51.768546"
    },
    {
      "id": "2cda3210-03e0-44ff-b0fd-2738cb87171f",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "ZfRpcGVybWFuZW4=",
        "fCnt": 65,
        "time": "2025-08-29T19:07:47.703+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 7.8,
            "rssi": -60,
            "board": 2,
            "nsTime": "2025-08-29T19:07:47.735548747+00:00",
            "channel": 2,
            "context": "YaZINA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84073257446289,
              "longitude": -84.3827133178711
            },
            "uplinkId": 62054,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529685.703s"
          }
        ],
        "txInfo": {
          "frequency": 904300000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "881ea922-1db5-4d29-961b-b2a4f840a46a"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:07:47.949681",
      "created_at": "2025-08-29T19:07:47.949681",
      "updated_at": "2025-08-29T19:07:47.949681"
    },
    {
      "id": "e9c8a5fa-bc93-4b4f-88ce-e31602d11fa2",
      "device_id": "1ff674b6-ab2c-449a-9ca8-e859aa352ab2",
      "lns_id": "aea1f3d6-bc17-4162-b5c1-ddf71c8811ed",
      "type": "uplink",
      "data": {
        "dr": 0,
        "adr": True,
        "data": "cm1lZPRmYWN0aXY=",
        "fCnt": 64,
        "time": "2025-08-29T19:05:45.275+00:00",
        "fPort": 2,
        "rxInfo": [
          {
            "snr": 11.2,
            "rssi": -61,
            "board": 1,
            "nsTime": "2025-08-29T19:05:45.307473420+00:00",
            "channel": 1,
            "context": "WlovRA==",
            "location": {
              "altitude": 3,
              "latitude": 33.84071350097656,
              "longitude": -84.3826675415039
            },
            "uplinkId": 54374,
            "crcStatus": "CRC_OK",
            "gatewayId": "7076ff00550806e4",
            "timeSinceGpsEpoch": "1440529563.275s"
          }
        ],
        "txInfo": {
          "frequency": 904100000,
          "modulation": {
            "lora": {
              "codeRate": "CR_4_5",
              "bandwidth": 125000,
              "spreadingFactor": 10
            }
          }
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
          "deviceClassEnabled": "CLASS_C"
        },
        "regionConfigId": "us915_1",
        "deduplicationId": "bccd3f78-437c-4eef-910a-6197ee595fcc"
      },
      "metadata": {},
      "captured_at": "2025-08-29T19:05:45.523418",
      "created_at": "2025-08-29T19:05:45.523418",
      "updated_at": "2025-08-29T19:05:45.523418"
    }
  ]
}
            ]
        }
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

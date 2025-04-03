"""Tests for `SMPBLETransport`."""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
import logging

from chirpstack_fuota_client.api.fuota import FuotaService, FuotaUtils
from google.protobuf.internal.well_known_types import Timestamp

from smp import header as smpheader
from smp import image_management as smpimg

from smpclient.requests.image_management import ImageUploadWrite
from smpclient.requests.os_management import EchoWrite
from smpclient.transport.chirpstack_fuota import (
    LoraBasicsClassNames,
    ChirpstackFuotaRegionNames,
    DeploymentDevice,
    SMPChirpstackFuotaTransport,
    SMPChirpstackFuotaConnectionError,
    ChirpstackFuotaMulticastGroupTypes,
    ChirpstackFuotaDownlinkSpeed,
    chirpstack_fuota_configurations,
    ChirpstackFuotaMulticastGroupTypes,
    ChirpstackFuotaDownlinkSpeed,
    ChirpstackFuotaDownlinkStats,
    SMPChirpstackFuotaTransportException
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
    t = SMPChirpstackFuotaTransport(multicast_group_type=ChirpstackFuotaMulticastGroupTypes.CLASS_C,
                                    downlink_speed=ChirpstackFuotaDownlinkSpeed.DL_SLOW)
    assert t.get_multicast_timeout_seconds(ChirpstackFuotaMulticastGroupTypes.CLASS_C, ChirpstackFuotaDownlinkSpeed.DL_SLOW) == 361
    assert t.get_multicast_timeout_seconds(ChirpstackFuotaMulticastGroupTypes.CLASS_B, ChirpstackFuotaDownlinkSpeed.DL_SLOW) == 4246

def test_check_status_response():
    """
    Test the check_status_response method to ensure it raises exceptions correctly.
    """
    t = SMPChirpstackFuotaTransport()

    # JSON string representation of the status_response
    json_string = '''{
        "created_at": 1743567559,
        "updated_at": 1743568006,
        "mc_group_setup_completed_at": 1743567564,
        "mc_session_completed_at": 1743567572,
        "frag_session_setup_completed_at": 1743567570,
        "enqueue_completed_at": 1743567616,
        "frag_status_completed_at": 1743568006,
        "device_status": [{
            "dev_eui": "9eef3ebb6a791a18",
            "created_at": 1743567559,
            "updated_at": 1743567572,
            "mc_group_setup_completed_at": 1743567564,
            "mc_session_completed_at": 1743567572,
            "frag_session_setup_completed_at": 1743567570,
            "frag_status_completed_at": 0,
            "logs": [{
                "created_at": 1743567559,
                "f_port": 200,
                "command": "McGroupSetupReq",
                "fields": {
                    "max_mc_fcnt": "4294967295",
                    "mc_group_id": "0",
                    "mc_key_encrypted": "5dd2bb08d023247efbc4623a23eb0552",
                    "mc_addr": "f352a222",
                    "min_mc_fcnt": "0"
                }
            }, {
                "created_at": 1743567564,
                "f_port": 200,
                "command": "McGroupSetupAns",
                "fields": {
                    "mc_group_id": "0",
                    "id_error": "false"
                }
            }, {
                "created_at": 1743567564,
                "f_port": 201,
                "command": "FragSessionSetupReq",
                "fields": {
                    "frag_size": "64",
                    "nb_frag": "32",
                    "padding": "0",
                    "descriptor": "00000000",
                    "frag_index": "0",
                    "fragmentation_matrix": "0",
                    "McGroupBitMask": "1",
                    "block_ack_delay": "1"
                }
            }, {
                "created_at": 1743567570,
                "f_port": 201,
                "command": "FragSessionSetupAns",
                "fields": {
                    "encoding_unsupported": "false",
                    "wrong_descriptor": "false",
                    "not_enough_memory": "false",
                    "frag_index": "0",
                    "frag_session_index_not_supported": "false"
                }
            }, {
                "created_at": 1743567570,
                "f_port": 200,
                "command": "McClassCSessionReq",
                "fields": {
                    "session_time": "1427602833",
                    "mc_group_id": "0",
                    "session_time_out": "8",
                    "dr": "9",
                    "dl_frequency": "923300000"
                }
            }, {
                "created_at": 1743567572,
                "f_port": 200,
                "command": "McClassCSessionAns",
                "fields": {
                    "dr_error": "false",
                    "freq_error": "false",
                    "mc_group_id": "0",
                    "mc_group_undefined": "false"
                }
            }, {
                "created_at": 1743567871,
                "f_port": 201,
                "command": "FragSessionStatusReq",
                "fields": {
                    "participants": "true",
                    "frag_index": "0"
                }
            }, {
                "created_at": 1743567882,
                "f_port": 201,
                "command": "FragSessionStatusAns",
                "fields": {
                    "missing_frag": "1",
                    "nb_frag_received": "33",
                    "not_enough_matrix_memory": "false",
                    "frag_index": "0"
                }
            }, {
                "created_at": 1743567916,
                "f_port": 201,
                "command": "FragSessionStatusReq",
                "fields": {
                    "participants": "true",
                    "frag_index": "0"
                }
            }, {
                "created_at": 1743567951,
                "f_port": 201,
                "command": "FragSessionStatusAns",
                "fields": {
                    "missing_frag": "1",
                    "nb_frag_received": "33",
                    "not_enough_matrix_memory": "false",
                    "frag_index": "0"
                }
            }, {
                "created_at": 1743567961,
                "f_port": 201,
                "command": "FragSessionStatusReq",
                "fields": {
                    "participants": "true",
                    "frag_index": "0"
                }
            }, {
                "created_at": 1743567987,
                "f_port": 201,
                "command": "FragSessionStatusAns",
                "fields": {
                    "missing_frag": "1",
                    "nb_frag_received": "33",
                    "not_enough_matrix_memory": "false",
                    "frag_index": "0"
                }
            }]
        }]
    }'''

    # Load the JSON object into the status_response variable
    status_response = json.loads(json_string)

    downlink_stats = ChirpstackFuotaDownlinkStats()

    assert t.check_status_response(status_response, downlink_stats) is True

@pytest.mark.asyncio
async def test_send_and_receive_timeout() -> None:
    t = SMPChirpstackFuotaTransport()
    req_header = ImageUploadWrite(off=2345,
                        data=b"",
                        len=54120,
                        image=1,
                        upgrade=None)

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
async def test_verify_app_id(mock_app_service):
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
async def test_get_matched_devices(mock_device_service):
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
    mock_device_service_instance.get.return_value = {"device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")}

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
async def test_connect(mock_device_service, mock_fuota_service, mock_app_service):
    # Arrange
    mock_app_service_instance = mock_app_service.return_value
    mock_fuota_service_instance = mock_fuota_service.return_value
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
    mock_device_service_instance.get.return_value = {"device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")}

    await transport.connect("address", 1.0)

@pytest.mark.asyncio
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_device_logs")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.create_deployment")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.get_deployment_status")
@patch("smpclient.transport.chirpstack_fuota.FuotaService.__init__", return_value=None)
@patch("smpclient.transport.chirpstack_fuota.ApplicationService")
@patch("smpclient.transport.chirpstack_fuota.DeviceService")
async def test_send(mock_device_service, mock_app_service, mock_fuota_service_init, mock_get_deployment_status, mock_create_deployment, mock_get_deployment_device_logs):
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
        downlink_speed=ChirpstackFuotaDownlinkSpeed.DL_SLOW
    )

    # Mock the connect method dependencies
    mock_app_service_instance.get = MagicMock()
    mock_device_service_instance.get = MagicMock()
    mock_device_service_instance.get.return_value = {"device": DeploymentDevice(dev_eui="test_eui", gen_app_key="test_key")}  # Ensure a valid device is returned

    # Call the connect method
    await transport.connect("address", 1.0)

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
    log_instance2.fields['nb_frag_received'] = 20
    log_instance2.fields['missing_frag'] = 1
    device_logs.logs.append(log_instance2)

    device_status_instance_log = MagicMock()
    device_status_instance_log.created_at.seconds = int(time.time())
    device_status_instance_log.created_at.nanos = 0
    device_logs.append(device_status_instance_log)
    mock_get_deployment_device_logs.return_value = device_logs

    # Act
    await transport.send(bytes([random.randint(0, 255) for _ in range(2500)]))

    expected_call_count = int(math.ceil(2500.0/chirpstack_fuota_configurations[ChirpstackFuotaMulticastGroupTypes.CLASS_B][ChirpstackFuotaDownlinkSpeed.DL_SLOW]["mtu"]))

    # Assert
    mock_create_deployment.assert_called()
    assert mock_create_deployment.call_count == 2
    mock_get_deployment_status.assert_called_with("valid_id")
    assert mock_get_deployment_status.call_count == 2

    # Verify the deployment_config
    called_args = mock_create_deployment.call_args[1]
    expected_deployment_config = {
        "multicast_timeout":
            chirpstack_fuota_configurations[ChirpstackFuotaMulticastGroupTypes.CLASS_B][ChirpstackFuotaDownlinkSpeed.DL_SLOW]["multicast_timeout"],
        "unicast_timeout":
            chirpstack_fuota_configurations[ChirpstackFuotaMulticastGroupTypes.CLASS_B][ChirpstackFuotaDownlinkSpeed.DL_SLOW]["unicast_timeout"],
        "fragmentation_fragment_size":
            chirpstack_fuota_configurations[ChirpstackFuotaMulticastGroupTypes.CLASS_B][ChirpstackFuotaDownlinkSpeed.DL_SLOW]["fragmentation_fragment_size"],
        "fragmentation_redundancy":
            chirpstack_fuota_configurations[ChirpstackFuotaMulticastGroupTypes.CLASS_B][ChirpstackFuotaDownlinkSpeed.DL_SLOW]["fragmentation_redundancy"],
    }
    for key, value in expected_deployment_config.items():
        assert called_args[key] == value


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

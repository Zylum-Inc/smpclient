"""A Chirpstack_FUOTA (LORAWAN_FUOTA) SMPTransport."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import sys
import time
import subprocess
import requests

from datetime import datetime

from logging import FileHandler, Formatter, StreamHandler

from enum import StrEnum
from typing import Final, List, Protocol, TypedDict
from uuid import UUID

from chirpstack_api import api as chirpstack_api
from chirpstack_fuota_client import  ApplicationService, DeviceService, DeviceProfileService, FuotaService, FuotaUtils

from smp.message import ReadRequest as SMPReadRequest
from smp import header as smphdr
from smp.header import CommandId
from smp import os_management as smpos
from smp import image_management as smpimg
from typing_extensions import TypeGuard, override

from smpclient.exceptions import SMPClientException
from smpclient.transport import SMPTransport, SMPTransportDisconnected

class SMPChirpstackFuotaConnectionError(SMPClientException):
    """Raised when an SMP Chirpstack FUOTA connection error occurs."""

class SMPChirpstackFuotaTransportException(SMPClientException):
    """Raised when an SMP Chirpstack FUOTA transport error occurs."""

cfc_logger = logging.getLogger(__name__)

"""
"fuota_config": {
                    "fragmentation_block_ack_delay": 1,
                    "fragmentation_fragment_size": 64,
                    "fragmentation_matrix": 0,
                    "fragmentation_redundancy": 10,
                    "fragmentation_session_index": 0,
                    "gen_app_key": "edc80e5f8b4ee778596c5bf8671a4fc1",
                    "multicast_dr": 9,
                    "multicast_frequency": 923300000,
                    "multicast_group_id": 0,
                    "multicast_group_type": "CLASS_C",
                    "multicast_ping_slot_period": 1,
                    "multicast_region": "US915",
                    "multicast_timeout": 9,
                    "request_fragmentation_session_status": "AFTER_SESSION_TIMEOUT",
                    "unicast_attempt_count": 1,
                    "unicast_timeout": 60
                },

"""

class ChirpstackFuotaDownlinkStats:
    def __init__(self):
        self._start_time = time.time()
        self._total_time = 0.0
        self._total_multicast_downlink_time = 0.0
        self._total_setup_time = 0.0
        self._setup_overhead = 0.0
        self._multicast_utilization = 0.0

    def update_downlink_stats(self, status_response: dict) -> None:
        self._total_multicast_downlink_time += (status_response["frag_status_completed_at"]
                                                - status_response["enqueue_completed_at"])
        self._total_setup_time += (status_response["enqueue_completed_at"] -
                                   status_response["mc_group_setup_completed_at"])
        self._total_time = time.time() - self._start_time

        if self._total_time > 0:
            self._multicast_utilization = self._total_multicast_downlink_time / self._total_time
            self._setup_overhead = self._total_setup_time / self._total_time

    def __str__(self) -> str:
        return json.dumps({
            "multicast_utilization": f"{self._multicast_utilization:.4f}",
            "setup_overhead": f"{self._setup_overhead:.4f}",
        })

class ChirpstackFuotaMulticastGroupTypes(StrEnum):
    CLASS_B = "CLASS_B"
    CLASS_C = "CLASS_C"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class ChirpstackFuotaDownlinkSpeed(StrEnum):
    DL_FAST = "DL_FAST"
    DL_MEDIUM = "DL_MEDIUM"
    DL_SLOW = "DL_SLOW"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


# Define the configurations for each downlink speed
chirpstack_fuota_configurations = {
    ChirpstackFuotaMulticastGroupTypes.CLASS_C: {
        ChirpstackFuotaDownlinkSpeed.DL_FAST: {
            "mtu": 4992,
            "multicast_dr": 11,
            "multicast_timeout": 8,
            "unicast_timeout": 45,
            "fragmentation_fragment_size": 208,
            "fragmentation_redundancy": 10,
            "multicast_ping_slot_period": 0,
        },
        ChirpstackFuotaDownlinkSpeed.DL_MEDIUM: {
            "mtu": 3072,
            "multicast_dr": 10,
            "multicast_timeout": 8,
            "unicast_timeout": 45,
            "fragmentation_fragment_size": 128,
            "fragmentation_redundancy": 10,
            "multicast_ping_slot_period": 0,
        },
        ChirpstackFuotaDownlinkSpeed.DL_SLOW: {
            "mtu": 2048,
            "multicast_dr": 9,
            "multicast_timeout": 8,
            "unicast_timeout": 45,
            "fragmentation_fragment_size": 64,
            "fragmentation_redundancy": 5,
            "multicast_ping_slot_period": 0,
        },
    },
    ChirpstackFuotaMulticastGroupTypes.CLASS_B: {
        ChirpstackFuotaDownlinkSpeed.DL_FAST: {
            "mtu": 4992,
            "multicast_dr": 11,
            "multicast_timeout": 8,
            "unicast_timeout": 45,
            "fragmentation_fragment_size": 208,
            "fragmentation_redundancy": 10,
            "multicast_ping_slot_period": 0,
        },
        ChirpstackFuotaDownlinkSpeed.DL_MEDIUM: {
            "mtu": 3072,
            "multicast_dr": 10,
            "multicast_timeout": 8,
            "unicast_timeout": 45,
            "fragmentation_fragment_size": 128,
            "fragmentation_redundancy": 10,
            "multicast_ping_slot_period": 0,
        },
        ChirpstackFuotaDownlinkSpeed.DL_SLOW: {
            "mtu": 1920,
            "multicast_dr": 10,
            "multicast_timeout": 5,
            "unicast_timeout": 90,
            "fragmentation_fragment_size": 64,
            "fragmentation_redundancy": 5,
            "multicast_ping_slot_period": 1,
        },
    }
}


class ChirpstackFuotaRegionNames(StrEnum):
    EU_868 = "EU868"
    AS_923_GRP1 = "AS923"
    US_915 = "US915"
    AU_915 = "AU915"
    CN_470 = "CN470"
    AS_923_GRP2 = "AS923_2"
    AS_923_GRP3 = "AS923_3"
    IN_865 = "IN865"
    KR_920 = "KR920"
    RU_864 = "RU864"
    AS_923_GRP4 = "AS923_4"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class LoraBasicsClassNames(StrEnum):
    CLASS_A = "CLASS_A"
    CLASS_B = "CLASS_B"
    CLASS_C = "CLASS_C"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))

class DeploymentDevice(TypedDict):
    dev_eui: str
    gen_app_key: str

class SMPChirpstackFuotaTransport(SMPTransport):
    """A Chirpstack Fuota (LORAWAN FUOTA) SMPTransport."""

    def __init__(self, mtu: int = 1024,
                 multicast_group_type: ChirpstackFuotaMulticastGroupTypes = ChirpstackFuotaMulticastGroupTypes.CLASS_C,
                 multicast_region: ChirpstackFuotaRegionNames = ChirpstackFuotaRegionNames.US_915,
                 chirpstack_server_addr: str = "localhost:8080",
                 chirpstack_server_api_token: str = "",
                 chirpstack_server_app_id: str = "",
                 devices: List[DeploymentDevice] = None,
                 chirpstack_fuota_server_addr: str = "localhost:8070",
                 send_max_duration_s: float = 3600.0,
                 downlink_speed: ChirpstackFuotaDownlinkSpeed = ChirpstackFuotaDownlinkSpeed.DL_SLOW,
                 tas_api_addr: str = "localhost:8002",
                 tas_api_lns_id: str = ""
                 ) -> None:
        """Initialize the SMP Chirpstack FUOTA transport.

        Args:
            mtu: The Maximum Transmission Unit (MTU) in 8-bit bytes.
        """
        self._timeout_s = 5.0
        self._send_max_duration_s = send_max_duration_s
        self._send_start_time = 0.0
        self._send_end_time = 0.0
        self._fuota_service = None
        self._app_service = None
        if devices is None:
            devices = []
        self._multicast_group_type = multicast_group_type
        self._multicast_region = multicast_region
        self._chirpstack_server_addr = chirpstack_server_addr
        self._chirpstack_server_api_token = chirpstack_server_api_token
        self._chirpstack_server_app_id = chirpstack_server_app_id
        self._devices = devices
        self._matched_devices = []
        self._chirpstack_fuota_server_addr = chirpstack_fuota_server_addr
        self._downlink_speed = downlink_speed
        self._tas_api_addr = tas_api_addr
        self._tas_api_lns_id = tas_api_lns_id
        self._off = 0
        self._image_size = 0
        self._mtu = chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["mtu"]
        cfc_logger.debug(f"Chirpstack Fuota Mtu: {self._mtu}")
        cfc_logger.debug(f"Here are the handlers: {cfc_logger.handlers}")


    async def verify_app_id(self, app_id: str) -> bool:
        verified = False
        try:
            self._app_service = ApplicationService(self._chirpstack_server_addr, self._chirpstack_server_api_token)
            application = self._app_service.get(app_id)
            if application is not None:
                verified = True
        except Exception as e:
            cfc_logger.error(f"Failed to verify app id {app_id}: {str(e)}")
        return verified

    async def get_matched_devices(self) -> List[DeploymentDevice]:
        matched_devices = []
        try:
            device_service = DeviceService(self._chirpstack_server_addr, self._chirpstack_server_api_token)
            for device in self._devices:
                device_response = device_service.get(device["dev_eui"])
                cfc_logger.debug(f"Device response: {device_response} type: {type(device_response)}")
                if device_response is not None:
                    matched_devices.append(device)
        except Exception as e:
            cfc_logger.error(f"Failed to get matched devices: {str(e)}")

        return matched_devices

    async def get_deployment_status(self, deployment_id: str) -> dict:
        try:
            deployment_status = self._fuota_service.get_deployment_status(deployment_id)
            status_response = FuotaUtils.serialize_deployment_status(deployment_status, "epoch")

            # Get device logs
            for device_status in status_response["device_status"]:
                try:
                    device_logs = self._fuota_service.get_deployment_device_logs(
                        deployment_id=deployment_id,
                        dev_eui=device_status["dev_eui"]
                    )
                    if device_logs and device_logs.logs:
                        device_status["logs"] = FuotaUtils.serialize_device_logs(device_logs, "epoch")

                except Exception as e:
                    print(f"Error getting device logs for {device_status['dev_eui']}: {str(e)}")
                    device_status["logs_error"] = str(e)
                    raise SMPChirpstackFuotaTransportException(f"Failed to get device logs: {str(e)}")

            return status_response

        except Exception as e:
            raise SMPChirpstackFuotaTransportException(f"Failed to get deployment status: {str(e)}")

    @staticmethod
    def check_status_response(status_response: dict, downlink_stats: ChirpstackFuotaDownlinkStats) -> bool:
        deployment_completed = False
        if status_response["frag_status_completed_at"] > 0:
            device_logs_test_count = 0
            completed_devices = 0
            for device_status in status_response["device_status"]:
                frag_session_setup_req = False
                frag_session_status_ans = False
                nb_frag_sent = 0
                nb_frag_received = 0
                missing_frag = -1
                for log_entry in device_status['logs']:
                    if log_entry['command'] == "FragSessionSetupReq":
                        frag_session_setup_req = True
                        nb_frag_sent = int(log_entry['fields']['nb_frag'])
                    elif log_entry['command'] == "FragSessionStatusAns":
                        frag_session_status_ans = True
                        nb_frag_received = int(log_entry['fields']['nb_frag_received'])
                        missing_frag = int(log_entry['fields']['missing_frag'])

                device_logs_test_count += 1
                # Hack: To handle a bad bug in the Semtech LBM 4.5.X code,
                # whereby the code reports a *phantom* missing fragment, even for a
                # successful FUOTA session where *no* fragments were lost
                if (frag_session_setup_req and frag_session_status_ans
                        and (nb_frag_sent == nb_frag_received
                             or (nb_frag_sent <= nb_frag_received and missing_frag == 0))):
                    completed_devices += 1

            if completed_devices > 0:
                downlink_stats.update_downlink_stats(status_response)
                cfc_logger.info(f"Downlink stats: {downlink_stats}")
                deployment_completed = True
            elif device_logs_test_count > 3:
                raise SMPChirpstackFuotaTransportException(f"Deployment failed for all devices")

        return deployment_completed


    @override
    async def connect(self, address: str, timeout_s: float) -> None:
        self._timeout_s = timeout_s
        cfc_logger.debug(f"Connecting to chirpstack network server: {self._chirpstack_server_addr}")
        try:
            if not await self.verify_app_id(self._chirpstack_server_app_id):
                raise SMPChirpstackFuotaConnectionError(f"Failed to get application {self._chirpstack_server_app_id}")
            self._fuota_service = FuotaService(self._chirpstack_fuota_server_addr, self._chirpstack_server_api_token)
            self._matched_devices = await self.get_matched_devices()
            if len(self._matched_devices) == 0:
                raise SMPChirpstackFuotaConnectionError(f"Failed to get any matching devices")

        except Exception as e:
            raise SMPChirpstackFuotaConnectionError(f"Failed to Connect with Chirpstack: {str(e)}")

    @override
    async def disconnect(self) -> None:
        cfc_logger.debug("Disconnecting from transport")
        self._fuota_service = None
        cfc_logger.info("Disconnected from transport")

    async def send_unicast(self, dev_eui: str, data: bytes, fport: int) -> None:
        cfc_logger.debug(f"Sending unicast data: {data}")
        try:
            device_service = DeviceService(self._chirpstack_server_addr, self._chirpstack_server_api_token)
            device_service.queue_downlink(dev_eui, data, fport)
        except Exception as e:
            cfc_logger.error(f"Failed to send unicast message to device {dev_eui}: {str(e)}")
            raise SMPChirpstackFuotaTransportException(f"Failed to send unicast message to device {dev_eui}: {str(e)}")

    # These functions have been copied from tas-cli. They should probably be modularized.
    def find_dev_id_by_dev_eui(self, dev_eui: str):
        lns_uri = f"{self._tas_api_addr}/devices/lns_config/?dev_eui={dev_eui}&lns_id={self._tas_api_lns_id}"

        cloud_lns_response = requests.get(lns_uri)

        if cloud_lns_response.status_code == 200:
            return cloud_lns_response.json()[0]['device_id']

        return None

    def get_messages_by_dev_id(self, dev_id: str, message_type: str = None, fport: int = None, after_epoch: int = None):
        request_uri = f"{self._tas_api_addr}/devices/{dev_id}/messages/"

        if message_type is not None:
            request_uri += f"?type={message_type}"

        if fport is not None:
            request_uri += f"&port={fport}"

        if after_epoch is not None:
            request_uri += f"&captured_at__gt={datetime.fromtimestamp(after_epoch).isoformat()}"

        cloud_lns_response = requests.get(request_uri)

        if cloud_lns_response.status_code == 200:
            return cloud_lns_response.json()

        raise SMPChirpstackFuotaTransportException(f"Failed to get messages for device {dev_id}")

    async def receive_unicast(self, after_epoch: int, dev_eui: str, fport: int, timeout_s: float):

        start_time = time.time()
        cfc_logger.debug(f"Receiving unicast data from device {dev_eui}")

        dev_id = self.find_dev_id_by_dev_eui(dev_eui)
        if dev_id is None:
            raise SMPChirpstackFuotaTransportException(f"Failed to find device ID for {dev_eui}")

        while time.time() - start_time < timeout_s:
            # Get the messages
            uplinks = self.get_messages_by_dev_id(dev_id, 'uplink', fport, after_epoch)

            sorted_uplinks = sorted(uplinks,
                                     key=lambda x: datetime.fromisoformat(x['data']['time'].replace('Z', '+00:00')))

            if len(sorted_uplinks) == 0:
                cfc_logger.debug(f"No messages received yet")
                await asyncio.sleep(5)
                continue

            payload_bytes = base64.b64decode(sorted_uplinks[0]["data"]["data"])

            if len(payload_bytes) < smphdr.Header.SIZE:  # pragma: no cover
                raise SMPChirpstackFuotaTransportException(
                    f"Buffer contents not big enough for SMP header: {payload_bytes=}"
                )

            header = smphdr.Header.loads(payload_bytes[: smphdr.Header.SIZE])
            cfc_logger.debug(f"Received {header=}")

            message_length = header.length + header.SIZE
            cfc_logger.debug(f"Waiting for the rest of the {message_length} byte response")

            if len(payload_bytes) == message_length:
                cfc_logger.debug(f"Received full message: {payload_bytes}")
                return payload_bytes

            for uplink in sorted_uplinks[1:]:
                more_payload_bytes = base64.b64decode(uplink["data"]["data"])
                cfc_logger.debug(f"Received more payload: {more_payload_bytes}")
                payload_bytes += more_payload_bytes
                # Check if we have received the full message
                if len(payload_bytes) == message_length:
                    break
                elif len(payload_bytes) > message_length:
                    raise SMPChirpstackFuotaTransportException(
                        f"Received too much data: {payload_bytes=}"
                    )

            return payload_bytes

        return None


    @override
    async def send(self, data: bytes) -> None:
        self._send_start_time = time.time()
        downlink_stats = ChirpstackFuotaDownlinkStats()
        cfc_logger.info(f"Sending {len(data)} B")
        cfc_logger.debug(f"Mtu: {self._mtu}")
        self._send_max_duration_s = max(500.0, 500.0 * (len(data)/self._mtu))
        cfc_logger.info(f"send_max_duration_s: {self._send_max_duration_s}")
        deployment_config = FuotaUtils.create_deployment_config(
            multicast_timeout=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["multicast_timeout"],
            unicast_timeout=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["unicast_timeout"],
            fragmentation_fragment_size=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["fragmentation_fragment_size"],
            fragmentation_redundancy=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["fragmentation_redundancy"],
            multicast_ping_slot_period=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["multicast_ping_slot_period"],
            unicast_attempt_count=3
        )
        for offset in range(0, len(data), self._mtu):
            cfc_logger.info(f"Creating deployment for offset {offset}")

            try:
                # Create the deployment
                deployment_response = self._fuota_service.create_deployment(
                    application_id=self._chirpstack_server_app_id,
                    devices=self._matched_devices,
                    multicast_group_type=self._multicast_group_type,
                    multicast_dr=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["multicast_dr"],
                    multicast_frequency=923300000,
                    multicast_group_id=0,
                    multicast_region=self._multicast_region,
                    request_fragmentation_session_status="AFTER_SESSION_TIMEOUT",
                    payload=data[offset : offset + self.mtu],
                    **deployment_config,
                )
            except Exception as e:
                raise SMPChirpstackFuotaTransportException(f"Failed create deployment for offset {offset}: {str(e)}")

            cfc_logger.debug(f"Created FUOTA deployment with ID: {deployment_response.id}")

            deployment_completed = False

            # logger.debug(f"Sleeping for 30 seconds")
            # await asyncio.sleep(30)

            cfc_logger.debug(f"Getting deployment status")

            try:
                while not deployment_completed:
                    # Get deployment status
                    status_response = await self.get_deployment_status(deployment_response.id)
                    cfc_logger.debug(f"status_response: {status_response}")
                    deployment_completed = self.check_status_response(status_response, downlink_stats)

                    if not deployment_completed:
                        if time.time() - self._send_start_time > self._send_max_duration_s:
                            raise SMPChirpstackFuotaTransportException(f"Deployment timeout exceeded")

                    await asyncio.sleep(self._timeout_s)
            except Exception as e:
                raise SMPChirpstackFuotaTransportException(f"Failed to get deployment status: {str(e)}")

        cfc_logger.info(f"Sent {len(data)} B")
        cfc_logger.info(f"Downlink stats: {downlink_stats}")
        cfc_logger.info(f"Effective Data Rate: { len(data) / downlink_stats._total_time:.4f} B/s")


    @override
    async def receive(self) -> bytes:
        cfc_logger.debug("Receiving data")
        data = await self._fuota_service.receive()
        cfc_logger.debug(f"Received {len(data)} B")
        return data

    @override
    async def send_and_receive(self, data: bytes) -> bytes:
        cfc_logger.debug(f"Sending and receiving {len(data)} B")
        #TODO: Make this send an actual SMP Unicast Downlink message to the device(s), and have them respond
        #      with an SMP Unicast Uplink message. For now, just pretend there is an actual received response
        req_header = smphdr.Header.loads(data[: smphdr.Header.SIZE])
        cfc_logger.debug(f"Received {req_header}")
        # Handle the so called MCUMgrParametersReadRequest
        if (req_header.group_id == smphdr.GroupId.OS_MANAGEMENT
                and req_header.command_id == CommandId.OSManagement.MCUMGR_PARAMETERS):
            mcumgr_params_read_request = smpos.MCUMgrParametersReadRequest.loads(data)
            cfc_logger.debug(f"Received MCUMgrParametersReadRequest: {mcumgr_params_read_request}")
            # Create the response
            # Make sure the response sequence number matches the request sequence number
            response = smpos.MCUMgrParametersReadResponse(buf_size=self._mtu, buf_count=1, sequence=req_header.sequence)
            cfc_logger.debug(f"Sending MCUMgrParametersReadResponse: {response}")
            return response.BYTES
        elif (req_header.group_id == smphdr.GroupId.IMAGE_MANAGEMENT
                and req_header.command_id == CommandId.ImageManagement.UPLOAD):
            cfc_logger.debug("Received ImageUploadWriteRequest")
            # Special handling for the *first* ImageUploadWriteRequest
            image_upload_write_request = smpimg.ImageUploadWriteRequest.loads(data)
            #logger.debug(f"Received ImageUploadWriteRequest: {image_upload_write_request}")
            cfc_logger.debug(f"ImageUploadWriteRequest.header: {image_upload_write_request.header}")
            cfc_logger.debug(f"ImageUploadWriteRequest.sequence: {image_upload_write_request.sequence}")
            cfc_logger.debug(f"len(ImageUploadWriteRequest.smp_data): {len(image_upload_write_request.smp_data)}")
            cfc_logger.debug(f"len(ImageUploadWriteRequest.data): {len(image_upload_write_request.data)}")
            cfc_logger.debug(f"ImageUploadWriteRequest.off: {image_upload_write_request.off} "
                         f".len: {image_upload_write_request.len} "
                         f".upgrade: {image_upload_write_request.upgrade}")
            # Check to see if this is the first block being transmitted
            if image_upload_write_request.off == 0:
                self._off = 0
                self._image_size = image_upload_write_request.len
            await self.send(data)
            await asyncio.sleep(self._timeout_s)

            # Manually update the offset
            self._off += len(image_upload_write_request.data)

            # Create the response
            response = smpimg.ImageUploadWriteResponse(sequence=req_header.sequence, off=self._off)
            cfc_logger.debug(f"Sending ImageBlockWriteResponse: {response}")
            return response.BYTES
        else:
            raise SMPChirpstackFuotaTransportException(f"Unsupported command: {req_header}")


    @property
    def max_unencoded_size(self) -> int:
        return self._mtu

    @property
    def mtu(self) -> int:
        return self._mtu


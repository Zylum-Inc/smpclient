"""A Chirpstack_FUOTA (LORAWAN_FUOTA) SMPTransport."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sys
import time

from logging import FileHandler, Formatter, StreamHandler

from enum import StrEnum
from typing import Final, List, Protocol, TypedDict
from uuid import UUID

from chirpstack_api import api as chirpstack_api
from chirpstack_fuota_client import  ApplicationService, DeviceService, DeviceProfileService, FuotaService, FuotaUtils

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

# Create a custom logger
def create_logger():
    custom_logger = logging.getLogger(__name__)
    custom_logger.setLevel(logging.DEBUG)

    # Create a file handler
    file_handler = FileHandler('smpclient.log')
    file_handler.setLevel(logging.DEBUG)

    # Create a streaming handler
    console = logging.StreamHandler(stream=sys.stdout)
    console.setLevel(logging.CRITICAL)

    # Create a formatter
    formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add the formatter to the handlers
    console.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    custom_logger.addHandler(console)
    custom_logger.addHandler(file_handler)

    return custom_logger

cfc_logger = create_logger()

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
            "mtu": 2560,
            "multicast_dr": 9,
            "multicast_timeout": 8,
            "unicast_timeout": 45,
            "fragmentation_fragment_size": 64,
            "fragmentation_redundancy": 10,
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
            "mtu": 1536,
            "multicast_dr": 9,
            "multicast_timeout": 5,
            "unicast_timeout": 90,
            "fragmentation_fragment_size": 64,
            "fragmentation_redundancy": 10,
            "multicast_ping_slot_period": 0,
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
                 downlink_speed: ChirpstackFuotaDownlinkSpeed = ChirpstackFuotaDownlinkSpeed.DL_SLOW
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
            multicast_ping_slot_period=chirpstack_fuota_configurations[self._multicast_group_type][self._downlink_speed]["multicast_ping_slot_period"]
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

            while not deployment_completed:
                # Get deployment status
                try:
                    deployment_status = self._fuota_service.get_deployment_status(deployment_response.id)
                    status_response = FuotaUtils.serialize_deployment_status(deployment_status, "epoch")

                    # Get device logs
                    for device_status in status_response["device_status"]:
                        try:
                            device_logs = self._fuota_service.get_deployment_device_logs(
                                deployment_id=deployment_response.id,
                                dev_eui=device_status["dev_eui"]
                            )
                            if device_logs and device_logs.logs:
                                device_status["logs"] = FuotaUtils.serialize_device_logs(device_logs, "epoch")

                        except Exception as e:
                            print(f"Error getting device logs for {device_status['dev_eui']}: {str(e)}")
                            device_status["logs_error"] = str(e)


                except Exception as e:
                    raise SMPChirpstackFuotaTransportException(f"Failed to get deployment status: {str(e)}")

                cfc_logger.debug(f"status_response: {status_response}")
                if status_response["frag_status_completed_at"] > 0:
                    device_logs_test_count = 0
                    completed_devices = 0
                    for device_status in status_response["device_status"]:
                        frag_session_setup_req = False
                        frag_session_status_ans = False
                        nb_frag_sent = 0
                        nb_frag_received = 0
                        for log_entry in device_status['logs']:
                            if log_entry['command'] == "FragSessionSetupReq":
                                frag_session_setup_req = True
                                nb_frag_sent = int(log_entry['fields']['nb_frag'])
                            elif log_entry['command'] == "FragSessionStatusAns":
                                frag_session_status_ans = True
                                nb_frag_received = int(log_entry['fields']['nb_frag_received'])

                        device_logs_test_count += 1
                        if frag_session_setup_req and frag_session_status_ans and nb_frag_sent == nb_frag_received:
                            completed_devices += 1

                    if completed_devices > 0:
                        downlink_stats.update_downlink_stats(status_response)
                        cfc_logger.info(f"Downlink stats: {downlink_stats}")
                        deployment_completed = True
                    elif device_logs_test_count > 3:
                        raise SMPChirpstackFuotaTransportException(f"Deployment failed for all devices")
                else:
                    if time.time() - self._send_start_time > self._send_max_duration_s:
                        raise SMPChirpstackFuotaTransportException(f"Deployment timeout exceeded")
                await asyncio.sleep(self._timeout_s)

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


"""A Chirpstack_FUOTA (LORAWAN_FUOTA) SMPTransport."""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time

from enum import StrEnum
from typing import Final, List, Protocol, TypedDict
from uuid import UUID

from chirpstack_api import api as chirpstack_api
from chirpstack_fuota_client import  ApplicationService, DeviceService, DeviceProfileService, FuotaService, FuotaUtils

from smp import header as smphdr
from typing_extensions import TypeGuard, override

from smpclient.exceptions import SMPClientException
from smpclient.transport import SMPTransport, SMPTransportDisconnected

class SMPChirpstackFuotaConnectionError(SMPClientException):
    """Raised when an SMP Chirpstack FUOTA connection error occurs."""

class SMPChirpstackFuotaTransportException(SMPClientException):
    """Raised when an SMP Chirpstack FUOTA transport error occurs."""

logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

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
                 multicast_group_type: LoraBasicsClassNames = LoraBasicsClassNames.CLASS_C,
                 multicast_region: ChirpstackFuotaRegionNames = ChirpstackFuotaRegionNames.US_915,
                 chirpstack_server_addr: str = "localhost:8080",
                 chirpstack_server_api_token: str = "",
                 chirpstack_server_app_id: str = "",
                 devices: List[DeploymentDevice] = None,
                 chirpstack_fuota_server_addr: str = "localhost:8070",
                 send_max_duration_s: float = 3600.0,
                 ) -> None:
        """Initialize the SMP Chirpstack FUOTA transport.

        Args:
            mtu: The Maximum Transmission Unit (MTU) in 8-bit bytes.
        """
        self._timeout_s = 5.0
        self._send_max_duration_s = send_max_duration_s
        self._send_start_time = 0.0
        self._send_end_time = 0.0
        self._mtu = mtu
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

    async def verify_app_id(self, app_id: str) -> bool:
        verified = False
        try:
            self._app_service = ApplicationService(self._chirpstack_server_addr, self._chirpstack_server_api_token)
            application = self._app_service.get(app_id)
            if application is not None:
                verified = True
        except Exception as e:
            logger.error(f"Failed to verify app id {app_id}: {str(e)}")
        return verified

    async def get_matched_devices(self) -> List[DeploymentDevice]:
        matched_devices = []
        try:
            device_service = DeviceService(self._chirpstack_server_addr, self._chirpstack_server_api_token)
            for device in self._devices:
                device_response = device_service.get(device["dev_eui"])
                logger.debug(f"Device response: {device_response} type: {type(device_response)}")
                if device_response is not None:
                    matched_devices.append(device)
        except Exception as e:
            logger.error(f"Failed to get matched devices: {str(e)}")

        return matched_devices

    @override
    async def connect(self, address: str, timeout_s: float) -> None:
        self._timeout_s = timeout_s
        logger.debug(f"Connecting to chirpstack network server: {self._chirpstack_server_addr}")
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
        logger.debug("Disconnecting from transport")
        self._fuota_service = None
        logger.info("Disconnected from transport")

    @override
    async def send(self, data: bytes) -> None:
        self._send_start_time = time.time()
        logger.info(f"Sending {len(data)} B")
        deployment_config = FuotaUtils.create_deployment_config(
            multicast_timeout=9,
            unicast_timeout=90,
            fragmentation_fragment_size=64,
            fragmentation_redundancy=100,
        )
        for offset in range(0, len(data), self.mtu):
            logger.info(f"Creating deployment for offset {offset}")

            try:
                # Create the deployment
                deployment_response = self._fuota_service.create_deployment(
                    application_id=self._chirpstack_server_app_id,
                    devices=self._matched_devices,
                    multicast_group_type=self._multicast_group_type,
                    multicast_dr=9,
                    multicast_frequency=923300000,
                    multicast_group_id=0,
                    multicast_region=self._multicast_region,
                    request_fragmentation_session_status="AFTER_SESSION_TIMEOUT",
                    payload=data[offset : offset + self.mtu],
                    **deployment_config,
                )
            except Exception as e:
                raise SMPChirpstackFuotaTransportException(f"Failed create deployment for offset {offset}: {str(e)}")

            logger.debug(f"Created FUOTA deployment with ID: {deployment_response.id}")

            deployment_completed = False

            # logger.debug(f"Sleeping for 30 seconds")
            # await asyncio.sleep(30)

            logger.debug(f"Getting deployment status")

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

                logger.debug(f"status_response: {status_response}")
                if status_response["frag_status_completed_at"] > 0:
                    deployment_completed = True
                else:
                    if time.time() - self._send_start_time > self._send_max_duration_s:
                        raise SMPChirpstackFuotaTransportException(f"Deployment timeout exceeded")
                await asyncio.sleep(self._timeout_s)

        logger.info(f"Sent {len(data)} B")


    @override
    async def receive(self) -> bytes:
        logger.debug("Receiving data")
        data = await self._fuota_service.receive()
        logger.debug(f"Received {len(data)} B")
        return data

    @override
    async def send_and_receive(self, data: bytes) -> bytes:
        logger.debug(f"Sending and receiving {len(data)} B")
        response = await self._fuota_service.send_and_receive(data)
        logger.debug(f"Received {len(response)} B")
        return response

    @property
    def mtu(self) -> int:
        return self._mtu

    @property
    def max_unencoded_size(self) -> int:
        return self._mtu - smphdr.HEADER_SIZE_BYTES

    @property
    def max_encoded_size(self) -> int:
        return FuotaUtils.get_max_payload_size(self.max_unencoded_size)

    @property
    def max_unencoded_size(self) -> int:
        return FuotaUtils.get_max_payload_size(self.max_encoded_size)

    @property
    def max_encoded_size(self) -> int:
        return self._mtu - smphdr.HEADER_SIZE_BYTES

    @property
    def mtu(self) -> int:
        return self._mtu


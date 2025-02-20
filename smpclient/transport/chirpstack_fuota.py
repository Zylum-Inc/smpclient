"""A Chirpstack_FUOTA (LORAWAN_FUOTA) SMPTransport."""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from enum import StrEnum
from typing import Final, List, Protocol
from uuid import UUID

from chirpstack_api import api as chirpstack_api
from chirpstack_fuota_client import  ApplicationService, DeviceProfileService, FuotaService, FuotaUtils

from smp import header as smphdr
from typing_extensions import TypeGuard, override

from smpclient.exceptions import SMPClientException
from smpclient.transport import SMPTransport, SMPTransportDisconnected

class SMPChirpstackFuotaAuthenticationError(SMPClientException):
    """Raised when an SMP Chirpstack FUOTA authentication error occurs."""

class SMPChirpstackFuotaTransportException(SMPClientException):
    """Raised when an SMP Chirpstack FUOTA transport error occurs."""

class SMPChirpstackFuotaDeviceNotFound(SMPChirpstackFuotaTransportException):
    """Raised when an SMP Chirpstack FUOTA device is not found."""

logger = logging.getLogger(__name__)

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

class LoraBasicRegionNames(StrEnum):
    EU_868 = "EU_868"
    AS_923_GRP1 = "AS_923_GRP1"
    US_915 = "US_915"
    AU_915 = "AU_915"
    CN_470 = "CN_470"
    WW2G4 = "WW2G4"
    AS_923_GRP2 = "AS_923_GRP2"
    AS_923_GRP3 = "AS_923_GRP3"
    IN_865 = "IN_865"
    KR_920 = "KR_920"
    RU_864 = "RU_864"
    CN_470_RP_1_0 = "CN_470_RP_1_0"
    AS_923_GRP4 = "AS_923_GRP4"

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

class SMPChirpstackFuotaTransport(SMPTransport):
    """A Chirpstack Fuota (LORAWAN FUOTA) SMPTransport."""

    def __init__(self, mtu: int = 1024,
                 group_type: LoraBasicsClassNames = LoraBasicsClassNames.CLASS_C,
                 region: LoraBasicRegionNames = LoraBasicRegionNames.US_915,
                 chirpstack_server_host: str = "localhost",
                 chirpstack_server_port: str = "8080",
                 chirpstack_server_api_token: str = "",
                 chirpstack_server_app_id: str = "",
                 dev_eui: str = "",
                 gen_app_key: str = "",
                 chirpstack_fuota_server_host: str = "localhost",
                 chirpstack_fuota_server_port: str = "8070",
                 chirpstack_fuota_server_api_token: str = "",
                 ) -> None:
        """Initialize the SMP Chirpstack FUOTA transport.

        Args:
            mtu: The Maximum Transmission Unit (MTU) in 8-bit bytes.
        """
        super().__init__(mtu)
        self._multicast_group_type = FuotaUtils.get_multicast_group_type(group_type)
        self._multicast_region = FuotaUtils.get_region(region)
        self._chirpstack_server_host = chirpstack_server_host
        self._chirpstack_server_port = chirpstack_server_port
        self._chirpstack_server_api_token = chirpstack_server_api_token
        self._chirpstack_server_app_id = chirpstack_server_app_id
        self._dev_eui = dev_eui
        self._gen_app_key = gen_app_key
        self._chirpstack_fuota_server_host = chirpstack_fuota_server_host
        self._chirpstack_fuota_server_port = chirpstack_fuota_server_port
        self._chirpstack_fuota_server_api_token = chirpstack_fuota_server_api_token


    @override
    async def connect(self, address: str, timeout_s: float) -> None:
        logger.debug(f"Connecting to {address=}")
        await self._fuota_service.connect(address)
        logger.info(f"Connected to {address=}")

    @override
    async def disconnect(self) -> None:
        logger.debug("Disconnecting from transport")
        self._fuota_service.disconnect()
        logger.info("Disconnected from transport")

    @override
    async def send(self, data: bytes) -> None:
        logger.debug(f"Sending {len(data)} B")
        await self._fuota_service.send(data)

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

    @property
    def max_unencoded_size(self) -> int:
"""A Chirpstack_FUOTA (LORAWAN_FUOTA) SMPTransport."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime, tzinfo
from enum import StrEnum
from logging import FileHandler, Formatter, StreamHandler
from typing import Final, List, Protocol, TypedDict
from uuid import UUID

import requests
from chirpstack_api import api as chirpstack_api
from chirpstack_fuota_client import (
    ApplicationService,
    DeviceProfileService,
    DeviceService,
    FuotaService,
    FuotaUtils,
)
from smp import header as smphdr
from smp import image_management as smpimg
from smp import os_management as smpos
from smp.header import CommandId
from smp.message import ReadRequest as SMPReadRequest
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
        self._total_multicast_downlink_time += (
            status_response["frag_status_completed_at"] - status_response["enqueue_completed_at"]
        )
        self._total_setup_time += (
            status_response["enqueue_completed_at"] - status_response["mc_group_setup_completed_at"]
        )
        self._total_time = time.time() - self._start_time

        if self._total_time > 0:
            self._multicast_utilization = self._total_multicast_downlink_time / self._total_time
            self._setup_overhead = self._total_setup_time / self._total_time

    def __str__(self) -> str:
        return json.dumps(
            {
                "multicast_utilization": f"{self._multicast_utilization:.4f}",
                "setup_overhead": f"{self._setup_overhead:.4f}",
            }
        )


class ChirpstackFuotaMulticastGroupTypes(StrEnum):
    CLASS_B = "CLASS_B"
    CLASS_C = "CLASS_C"

    @classmethod
    def list(cls) -> list[str]:
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
    },
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

    def __init__(
        self,
        mtu: int = 1024,
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
        tas_api_lns_id: str = "",
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
        self._last_send_time = 0.0
        self._expected_response_sequence = 0
        self._expected_response_group_id = 0
        self._expected_response_command_id = 0
        self._off = 0
        self._image_size = 0
        self._mtu = chirpstack_fuota_configurations[self._multicast_group_type][
            self._downlink_speed
        ]["mtu"]
        # Add tracking for processed uplinks and pending assembly
        self._processed_uplink_timestamps: set[str] = set()
        self._pending_uplinks: dict[str, list] = {}  # dev_eui -> list of pending uplinks
        cfc_logger.debug(f"Chirpstack Fuota Mtu: {self._mtu}")
        cfc_logger.debug(f"Here are the handlers: {cfc_logger.handlers}")

    async def verify_app_id(self, app_id: str) -> bool:
        verified = False
        try:
            self._app_service = ApplicationService(
                self._chirpstack_server_addr, self._chirpstack_server_api_token
            )
            application = self._app_service.get(app_id)
            if application is not None:
                verified = True
        except Exception as e:
            cfc_logger.error(f"Failed to verify app id {app_id}: {str(e)}")
        return verified

    async def get_matched_devices(self) -> List[DeploymentDevice]:
        matched_devices = []
        try:
            device_service = DeviceService(
                self._chirpstack_server_addr, self._chirpstack_server_api_token
            )
            for device in self._devices:
                device_response = device_service.get(device["dev_eui"])
                cfc_logger.debug(
                    f"Device response: {device_response} type: {type(device_response)}"
                )
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
                        deployment_id=deployment_id, dev_eui=device_status["dev_eui"]
                    )
                    if device_logs and device_logs.logs:
                        device_status["logs"] = FuotaUtils.serialize_device_logs(
                            device_logs, "epoch"
                        )

                except Exception as e:
                    print(f"Error getting device logs for {device_status['dev_eui']}: {str(e)}")
                    device_status["logs_error"] = str(e)
                    raise SMPChirpstackFuotaTransportException(
                        f"Failed to get device logs: {str(e)}"
                    )

            return status_response

        except Exception as e:
            raise SMPChirpstackFuotaTransportException(f"Failed to get deployment status: {str(e)}")

    @staticmethod
    def get_multicast_timeout_seconds(
        group_type: ChirpstackFuotaMulticastGroupTypes, downlink_speed: ChirpstackFuotaDownlinkSpeed
    ) -> int:
        timeout_exponent = chirpstack_fuota_configurations[group_type][downlink_speed][
            "multicast_timeout"
        ]

        timeout_seconds = pow(2, timeout_exponent)
        if group_type == ChirpstackFuotaMulticastGroupTypes.CLASS_B:
            timeout_seconds = timeout_seconds * 128

        timeout_seconds = (
            timeout_seconds
            + chirpstack_fuota_configurations[group_type][downlink_speed]["unicast_timeout"]
        )

        timeout_seconds = timeout_seconds + 60  # Add 60 seconds for good measure

        return timeout_seconds

    @staticmethod
    def check_status_response(
        status_response: dict, downlink_stats: ChirpstackFuotaDownlinkStats
    ) -> bool:
        deployment_completed = False
        if status_response["frag_status_completed_at"] > 0:
            device_logs_test_count = 0
            completed_devices = 0
            for device_status in status_response["device_status"]:
                cfc_logger.debug(f"Device status: {device_status}")
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
                cfc_logger.debug(f"Device logs test count: {device_logs_test_count}")
                cfc_logger.debug(
                    f"frag_session_setup_req: {frag_session_setup_req}, "
                    f"frag_session_status_ans: {frag_session_status_ans}, "
                    f"nb_frag_sent: {nb_frag_sent}, "
                    f"nb_frag_received: {nb_frag_received}, "
                    f"missing_frag: {missing_frag}"
                )
                # Hack: To handle a bad bug in the Semtech LBM 4.5.X code,
                # whereby the code reports a *phantom* missing fragment, even for a
                # successful FUOTA session where *no* fragments were lost
                if (
                    frag_session_setup_req
                    and frag_session_status_ans
                    and (
                        nb_frag_sent == nb_frag_received
                        or (nb_frag_sent <= nb_frag_received and missing_frag == 0)
                    )
                ):
                    completed_devices += 1
                cfc_logger.debug(f"completed_devices: {completed_devices}")

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
                raise SMPChirpstackFuotaConnectionError(
                    f"Failed to get application {self._chirpstack_server_app_id}"
                )
            self._fuota_service = FuotaService(
                self._chirpstack_fuota_server_addr, self._chirpstack_server_api_token
            )
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
        self._last_send_time = time.time() - 60.0
        cfc_logger.debug(f"Sending unicast data: {data}")
        try:
            device_service = DeviceService(
                self._chirpstack_server_addr, self._chirpstack_server_api_token
            )
            device_service.queue_downlink(dev_eui, data, fport)
        except Exception as e:
            cfc_logger.error(f"Failed to send unicast message to device {dev_eui}: {str(e)}")
            raise SMPChirpstackFuotaTransportException(
                f"Failed to send unicast message to device {dev_eui}: {str(e)}"
            )

    # These functions have been copied from tas-cli. They should probably be modularized.
    def find_dev_id_by_dev_eui(self, dev_eui: str) -> str | None:
        lns_uri = f"{self._tas_api_addr}/devices/lns_config/?dev_eui={dev_eui}&lns_id={self._tas_api_lns_id}"

        cloud_lns_response = requests.get(lns_uri)

        if cloud_lns_response.status_code == 200:
            return cloud_lns_response.json()[0]['device_id']

        return None

    def get_messages_by_dev_id(
        self,
        dev_id: str,
        message_type: str | None = None,
        fport: int | None = None,
        after_epoch: int | None = None,
    ) -> dict:
        request_uri = f"{self._tas_api_addr}/devices/{dev_id}/messages/"

        if message_type is not None:
            request_uri += f"?type={message_type}"

        if fport is not None:
            request_uri += f"&port={fport}"

        if after_epoch is not None:
            request_uri += f"&captured_at__gt={urllib.parse.quote(datetime.utcfromtimestamp(after_epoch).isoformat(timespec='seconds'))}"

        cfc_logger.debug(f"Requesting messages from {request_uri}")

        cloud_lns_response = requests.get(request_uri)

        if cloud_lns_response.status_code == 200:
            return cloud_lns_response.json()

        raise SMPChirpstackFuotaTransportException(f"Failed to get messages for device {dev_id}")
    

    def _validate_device_id(self, dev_eui: str) -> str:
        """Validate that the device EUI has a valid device ID."""
        dev_id = self.find_dev_id_by_dev_eui(dev_eui)
        if dev_id is None:
            raise SMPChirpstackFuotaTransportException(f"Failed to find device ID for {dev_eui}")
        return dev_id

    def _should_send_nudge(self, no_uplink_count: int) -> bool:
        """Determine if we should send a nudge to trigger a response."""
        return no_uplink_count >= 4

    async def _send_nudge(self, dev_eui: str) -> None:
        """Send a random unicast message to nudge the device for a response."""
        cfc_logger.debug("No uplinks received for 4 ticks (20 seconds), sending random unicast")
        random_payload = b'\x00\x01\x02\x03\x04'  # 5 random bytes
        await self.send_unicast(dev_eui, random_payload, 4)  # Send to fport 4

    def _get_sorted_uplinks(self, dev_id: str, fport: int, after_epoch: int) -> list:
        """Get and sort uplink messages by timestamp."""
        uplinks = self.get_messages_by_dev_id(dev_id, 'uplink', fport, after_epoch)
        return sorted(
            uplinks['events'],
            key=lambda x: x['data']['fCnt'],
        )

    def _filter_unprocessed_uplinks(self, sorted_uplinks: list) -> tuple[list, int]:
        """Filter out already processed uplinks and return the latest timestamp.
        
        Returns:
            Tuple of (filtered_uplinks, latest_timestamp_epoch)
        """
        filtered_uplinks = []
        latest_timestamp_epoch = 0
        
        for uplink in sorted_uplinks:
            timestamp_str = uplink['data']['time']
            # Convert timestamp to epoch for comparison
            timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            timestamp_epoch = int(timestamp_dt.timestamp())
            
            # Skip if already processed
            if timestamp_str in self._processed_uplink_timestamps:
                cfc_logger.debug(f"Skipping already processed uplink with timestamp: {timestamp_str}")
                continue
                
            filtered_uplinks.append(uplink)
            latest_timestamp_epoch = max(latest_timestamp_epoch, timestamp_epoch)
        
        return filtered_uplinks, latest_timestamp_epoch

    def _mark_uplinks_as_processed(self, uplinks: list) -> None:
        """Mark uplinks as processed by adding their timestamps to the processed set."""
        for uplink in uplinks:
            timestamp_str = uplink['data']['time']
            self._processed_uplink_timestamps.add(timestamp_str)
            cfc_logger.debug(f"Marked uplink as processed: {timestamp_str}")

    def _get_pending_uplinks(self, dev_eui: str) -> list:
        """Get pending uplinks for a device."""
        return self._pending_uplinks.get(dev_eui, [])

    def _add_pending_uplinks(self, dev_eui: str, uplinks: list) -> None:
        """Add uplinks to the pending list for a device."""
        if dev_eui not in self._pending_uplinks:
            self._pending_uplinks[dev_eui] = []
        self._pending_uplinks[dev_eui].extend(uplinks)
        cfc_logger.debug(f"Added {len(uplinks)} uplinks to pending list for {dev_eui}")

    def _clear_pending_uplinks(self, dev_eui: str) -> None:
        """Clear pending uplinks for a device (when message is successfully assembled)."""
        if dev_eui in self._pending_uplinks:
            del self._pending_uplinks[dev_eui]
            cfc_logger.debug(f"Cleared pending uplinks for {dev_eui}")

    def _validate_smp_header(self, payload_bytes: bytes) -> None:
        """Validate that the payload contains a valid SMP header."""
        if len(payload_bytes) < smphdr.Header.SIZE:
            raise SMPChirpstackFuotaTransportException(
                f"Buffer contents not big enough for SMP header: {payload_bytes!r}"
            )

        if len(payload_bytes) > 0:
            first_byte = payload_bytes[0]
            op_value = first_byte & 0x07  # Extract OP field (bits 0-2)
            if op_value > 3:  # Valid OP values are 0-3
                cfc_logger.warning(
                    f"Received non-SMP data (invalid OP {op_value}): {payload_bytes!r}"
                )
                raise SMPChirpstackFuotaTransportException(
                    f"Device sent non-SMP data instead of SMP response: {payload_bytes!r}"
                )

    def _is_valid_response_header(self, header: smphdr.Header) -> bool:
        """Check if the header matches expected response parameters."""
        return (
            header.group_id == self._expected_response_group_id
            and header.command_id == self._expected_response_command_id
            and header.sequence == self._expected_response_sequence
        )

    def _assemble_message_from_uplinks(self, sorted_uplinks: list) -> tuple[bytes | None, list]:
        """Assemble a complete SMP message from sorted uplinks.
        
        Returns:
            Tuple of (complete_message, remaining_uplinks)
        """
        if not sorted_uplinks:
            return None, []

        # Start with the first uplink
        payload_bytes = base64.b64decode(sorted_uplinks[0]["data"]["data"])
        self._validate_smp_header(payload_bytes)
        
        header = smphdr.Header.loads(payload_bytes[:smphdr.Header.SIZE])
        cfc_logger.debug(f"Received {header=}")
        
        message_length = header.length + header.SIZE
        cfc_logger.debug(f"Waiting for the rest of the {message_length} byte response, received {len(payload_bytes)} bytes")

        # Check if we have a complete message in the first uplink
        if len(payload_bytes) == message_length:
            cfc_logger.debug(f"Received complete message in first uplink: {payload_bytes!r}")
            if self._is_valid_response_header(header):
                return payload_bytes, sorted_uplinks[1:]  # Return remaining uplinks
            else:
                cfc_logger.debug(f"Received response with unexpected group_id or command_id: "
                                 f"{header.group_id} != {self._expected_response_group_id} or "
                                 f"{header.command_id} != {self._expected_response_command_id}, continuing to process remaining uplinks")
                return None, sorted_uplinks[1:]

        # Process additional uplinks to assemble the complete message
        remaining_uplinks = []
        for uplink in sorted_uplinks[1:]:
            more_payload_bytes = base64.b64decode(uplink["data"]["data"])
            
            # If we don't have a valid header yet, try to get one from this uplink
            if header is None:
                self._validate_smp_header(more_payload_bytes)
                header = smphdr.Header.loads(more_payload_bytes[:smphdr.Header.SIZE])
                payload_bytes = more_payload_bytes
                cfc_logger.debug(f"Received new {header=}")

                # Validate header matches expected response
                if not (header.group_id == self._expected_response_group_id and 
                       header.command_id == self._expected_response_command_id):
                    cfc_logger.debug(
                        f"Received response with unexpected group_id or command_id: "
                        f"{header.group_id} != {self._expected_response_group_id} or "
                        f"{header.command_id} != {self._expected_response_command_id}"
                    )
                    remaining_uplinks.append(uplink)
                    continue

                message_length = header.length + header.SIZE
                cfc_logger.debug(f"Waiting for the rest of the new {message_length} byte response")
            else:
                cfc_logger.debug(f"Received more payload: {more_payload_bytes!r}")
                payload_bytes += more_payload_bytes

            # Check if we have received the full message
            if len(payload_bytes) == message_length:
                cfc_logger.debug(f"Received full message: {payload_bytes!r}")
                if self._is_valid_response_header(header):
                    return payload_bytes, remaining_uplinks
                else:
                    cfc_logger.debug(
                        f"Sequence number mismatch: {header.sequence} != {self._expected_response_sequence}"
                    )
                    header = None
                    remaining_uplinks.append(uplink)
                    continue
            elif len(payload_bytes) > message_length:
                raise SMPChirpstackFuotaTransportException(
                    f"Received too much data: {payload_bytes!r}"
                )

        # If we get here, we couldn't assemble a complete message
        # Return all uplinks as remaining for next iteration
        return None, sorted_uplinks

    async def receive_unicast(
        self, after_epoch: int, dev_eui: str, fport: int, timeout_s: float
    ) -> bytes | None:
        """Receive unicast data from a device, assembling complete SMP messages from uplinks.
        
        Args:
            after_epoch: Timestamp after which to look for messages
            dev_eui: Device EUI to receive from
            fport: Port number to filter messages
            timeout_s: Timeout in seconds
            
        Returns:
            Complete SMP message bytes or None if timeout
        """
        start_time = time.time()
        current_after_epoch = after_epoch
        cfc_logger.debug(f"Receiving unicast data from device {dev_eui}, after {after_epoch} seconds")

        # Validate device ID
        dev_id = self._validate_device_id(dev_eui)
        no_uplink_received_count = 0

        while True:
            # Check timeout
            if time.time() - start_time > timeout_s:
                break

            # Send nudge if needed
            if self._should_send_nudge(no_uplink_received_count):
                await self._send_nudge(dev_eui)
                no_uplink_received_count = 0
            else:
                no_uplink_received_count += 1

            # Get pending uplinks from previous iterations
            pending_uplinks = self._get_pending_uplinks(dev_eui)
            
            # Get and sort new uplinks
            sorted_uplinks = self._get_sorted_uplinks(dev_id, fport, current_after_epoch)
            
            # Filter out already processed uplinks and get latest timestamp
            filtered_uplinks, latest_timestamp_epoch = self._filter_unprocessed_uplinks(sorted_uplinks)
            
            # Combine pending and new uplinks
            all_uplinks = pending_uplinks + filtered_uplinks
            
            if not all_uplinks:
                cfc_logger.debug("No messages received yet")
                await asyncio.sleep(5)
                continue

            cfc_logger.debug(f"Processing {len(all_uplinks)} total uplinks ({len(pending_uplinks)} pending + {len(filtered_uplinks)} new)")

            # Try to assemble a complete message
            complete_message, remaining_uplinks = self._assemble_message_from_uplinks(all_uplinks)
            
            # Mark new uplinks as processed (but not pending ones)
            if filtered_uplinks:
                self._mark_uplinks_as_processed(filtered_uplinks)
            
            # Update the after_epoch to the latest timestamp we've seen
            if latest_timestamp_epoch > current_after_epoch:
                current_after_epoch = latest_timestamp_epoch
                cfc_logger.debug(f"Updated after_epoch to: {current_after_epoch}")
            
            if complete_message is not None:
                # Successfully assembled a message, clear pending uplinks
                self._clear_pending_uplinks(dev_eui)
                return complete_message
            
            # Couldn't assemble a complete message, save remaining uplinks for next iteration
            if remaining_uplinks:
                self._add_pending_uplinks(dev_eui, remaining_uplinks)
                cfc_logger.debug(f"Saved {len(remaining_uplinks)} uplinks for next iteration")
            else:
                # No remaining uplinks, clear pending list
                self._clear_pending_uplinks(dev_eui)

            await asyncio.sleep(5)

        return None

    async def receive_unicast_old(
        self, after_epoch: int, dev_eui: str, fport: int, timeout_s: float
    ) -> bytes | None:

        start_time = time.time()
        cfc_logger.debug(
            f"Receiving unicast data from device {dev_eui}, after {after_epoch} seconds"
        )

        dev_id = self.find_dev_id_by_dev_eui(dev_eui)
        if dev_id is None:
            raise SMPChirpstackFuotaTransportException(f"Failed to find device ID for {dev_eui}")

        no_uplink_received_count = 0

        while True:
            diff = time.time() - start_time

            # Check if we need to send a random unicast to trigger response
            if no_uplink_received_count >= 4:
                cfc_logger.debug(
                    "No uplinks received for 4 ticks (20 seconds), sending random unicast"
                )
                random_payload = b'\x00\x01\x02\x03\x04'  # 5 random bytes
                await self.send_unicast(dev_eui, random_payload, 4)  # Send to fport 4
                no_uplink_received_count = 0
            else:
                no_uplink_received_count += 1

            if diff > timeout_s:
                break

            cfc_logger.debug(f"diff: {diff} seconds, timeout: {timeout_s} seconds")

            # Get the messages
            uplinks = self.get_messages_by_dev_id(dev_id, 'uplink', fport, after_epoch)

            sorted_uplinks = sorted(
                uplinks['events'],
                key=lambda x: datetime.fromisoformat(x['data']['time'].replace('Z', '+00:00')),
            )

            if len(sorted_uplinks) == 0:
                cfc_logger.debug(f"No messages received yet")
                await asyncio.sleep(5)
                continue

            cfc_logger.debug(f"len(sorted_uplinks): {len(sorted_uplinks)}")

            payload_bytes = base64.b64decode(sorted_uplinks[0]["data"]["data"])

            if len(payload_bytes) < smphdr.Header.SIZE:
                raise SMPChirpstackFuotaTransportException(
                    f"Buffer contents not big enough for SMP header: {payload_bytes=}"
                )

            # Validate that this looks like a valid SMP message
            if len(payload_bytes) > 0:
                first_byte = payload_bytes[0]
                op_value = first_byte & 0x07  # Extract OP field (bits 0-2)
                if op_value > 3:  # Valid OP values are 0-3
                    cfc_logger.warning(
                        f"Received non-SMP data (invalid OP {op_value}): {payload_bytes}"
                    )
                    raise SMPChirpstackFuotaTransportException(
                        f"Device sent non-SMP data instead of SMP response: {payload_bytes}"
                    )

            header = smphdr.Header.loads(payload_bytes[: smphdr.Header.SIZE])
            cfc_logger.debug(f"Received {header=}")

            message_length = header.length + header.SIZE
            cfc_logger.debug(f"Waiting for the rest of the {message_length} byte response")

            if len(payload_bytes) == message_length:
                cfc_logger.debug(f"Received full message: {payload_bytes}")
                # Additionally, make sure the sequence numbers match the expected value
                if header.sequence == self._expected_response_sequence:
                    return payload_bytes
                else:
                    cfc_logger.debug(
                        f"Sequence number mismatch: {header.sequence} != {self._expected_response_sequence}"
                    )
                    header = None
                    if len(sorted_uplinks) == 1:
                        cfc_logger.debug(f"Received only one payload, waiting for more")
                        await asyncio.sleep(5)
                        continue

            for uplink in sorted_uplinks[1:]:
                more_payload_bytes = base64.b64decode(uplink["data"]["data"])
                if header is None:
                    header = smphdr.Header.loads(more_payload_bytes[: smphdr.Header.SIZE])
                    payload_bytes = more_payload_bytes
                    cfc_logger.debug(f"Received new {header=}")

                    if (
                        header.group_id != self._expected_response_group_id
                        or header.command_id != self._expected_response_command_id
                    ):
                        cfc_logger.debug(
                            f"Received response with unexpected group_id or command_id: {header.group_id} != {self._expected_response_group_id} or {header.command_id} != {self._expected_response_command_id}"
                        )
                        continue

                    message_length = header.length + header.SIZE
                    cfc_logger.debug(
                        f"Waiting for the rest of the new {message_length} byte response"
                    )
                else:
                    cfc_logger.debug(f"Received more payload: {more_payload_bytes}")
                    payload_bytes += more_payload_bytes

                # Check if we have received the full message
                if len(payload_bytes) == message_length:
                    cfc_logger.debug(f"Received full message: {payload_bytes}")
                    # Additionally, make sure the sequence numbers match the expected value
                    if header.sequence == self._expected_response_sequence:
                        return payload_bytes
                    else:
                        cfc_logger.debug(
                            f"Sequence number mismatch: {header.sequence} != {self._expected_response_sequence}"
                        )
                        header = None
                        continue
                elif len(payload_bytes) > message_length:
                    raise SMPChirpstackFuotaTransportException(
                        f"Received too much data: {payload_bytes=}"
                    )

            await asyncio.sleep(5)
            continue

        return None

    async def send_multicast(self, data: bytes) -> None:
        self._send_start_time = time.time()
        downlink_stats = ChirpstackFuotaDownlinkStats()
        cfc_logger.info(f"Sending {len(data)} B")
        cfc_logger.debug(f"Mtu: {self._mtu}")
        self._send_max_duration_s = max(500.0, 500.0 * (len(data) / self._mtu))
        cfc_logger.info(f"send_max_duration_s: {self._send_max_duration_s}")
        deployment_config = FuotaUtils.create_deployment_config(
            multicast_timeout=chirpstack_fuota_configurations[self._multicast_group_type][
                self._downlink_speed
            ]["multicast_timeout"],
            unicast_timeout=chirpstack_fuota_configurations[self._multicast_group_type][
                self._downlink_speed
            ]["unicast_timeout"],
            fragmentation_fragment_size=chirpstack_fuota_configurations[self._multicast_group_type][
                self._downlink_speed
            ]["fragmentation_fragment_size"],
            fragmentation_redundancy=chirpstack_fuota_configurations[self._multicast_group_type][
                self._downlink_speed
            ]["fragmentation_redundancy"],
            multicast_ping_slot_period=chirpstack_fuota_configurations[self._multicast_group_type][
                self._downlink_speed
            ]["multicast_ping_slot_period"],
            unicast_attempt_count=3,
        )
        for offset in range(0, len(data), self._mtu):
            cfc_logger.info(f"Creating deployment for offset {offset}")

            try:
                # Create the deployment
                deployment_response = self._fuota_service.create_deployment(
                    application_id=self._chirpstack_server_app_id,
                    devices=self._matched_devices,
                    multicast_group_type=self._multicast_group_type,
                    multicast_dr=chirpstack_fuota_configurations[self._multicast_group_type][
                        self._downlink_speed
                    ]["multicast_dr"],
                    multicast_frequency=923300000,
                    multicast_group_id=0,
                    multicast_region=self._multicast_region,
                    request_fragmentation_session_status="AFTER_SESSION_TIMEOUT",
                    payload=data[offset : offset + self.mtu],
                    **deployment_config,
                )
            except Exception as e:
                raise SMPChirpstackFuotaTransportException(
                    f"Failed create deployment for offset {offset}: {str(e)}"
                )

            cfc_logger.debug(f"Created FUOTA deployment with ID: {deployment_response.id}")

            deployment_completed = False

            timeout_seconds = self.get_multicast_timeout_seconds(
                self._multicast_group_type, self._downlink_speed
            )

            cfc_logger.debug(f"Sleeping for {timeout_seconds} seconds")
            await asyncio.sleep(timeout_seconds)

            cfc_logger.debug(f"Getting deployment status")

            try:
                while not deployment_completed:
                    # Get deployment status
                    status_response = await self.get_deployment_status(deployment_response.id)
                    cfc_logger.debug(f"status_response: {status_response}")
                    deployment_completed = self.check_status_response(
                        status_response, downlink_stats
                    )

                    if not deployment_completed:
                        if time.time() - self._send_start_time > self._send_max_duration_s:
                            raise SMPChirpstackFuotaTransportException(
                                f"Deployment timeout exceeded"
                            )

                    await asyncio.sleep(self._timeout_s)
            except Exception as e:
                # Don't raise an exception (for now). Let the higher layers fail
                # This is because the FUOTA server can sometimes be wrong
                # about the status of the deployment, and we should get a confirmation about
                # this fragment upload from the (out of band) Unicast uplink anyway
                # Belt and Suspenders...
                cfc_logger.warning(f"Failed to get deployment status: {str(e)}")

        cfc_logger.info(f"Sent {len(data)} B")
        cfc_logger.info(f"Downlink stats: {downlink_stats}")
        if downlink_stats._total_time > 0:
            cfc_logger.info(
                f"Effective Data Rate: { len(data) / downlink_stats._total_time:.4f} B/s"
            )

    @override
    async def send(self, data: bytes) -> None:
        cfc_logger.debug(f"Sending {len(data)} B")
        req_header = smphdr.Header.loads(data[: smphdr.Header.SIZE])
        cfc_logger.debug(f"Header {req_header=}")
        self._expected_response_group_id = req_header.group_id
        self._expected_response_command_id = req_header.command_id

        if (
            req_header.group_id == smphdr.GroupId.IMAGE_MANAGEMENT
            and req_header.command_id == CommandId.ImageManagement.UPLOAD
        ):
            # First, extract the offset value (in case we fail to get a response)
            image_upload_write_request = smpimg.ImageUploadWriteRequest.loads(data)
            self._off = image_upload_write_request.off
            cfc_logger.debug("Sending ImageUploadWriteRequest")
            # Send the data as a multicast downlink
            await self.send_multicast(data)
        else:
            # Send the data as unicast downlinks to each of the matched devices
            for device in self._matched_devices:
                cfc_logger.debug(f"Sending to device {device['dev_eui']}")
                await self.send_unicast(device["dev_eui"], data, 2)

    @override
    async def receive(self) -> bytes:
        cfc_logger.debug("Receiving data")
        # Received unicast data from each of the matched devices - This will probably break if there are multiple devices
        for device in self._matched_devices:
            cfc_logger.debug(f"Receiving from device {device['dev_eui']}")
            data = await self.receive_unicast(int(self._last_send_time), device["dev_eui"], 2, 360)
            if data is not None:
                cfc_logger.debug(f"Received {len(data)} B")
                return data

        cfc_logger.debug(f"No data received")
        raise SMPChirpstackFuotaTransportException(f"No data received")

    @override
    async def send_and_receive(self, data: bytes) -> bytes:
        cfc_logger.debug(f"Sending and receiving {len(data)} B")
        # Make the last send time 60 seconds *before* the current time (to fix variations in time)
        self._last_send_time = time.time() - 60.0
        await self.send(data)
        header = smphdr.Header.loads(data[: smphdr.Header.SIZE])
        self._expected_response_sequence = header.sequence
        try:
            return await self.receive()
        except SMPChirpstackFuotaTransportException as e:
            cfc_logger.error(f"Failed to receive data: {str(e)}")
            cfc_logger.debug("Sending ImageUploadWriteResponse with the same offset as the request")
            cfc_logger.debug(f"{self._expected_response_sequence=}")
            cfc_logger.debug(f"{self._off=}")
            # Create the response
            response = smpimg.ImageUploadWriteResponse(
                sequence=self._expected_response_sequence, off=self._off
            )
            return response.BYTES

    @property
    def max_unencoded_size(self) -> int:
        return self._mtu

    @property
    def mtu(self) -> int:
        return self._mtu

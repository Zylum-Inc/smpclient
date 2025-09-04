#!/usr/bin/env python3
"""
Chirpstack FUOTA Deployment Status CLI

A standalone command line application that reads deployment status
from a Chirpstack FUOTA server given a configuration file and deployment ID.

This is a development tool and requires dev dependencies to be installed.
Install with: poetry install --with dev
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Handle TOML parsing for different Python versions
if sys.version_info >= (3, 11):
    import tomllib

    def load_toml(file_path: Path) -> Dict:
        with open(file_path, 'rb') as f:
            return tomllib.load(f)

else:
    try:
        import tomli

        def load_toml(file_path: Path) -> Dict:
            with open(file_path, 'rb') as f:
                return tomli.load(f)

    except ImportError:
        print("Error: 'tomli' package is required for Python < 3.11.")
        print("This is a development tool. Install dev dependencies with:")
        print("  poetry install --with dev")
        print("Or install manually with:")
        print("  pip install tomli")
        sys.exit(1)

try:
    import typer
except ImportError:
    print("Error: 'typer' package is required.")
    print("This is a development tool. Install dev dependencies with:")
    print("  poetry install --with dev")
    print("Or install manually with:")
    print("  pip install typer")
    sys.exit(1)

from chirpstack_fuota_client import FuotaService, FuotaUtils
from typing_extensions import Annotated

from smpclient.transport.chirpstack_fuota import (
    ChirpstackFuotaDownlinkSpeed,
    ChirpstackFuotaMulticastGroupTypes,
    ChirpstackFuotaRegionNames,
    DeploymentDevice,
    SMPChirpstackFuotaTransport,
    SMPChirpstackFuotaTransportException,
)

app = typer.Typer(
    name="chirpstack-fuota-status",
    help="Get deployment status from Chirpstack FUOTA server",
    no_args_is_help=True,
)


def load_config(config_file: Path) -> Dict:
    """Load configuration from TOML file."""
    if not config_file.exists():
        typer.echo(f"Error: Configuration file {config_file} not found", err=True)
        raise typer.Exit(1)

    try:
        return load_toml(config_file)
    except Exception as e:
        typer.echo(f"Error: Failed to parse TOML file: {e}", err=True)
        raise typer.Exit(1)


def validate_config(config: Dict) -> None:
    """Validate required configuration fields."""
    required_sections = ["chirpstack", "fuota"]
    missing_sections = [section for section in required_sections if section not in config]
    if missing_sections:
        typer.echo(f"Error: Missing required configuration sections: {missing_sections}", err=True)
        raise typer.Exit(1)

    # Validate chirpstack section
    chirpstack_required = ["server_addr", "api_token"]
    missing_chirpstack = [
        field for field in chirpstack_required if field not in config["chirpstack"]
    ]
    if missing_chirpstack:
        typer.echo(f"Error: Missing required chirpstack fields: {missing_chirpstack}", err=True)
        raise typer.Exit(1)

    # Validate fuota section
    fuota_required = ["server_addr", "app_id"]
    missing_fuota = [field for field in fuota_required if field not in config["fuota"]]
    if missing_fuota:
        typer.echo(f"Error: Missing required fuota fields: {missing_fuota}", err=True)
        raise typer.Exit(1)


def create_transport_from_config(config: Dict) -> SMPChirpstackFuotaTransport:
    """Create SMPChirpstackFuotaTransport from configuration."""

    # Parse devices from deployment_devices
    devices: List[DeploymentDevice] = []
    if "deployment_devices" in config["fuota"]:
        for device_config in config["fuota"]["deployment_devices"]:
            devices.append(
                {
                    "dev_eui": device_config["dev_eui"],
                    "gen_app_key": device_config.get("gen_app_key", ""),
                }
            )

    # Parse multicast group type
    multicast_group_type = ChirpstackFuotaMulticastGroupTypes.CLASS_C
    if "multicast_group_type" in config["fuota"]:
        try:
            multicast_group_type = ChirpstackFuotaMulticastGroupTypes(
                config["fuota"]["multicast_group_type"]
            )
        except ValueError:
            typer.echo(
                f"Warning: Invalid multicast_group_type '{config['fuota']['multicast_group_type']}', using CLASS_C"
            )

    # Parse multicast region (default to US915 if not specified)
    multicast_region = ChirpstackFuotaRegionNames.US_915
    if "multicast_region" in config["fuota"]:
        try:
            multicast_region = ChirpstackFuotaRegionNames(config["fuota"]["multicast_region"])
        except ValueError:
            typer.echo(
                f"Warning: Invalid multicast_region '{config['fuota']['multicast_region']}', using US915"
            )

    # Parse downlink speed
    downlink_speed = ChirpstackFuotaDownlinkSpeed.DL_SLOW
    if "downlink_speed" in config["fuota"]:
        try:
            downlink_speed = ChirpstackFuotaDownlinkSpeed(config["fuota"]["downlink_speed"])
        except ValueError:
            typer.echo(
                f"Warning: Invalid downlink_speed '{config['fuota']['downlink_speed']}', using DL_SLOW"
            )

    # Get TAS configuration if available
    tas_api_addr = "localhost:8002"
    tas_api_lns_id = ""
    if "tas" in config:
        tas_api_addr = config["tas"].get("server_addr", "localhost:8002")
        tas_api_lns_id = config["tas"].get("lns_id", "")

    return SMPChirpstackFuotaTransport(
        mtu=config["fuota"].get("mtu", 1024),
        multicast_group_type=multicast_group_type,
        multicast_region=multicast_region,
        chirpstack_server_addr=config["chirpstack"]["server_addr"],
        chirpstack_server_api_token=config["chirpstack"]["api_token"],
        chirpstack_server_app_id=config["fuota"]["app_id"],
        devices=devices,
        chirpstack_fuota_server_addr=config["fuota"]["server_addr"],
        send_max_duration_s=config["fuota"].get("send_max_duration_s", 3600.0),
        downlink_speed=downlink_speed,
        tas_api_addr=tas_api_addr,
        tas_api_lns_id=tas_api_lns_id,
    )


async def get_deployment_status_async(
    transport: SMPChirpstackFuotaTransport, deployment_id: str
) -> Dict:
    """Get deployment status asynchronously."""
    # Initialize the FUOTA service directly since we only need status, not full transport
    fuota_service = FuotaService(
        transport._chirpstack_fuota_server_addr, transport._chirpstack_server_api_token
    )
    transport._fuota_service = fuota_service

    return await transport.get_deployment_status(deployment_id)


@app.command()
def status(
    config_file: Annotated[
        Path, typer.Argument(help="Path to the chirpstack_fuota.toml configuration file")
    ],
    deployment_id: Annotated[str, typer.Argument(help="The deployment ID to query")],
    output_format: Annotated[str, typer.Option("--format", "-f", help="Output format")] = "json",
    pretty: Annotated[
        bool, typer.Option("--pretty", "-p", help="Pretty print JSON output")
    ] = False,
    output_file: Annotated[
        Optional[Path], typer.Option("--output", "-o", help="Write output to file")
    ] = None,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Enable verbose logging")
    ] = False,
) -> None:
    """Get deployment status and device logs for a Chirpstack FUOTA deployment."""

    if verbose:
        import logging

        logging.basicConfig(level=logging.DEBUG)

    # Load and validate configuration
    typer.echo(f"Loading configuration from {config_file}...")
    config = load_config(config_file)
    validate_config(config)

    # Create transport
    try:
        typer.echo("Creating Chirpstack FUOTA transport...")
        transport = create_transport_from_config(config)
    except Exception as e:
        typer.echo(f"Error: Failed to create transport: {e}", err=True)
        raise typer.Exit(1)

    # Get deployment status
    try:
        typer.echo(f"Fetching deployment status for ID: {deployment_id}...")
        status_response = asyncio.run(get_deployment_status_async(transport, deployment_id))
    except SMPChirpstackFuotaTransportException as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: Failed to get deployment status: {e}", err=True)
        raise typer.Exit(1)

    # Format output
    if output_format.lower() == "json":
        if pretty:
            output = json.dumps(status_response, indent=2, sort_keys=True)
        else:
            output = json.dumps(status_response)
    else:
        typer.echo(
            f"Error: Unsupported output format '{output_format}'. Only 'json' is supported.",
            err=True,
        )
        raise typer.Exit(1)

    # Write output
    if output_file:
        try:
            output_file.write_text(output)
            typer.echo(f"Status written to {output_file}")
        except Exception as e:
            typer.echo(f"Error: Failed to write to {output_file}: {e}", err=True)
            raise typer.Exit(1)
    else:
        typer.echo(output)


@app.command()
def config_template() -> None:
    """Generate a sample configuration file template."""
    template_lines = [
        "# ChirpStack Fuota SMPTransport configuration",
        "[chirpstack]",
        'server_addr = "localhost:8080"',
        'api_token = "your_api_token_here"',
        "",
        "[fuota]",
        'server_addr = "localhost:8070"',
        '# The app_id with which the devices are associated',
        'app_id = "your_app_id_here"',
        '# The list of dev_eui, gen_app_key pairs to send the firmware chunks to',
        '# [{dev_eui = "", gen_app_key = ""}, ...]',
        'deployment_devices = [{dev_eui="e3ab5182159e6599", gen_app_key="d8548200000000000000000000000001"}]',
        "",
        'downlink_speed = "DL_SLOW"',
        'multicast_group_type = "CLASS_C"',
        "",
        "[tas]",
        "# Needed to get (unicast) uplink messages from the chirpstack connected devices",
        'server_addr = "http://localhost:8002"',
        'lns_id = "your_lns_id_here"',
    ]

    for line in template_lines:
        typer.echo(line)


@app.command()
def list_options() -> None:
    """List available configuration options."""
    typer.echo("Available multicast group types:")
    for group_type in ChirpstackFuotaMulticastGroupTypes.list():
        typer.echo(f"  - {group_type}")

    typer.echo("\nAvailable multicast regions:")
    for region in ChirpstackFuotaRegionNames.list():
        typer.echo(f"  - {region}")

    typer.echo("\nAvailable downlink speeds:")
    for speed in ChirpstackFuotaDownlinkSpeed.list():
        typer.echo(f"  - {speed}")


if __name__ == "__main__":
    app()

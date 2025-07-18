from __future__ import annotations

import json
import os
import re
import signal
import time
from datetime import datetime, timezone

import rdflib.term
import requests
from rdflib import Graph
from termcolor import colored

from qlever.command import QleverCommand
from qlever.log import log
from qlever.util import run_command

from qlever.containerize import Containerize


class OsmUpdateCommand(QleverCommand):
    """
    Class for executing the `osm-update` command.
    """

    def __init__(self):
        self.planet_replication_server_url = \
            "https://planet.osm.org/replication/"

    def description(self) -> str:
        return "Update OSM data for a given dataset"

    def should_have_qleverfile(self) -> bool:
        return True

    def relevant_qleverfile_arguments(self) -> dict[str: list[str]]:
        return {"data": ["name"],
                "server": ["host_name", "port", "access_token"]}

    def additional_arguments(self, subparser) -> None:
        subparser.add_argument(
            "granularity",
            nargs=1,
            choices=["minute", "hour", "day"],
            type=str,
            help="The interval in which the OSM data should be updated."
                 "Choose from 'minute', 'hour', or 'day'.",
        )
        subparser.add_argument(
            "--polyfile",
            nargs='?',
            type=str,
            help="The poly file that defines the boundaries of your osm "
                 "dataset. (Poly files for country extracts are available at "
                 "https://download.geofabrik.de/) If no poly file is provided,"
                 " the complete osm planet data will be used.",
        )
        subparser.add_argument(
            "--replication-server",
            nargs='?',
            type=str,
            help="The URL of the OSM replication server to use. By default, "
                 "the OSM planet replication server "
                 "('https://planet.osm.org/replication/) is used."
        )

    def execute(self, args) -> bool:
        # If the user has specified a replication server, use that one,
        # otherwise we use the planet replication server with the specified
        # granularity.
        granularity = args.granularity[0]
        if args.replication_server:
            replication_server = args.replication_server
        else:
            replication_server = (f"{self.planet_replication_server_url}"
                                  f"{granularity}/")

        cmd_description = []
        cmd_description.append(
            f"Update OSM data for dataset '{args.name}' with granularity "
            f"'{granularity}' from the OSM replication server "
            f"'{replication_server}'."
        )
        self.show("\n".join(cmd_description), only_show=args.show)

        if not self.execute_olu(replication_server, args):
            return False

        return True

    def execute_olu(self, replication_server_url: str, args) -> bool:
        sparql_endpoint = f"http://{args.host_name}:{args.port}"
        container_name = f"olu-{args.name}"

        olu_cmd = f"{sparql_endpoint}"
        olu_cmd += f" -a {args.access_token}"
        olu_cmd += f" -f {replication_server_url}"
        olu_cmd += f" --qlever"

        # If the user has specified a polygon file, we add it to the command.
        if args.polyfile:
            # Check if polygon file exists
            if not os.path.exists(args.polyfile):
                log.error(f'No file matching "{args.polyfile}" found')
                log.info("")
                log.info("Check if the polyfile exists and if the path is "
                         "correct.")
                return False

            olu_cmd += f" --polygon {args.polyfile}"

        olu_cmd = Containerize().containerize_command(
            olu_cmd,
            "docker",
            "run --rm",
            "olu:latest",
            container_name,
            volumes=[("$(pwd)", "/update")],
            working_directory="/update",
            use_bash=False
        )

        self.show(f"{olu_cmd}", only_show=args.show)
        if args.show:
            return True

        try:
            run_command(olu_cmd, show_output=True)
        except Exception as e:
            log.error(f"Error running osm-live-updates: {e}")
            return False

        return True

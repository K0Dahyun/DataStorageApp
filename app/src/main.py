# Copyright (c) 2022 Robert Bosch GmbH and Microsoft Corporation
#
# This program and the accompanying materials are made available under the
# terms of the Apache License, Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import csv
import json
import logging
import signal
import time
from collections import deque

from vehicle import Vehicle, vehicle  # type: ignore
from velocitas_sdk.util.log import (  # type: ignore
    get_opentelemetry_log_factory,
    get_opentelemetry_log_format,
)
from velocitas_sdk.vdb.reply import DataPointReply
from velocitas_sdk.vehicle_app import VehicleApp, subscribe_topic

# Configure the VehicleApp logger with the necessary log config and level.
logging.setLogRecordFactory(get_opentelemetry_log_factory())
logging.basicConfig(format=get_opentelemetry_log_format())
logging.getLogger().setLevel("DEBUG")
logger = logging.getLogger(__name__)

SET_CRASH_EVENT_TOPIC = "crashdetect/crashed"
SET_CRASH_RESPONSE_TOPIC = "crashdetect/crashed/response"

CSV_FILE_NAME_TOPIC = "vehicle_data.csv"
CSV_FILE_PATH = "."


class DataStorageApp(VehicleApp):
    def __init__(self, vehicle_client: Vehicle):
        super().__init__()
        self.Vehicle = vehicle_client
        self.distance = 0
        self.accel = 0
        self.speed = 0
        self.displacement = 0

        self.storage = deque([])

    async def on_start(self):
        await self.Vehicle.ADAS.ObstacleDetection.IsWarning.subscribe(
            self.on_distance_change
        )
        await self.Vehicle.Acceleration.Longitudinal.subscribe(self.on_accel_change)
        await self.Vehicle.Speed.subscribe(self.on_speed_change)
        await self.Vehicle.Powertrain.CombustionEngine.Displacement.subscribe(
            self.on_displacement_change
        )
        await self.timer_for_csv()

    async def on_distance_change(self, data: DataPointReply):
        self.distance = data.get(self.Vehicle.ADAS.ObstacleDetection.IsWarning).value

    async def on_accel_change(self, data: DataPointReply):
        self.accel = data.get(self.Vehicle.Acceleration.Longitudinal).value

    async def on_speed_change(self, data: DataPointReply):
        self.speed = data.get(self.Vehicle.Speed).value

    async def on_displacement_change(self, data: DataPointReply):
        self.displacement = data.get(
            self.Vehicle.Powertrain.CombustionEngine.Displacement
        ).value

    async def timer_for_csv(self):
        while True:
            temp = [
                time.time(),
                self.distance,
                self.accel,
                self.speed,
                self.displacement,
            ]
            if len(self.storage) == 300:
                self.storage.popleft()

            self.storage.append(temp)
            time.sleep(0.1)

    @subscribe_topic(SET_CRASH_EVENT_TOPIC)
    async def on_crash_event_received(self, data_str: str) -> None:
        logger.info("Data Storaging...")
        data = json.loads(data_str)
        if data["status"] == 0:
            return
        logger.info("Data Storaging...")
        with open(CSV_FILE_NAME_TOPIC, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["timestamp", "distance", "accel", "speed", "displacement"])
            for entry in self.storage:
                writer.writerow(entry)

        self.storage.clear()

        await self.set_response(0)

    async def set_response(self, status):
        await self.publish_event(
            SET_CRASH_RESPONSE_TOPIC,
            json.dumps(
                {
                    "result": {
                        "status": status,
                    },
                }
            ),
        )


async def main():
    """Main function"""
    logger.info("Starting DataStorageApp...")
    vehicle_app = DataStorageApp(vehicle)
    await vehicle_app.run()


LOOP = asyncio.get_event_loop()
LOOP.add_signal_handler(signal.SIGTERM, LOOP.stop)
LOOP.run_until_complete(main())
LOOP.close()

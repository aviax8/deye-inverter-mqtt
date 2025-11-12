# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging

from deye_events import DeyeEventList, DeyeEventProcessor, DeyeObservationEvent
from deye_config import DeyeLoggerConfig
from deye_observation import Observation
from deye_sensor import GroupSensor, Sensor


class DeyeGroupSensorEvaluator(DeyeEventProcessor):
    """
    Evaluates data for group sensors.
    """

    def __init__(self, logger_config: DeyeLoggerConfig, sensor_list: list[Sensor]):
        self.__log = logger_config.logger_adapter(logging.getLogger(DeyeGroupSensorEvaluator.__name__))

        # Select group aggregation sensors
        group_sensors: list[GroupSensor] = [
            s for s in sensor_list if isinstance(s, GroupSensor)
        ]

        # Assign matching sensors to each group
        for group_sensor in group_sensors:
            group_sensor.set_matching_sensors(sensor_list)

    def get_id(self) -> str:
        return "group_sensor_evaluator"

    def get_description(self) -> str:
        return "Group sensors data into a one observation/topic"

    def process(self, events: DeyeEventList):
        # Select observations
        observations: list[Observation] = [
            e.observation for e in events if isinstance(e, DeyeObservationEvent)
        ]

        # Select group sensor observations
        group_observations: list[GroupSensor] = [
            o for o in observations if isinstance(o.sensor, GroupSensor)
        ]

        for group_observation in group_observations:
            data_parts = []
            for item_observation in observations:
                if not group_observation.sensor.has_sensor(item_observation.sensor):
                    continue
                if item_observation.value is None:
                    continue

                key = item_observation.sensor.name.replace(" ", "_")
                val = item_observation.sensor.format_value(item_observation.value)
                data_parts.append(f'"{key}": {val}')

            # JSON string
            data = "{ " + ", ".join(data_parts) + " }"

            group_observation.value = data

            self.__log.debug(f"Created logger {events.logger_index} group observation for {group_observation.sensor.name}: {data}")



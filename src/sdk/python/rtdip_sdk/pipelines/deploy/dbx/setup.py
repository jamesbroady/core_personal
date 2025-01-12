# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from setuptools import find_packages, setup

PACKAGE_REQUIREMENTS = ["pyyaml"]

setup(
    name=os.environ.get("RTDIP_PACKAGE_NAME"),
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    entry_points = {
        "console_scripts": [
            "pipeline = dbx.rtdip.tasks.pipeline_task:entrypoint",
    ]},
    version=os.environ.get("RTDIP_PACKAGE_VERSION"),
    description=os.environ.get("RTDIP_PACKAGE_DESCRIPTION"),
    author="",
)
#!/usr/bin/env bash

# Copyright 2025 Google LLC
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

# This script is designed to be run within a Docker container. It creates
# a PyPI package from the files prepared by the build.sh script.

set -euxo pipefail

# Generate setup.py file
cat <<EOF | sed "s/{PACKAGE_VERSION}/$PACKAGE_VERSION/g" > /build/pkg/setup.py
from setuptools import setup, find_packages

setup(
    name="arolla-test",
    version="{PACKAGE_VERSION}",
    package_data={"arolla": ["*.so", "*/*.so", "*/*/*.so", "*/*/*/*.so"]},
    packages=find_packages(include=["arolla", "arolla.*"]),
    install_requires=[
        'protobuf>=5.26.1,<6.0.0dev',
    ],
    zip_safe=False,
)
EOF

# Build PyPI package
cd /build/pkg
python3 setup.py clean --all
python3 setup.py bdist_wheel --plat-name=manylinux_2_35_x86_64 --python-tag=cp311

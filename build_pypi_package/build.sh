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


# This script is designed to be run within a Docker container. It prepares
# the files for the PyPI package in the `/build/pkg/arolla` directory.
#
# The versions of the dependencies and toolchain are selected to be compatible
# with Google Colab, which is the primary target environment for the resulting
# package.

set -euxo pipefail

# Add Bazel distribution URI as a package source
# (based on https://bazel.build/install/ubuntu#install-on-ubuntu)
apt update
apt install apt-transport-https curl gnupg -y
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
mv bazel-archive-keyring.gpg /usr/share/keyrings
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list

# Install build dependencies.
apt update
apt install -y \
    bazel \
    g++-12 \
    git \
    patchelf \
    python3 \
    #

bazel_flags='-c opt'
bazel_flags+=' --enable_bzlmod'
bazel_flags+=' --cxxopt=-Wno-attributes'
bazel_flags+=' --cxxopt=-Wno-deprecated'
bazel_flags+=' --cxxopt=-Wno-deprecated-declarations'
bazel_flags+=' --cxxopt=-Wno-deprecated-enum-enum-conversion'
bazel_flags+=' --cxxopt=-Wno-maybe-uninitialized'
bazel_flags+=' --cxxopt=-Wno-parentheses'
bazel_flags+=' --cxxopt=-Wno-restrict'
bazel_flags+=' --cxxopt=-Wno-sign-compare'
bazel_flags+=' --cxxopt=-Wno-stringop-truncation'
bazel_flags+=' --cxxopt=-Wno-uninitialized'
bazel_flags+=' --cxxopt=-Wno-unknown-pragmas'
bazel_flags+=' --cxxopt=-Wno-unused-but-set-variable'

# Building a version of `protoc` compatible with Google Colab.
# (The current version is google.protobuf.__version__ == '5.29.4'.)
#             vvvv                                          ^^^^
git clone -b v29.4 --depth 1 https://github.com/protocolbuffers/protobuf.git /build/protobuf
cd /build/protobuf
bazel build $bazel_flags //src/google/protobuf/compiler:protoc
PROTOC=/build/protobuf/bazel-bin/protoc

# Building Arolla.
git clone --depth 1 https://github.com/google/arolla.git /build/arolla
cd /build/arolla

# This part is not a part of the PyPI package.
rm -rf py/arolla/codegen

env CC=gcc-12 CXX=g++-12 \
    bazel build $bazel_flags //py/arolla/...

# Prepare the package files in /build/pkg/arolla.
mkdir -p /build/pkg/arolla

# Copying the python files to /build/pkg/arolla, keeping the directory
# structure.
bazel cquery $bazel_flags \
    'config(kind(py_library, attr(testonly, 0, //py/arolla/...)), target)' \
    --output files \
| awk '{ S=$0; D=S; sub(/^(.*\/)?py\/arolla/, "/build/pkg/arolla", D); system("install -v -m 0644 -D "S" "D) }'

# Copying compiled python extensions to /build/pkg/arolla.
bazel cquery $bazel_flags \
    'config(kind(cc_binary, attr(testonly, 0, //py/arolla/...)), target)' \
    --output files \
| awk '{ S=$0; D=S; sub(/^(.*\/)?py\/arolla/, "/build/pkg/arolla", D); system("install -v -m 0644 -D "S" "D) }'

# Copying additional shared libraries to /build/pkg/arolla.
bazel cquery $bazel_flags \
    'config(kind(cc_shared_library, attr(testonly, 0, //py/arolla/...)), target)' \
    --output files \
| awk '{ S=$0; D=S; sub(/^(.*\/)?py\/arolla/, "/build/pkg/arolla", D); system("install -v -D "S" "D) }'

# Manually compile proto files for Python using a specific version of protoc.
$PROTOC --python_out=. arolla/proto/serialization_base.proto
install -v -m 0644 -D arolla/proto/serialization_base_pb2.py -t /build/pkg/arolla/proto

# Add __init__.py files
find /build/pkg/arolla -type d | awk '{ system("touch "$1"/__init__.py") }'

# With a PyPI package, we can only be sure that the package file structure will
# be preserved, but we have no control over where files are installed. This
# creates an issue for shared libraries, such as Python extensions. If
# the shared libraries depend on each other, the dynamic linker may not be able
# to automatically locate such dependencies, especially if a dependency is
# located in a different directory.
#
# To help the dynamic linker, we embed an RPATH with a relative path that points
# to the `arolla/dynamic_deps` directory.
#
# The Arolla PyPI package includes two kinds of shared libraries:
#
# 1. "Reusable" shared libraries: These libraries are installed into
#    the `arolla/dynamic_deps` directory and may depend on each other.
# 2. Python extensions: These may only depend on the "reusable" shared libraries
#    from `arolla/dynamic_deps`, but not on each other.
#
# In both cases, it's sufficient if the dynamic linker can find the dependencies
# in `arolla/dynamic_deps`.
#
for i in `find /build/pkg/arolla -name '*.so'`; do
  patchelf --set-rpath '$ORIGIN'/$(realpath --relative-to `dirname $i` /build/pkg/arolla/dynamic_deps) $i
done

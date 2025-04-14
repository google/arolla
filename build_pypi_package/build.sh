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
# the files for the PyPI package in the directory:
#
#     /build/pkg/arolla
#
# The versions of the dependencies and toolchain are selected to be compatible
# with Google Colab, which is the primary target environment for the resulting
# package.

set -euxo pipefail

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
(
  # (The current version is google.protobuf.__version__ == '5.29.4'.)
  #             vvvv                                          ^^^^
  git clone -b v29.4 --depth 1 https://github.com/protocolbuffers/protobuf.git /build/protobuf
  cd /build/protobuf
  bazel build $bazel_flags //src/google/protobuf/compiler:protoc
)
PROTOC=/build/protobuf/bazel-bin/protoc

# Prepare files in /build/pkg/arolla.
(
  # Building Arolla.
  git clone --depth 1 https://github.com/google/arolla.git /build/arolla
  cd /build/arolla

  rm -rf py/arolla/codegen  # This part is not a part of the PyPI package.

  bazel query 'kind("cc_binary|cc_shared_library", attr(testonly, 0, //py/...))' \
    | env CC=gcc-12 CXX=g++-12 xargs bazel build $bazel_flags

  # Copying the python files, compiled python extensions, and "reusable" shared
  # libraries to /build/pkg/arolla, keeping the directory structure.
  bazel cquery $bazel_flags \
    'config(kind("py_library|cc_binary|cc_shared_library", attr(testonly, 0, //py/...)), target)' --output files \
    | awk '{ S=$0; D=S; sub(/^(.*\/)?py\/arolla/, "/build/pkg/arolla", D); system("install -v -m 0644 -D "S" "D) }'

  # Manually compile proto files for Python using a specific version of protoc.
  $PROTOC --python_out=. arolla/proto/serialization_base.proto
  install -v -m 0644 -D arolla/proto/serialization_base_pb2.py -t /build/pkg/arolla/proto

  # Add __init__.py files
  find /build/pkg/arolla -type d | awk '{ system("touch "$1"/__init__.py") }'
)

# Additional adjustments for the shared libraries
(
  # With a PyPI package, we can only be sure that the package file structure
  # will be preserved, but we have no control over where files are installed.
  # This creates an issue for shared libraries, such as Python extensions. If
  # the shared libraries depend on each other, the dynamic linker may not be
  # able to automatically locate such dependencies, especially if a dependency
  # is located in a different directory.

  # The Arolla PyPI package includes two kinds of shared libraries:
  #
  # 1. "Reusable" shared libraries: These libraries are installed into
  #    the `dynamic_deps` directory and may depend on each other.
  # 2. Python extensions: These may only depend on the "reusable" shared
  #    libraries from `dynamic_deps`, but not on each other.

  # Setting a relative path to the `dynamic_deps` directory in the RPATH is
  # sufficient for the dynamic linker to find same-package dependencies.
  for i in `find /build/pkg/arolla -name '*.so'`; do
    patchelf --set-rpath '$ORIGIN'/$(realpath --relative-to `dirname $i` /build/pkg/arolla/dynamic_deps) $i
  done
)

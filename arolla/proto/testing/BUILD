# Copyright 2024 Google LLC
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

load("@rules_python//python:proto.bzl", "py_proto_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

proto_library(
    name = "test_proto",
    testonly = 1,
    srcs = ["test.proto"],
    deps = [
        ":test_proto3",
    ],
)

cc_proto_library(
    name = "test_cc_proto",
    testonly = 1,
    deps = [":test_proto"],
)

py_proto_library(
    name = "test_py_proto",
    testonly = 1,
    deps = [":test_proto"],
)

proto_library(
    name = "test_proto3",
    testonly = 1,
    srcs = ["test_proto3.proto"],
)

cc_proto_library(
    name = "test_proto3_cc_proto",
    testonly = 1,
    deps = [":test_proto3"],
)

py_proto_library(
    name = "test_proto3_py_proto",
    testonly = 1,
    deps = [":test_proto3"],
)

proto_library(
    name = "test_proto_extension",
    testonly = 1,
    srcs = ["test_extension.proto"],
    deps = [
        ":test_proto",
    ],
)

cc_proto_library(
    name = "test_extension_cc_proto",
    testonly = 1,
    deps = [":test_proto_extension"],
)

py_proto_library(
    name = "test_extension_py_proto",
    testonly = 1,
    deps = [":test_proto_extension"],
)

proto_library(
    name = "test_proto_benchmark_extension",
    testonly = 1,
    srcs = ["test_benchmark_extension.proto"],
    deps = [
        ":test_proto",
    ],
)

cc_proto_library(
    name = "test_benchmark_extension_cc_proto",
    testonly = 1,
    deps = [":test_proto_benchmark_extension"],
)

py_proto_library(
    name = "test_benchmark_extension_py_proto",
    testonly = 1,
    deps = [":test_proto_benchmark_extension"],
)

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

cc_library(
    name = "proto",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

proto_library(
    name = "io_proto",
    srcs = ["io.proto"],
)

cc_proto_library(
    name = "io_cc_proto",
    deps = [":io_proto"],
)

py_proto_library(
    name = "io_py_pb2",
    deps = [":io_proto"],
)

cc_test(
    name = "types_test",
    srcs = ["types_test.cc"],
    deps = [
        ":proto",
        ":test_cc_proto",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

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

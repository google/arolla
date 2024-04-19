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

# Base serialization facilities for arolla expressions and values.
#
# Decoder and encoder defined here know virtually nothing about actual value
# serialization. The decoder explicitly depends on ExprOperator qtype, but
# provides no serialization format for ExprOperators.
#
# All concrete types and serialization formats need to be provided externally.
#

load("@rules_python//python:proto.bzl", "py_proto_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

proto_library(
    name = "base_proto",
    srcs = [
        "base.proto",
    ],
)

cc_proto_library(
    name = "base_cc_proto",
    deps = [
        ":base_proto",
    ],
)

py_proto_library(
    name = "base_py_proto",
    deps = [":base_proto"],
)

cc_library(
    name = "serialization_base",
    srcs = [
        "decode.cc",
        "encode.cc",
    ],
    hdrs = [
        "decode.h",
        "encode.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":base_cc_proto",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "decode_test",
    srcs = [
        "decode_test.cc",
    ],
    deps = [
        ":base_cc_proto",
        ":serialization_base",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "encode_test",
    srcs = [
        "encode_test.cc",
    ],
    deps = [
        ":base_cc_proto",
        ":serialization_base",
        "//arolla/expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

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
    name = "operator_codec_proto",
    srcs = ["operator_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

proto_library(
    name = "scalar_codec_proto",
    srcs = ["scalar_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

proto_library(
    name = "optional_codec_proto",
    srcs = ["optional_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

proto_library(
    name = "tuple_codec_proto",
    srcs = ["tuple_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "operator_codec_cc_proto",
    deps = [":operator_codec_proto"],
)

cc_proto_library(
    name = "scalar_codec_cc_proto",
    deps = [":scalar_codec_proto"],
)

cc_proto_library(
    name = "optional_codec_cc_proto",
    deps = [":optional_codec_proto"],
)

cc_proto_library(
    name = "tuple_codec_cc_proto",
    deps = [":tuple_codec_proto"],
)

py_proto_library(
    name = "operator_codec_py_proto",
    testonly = False,  # Used in the operator package generation.
    deps = [":operator_codec_proto"],
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = [":__subpackages__"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

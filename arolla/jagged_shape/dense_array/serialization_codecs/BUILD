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

# Serialization codecs for jagged dense array shapes.

load("@rules_python//python:proto.bzl", "py_proto_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "serialization_codecs",
    srcs = [
        "decoder.cc",
        "encoder.cc",
    ],
    hdrs = [],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":codec_name",
        ":jagged_shape_codec_cc_proto",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/jagged_shape/dense_array",
        "//arolla/jagged_shape/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:registry",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

proto_library(
    name = "jagged_shape_codec_proto",
    srcs = ["jagged_shape_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "jagged_shape_codec_cc_proto",
    deps = [":jagged_shape_codec_proto"],
)

py_proto_library(
    name = "jagged_shape_codec_py_proto",
    testonly = True,
    deps = [":jagged_shape_codec_proto"],
)

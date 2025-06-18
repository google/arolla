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

load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:private"])

cc_library(
    name = "s11n",
    srcs = [
        "codec.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        ":codec_cc_proto",
        "//arolla/derived_qtype",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:registry",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

licenses(["notice"])

proto_library(
    name = "codec_proto",
    srcs = ["codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "codec_cc_proto",
    deps = [":codec_proto"],
)

py_proto_library(
    name = "codec_py_proto",
    testonly = True,
    visibility = ["//visibility:public"],
    deps = [":codec_proto"],
)

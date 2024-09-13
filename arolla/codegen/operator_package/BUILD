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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_python//python:proto.bzl", "py_proto_library")
load(
    "//arolla/codegen/operator_package:operator_package.bzl",
    "arolla_cc_embed_operator_package",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "operator_package",
    srcs = ["operator_package.cc"],
    hdrs = ["operator_package.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":operator_package_cc_proto",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization",
        "//arolla/serialization_codecs/generic:operator_codec_cc_proto",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//src/google/protobuf/io",
        "@com_google_protobuf//src/google/protobuf/io:gzip_stream",
    ],
)

cc_test(
    name = "operator_package_test",
    srcs = ["operator_package_test.cc"],
    deps = [
        ":operator_package",
        ":operator_package_cc_proto",
        "//arolla/expr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/serialization",
        "//arolla/serialization_codecs:all",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

proto_library(
    name = "operator_package_proto",
    srcs = ["operator_package.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "operator_package_cc_proto",
    deps = [":operator_package_proto"],
)

py_proto_library(
    name = "operator_package_py_proto",
    deps = [":operator_package_proto"],
)

exports_files(["operator_package.cc.jinja2"])

bzl_library(
    name = "operator_package_bzl",
    srcs = ["operator_package.bzl"],
    deps = ["//arolla/codegen:utils_bzl"],
)

arolla_cc_embed_operator_package(
    name = "arolla_cc_embed_operator_package_test",
    testonly = True,
    srcs = ["test.bin"],
    arolla_initializer = {
        "name": "@arolla/codegen/operator_package:test",
        "reverse_deps": [
            "@test//dependency:reverse_1",
            "@test//dependency:reverse_2",
        ],
        "deps": [
            "@test//dependency:1",
            "@test//dependency:2",
        ],
    },
)

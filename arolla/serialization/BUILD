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

# Serialization facilities for arolla expressions and values.
#
# You need to depend on codecs to use it, for example:
#
#   //arolla/serialization_codecs:all_decoders
#
# or
#
#   //arolla/serialization_codecs:all.
#

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "serialization",
    hdrs = [
        "decode.h",
        "encode.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":decode",
        ":encode",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

# TODO: Once all clients have migrated, merge this target into ":serialization".
cc_library(
    name = "decode",
    srcs = ["decode.cc"],
    hdrs = ["decode.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["avoid_dep"],
    deps = [
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

# TODO: Once all clients have migrated, merge this target into ":serialization".
cc_library(
    name = "encode",
    srcs = ["encode.cc"],
    hdrs = ["encode.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["avoid_dep"],
    deps = [
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "serialization_test",
    srcs = ["serialization_test.cc"],
    deps = [
        ":serialization",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/serialization_codecs:all",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

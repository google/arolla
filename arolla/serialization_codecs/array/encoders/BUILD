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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "encoders",
    srcs = [
        "array_encoder.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs:registry",
        "//arolla/serialization_codecs/array:array_codec_cc_proto",
        "//arolla/serialization_codecs/array:codec_name",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
    alwayslink = True,
)

# See tests in py/arolla/s11n/testing
cc_test(
    name = "array_encoder_test",
    srcs = ["array_encoder_test.cc"],
    deps = [
        ":encoders",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/qtype",
        "//arolla/serialization",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs/array:array_codec_cc_proto",
        "//arolla/serialization_codecs/dense_array/encoders",
        "//arolla/serialization_codecs/generic/encoders",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

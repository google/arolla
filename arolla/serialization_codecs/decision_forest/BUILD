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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

proto_library(
    name = "decision_forest_codec_proto",
    srcs = ["decision_forest_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "decision_forest_codec_cc_proto",
    deps = [":decision_forest_codec_proto"],
)

cc_test(
    name = "decision_forest_codec_test",
    srcs = ["decision_forest_codec_test.cc"],
    deps = [
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/serialization:decode",
        "//arolla/serialization:encode",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:all",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = [":__subpackages__"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

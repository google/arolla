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

# Proto related .

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "proto",
    srcs = [
        "proto_input_loader.cc",
    ],
    hdrs = [
        "proto_input_loader.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto",
        "//arolla/proto/reflection",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_input_loader_test",
    srcs = ["proto_input_loader_test.cc"],
    deps = [
        ":proto",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "proto_input_loader_errors_test",
    srcs = ["proto_input_loader_errors_test.cc"],
    deps = [
        ":proto",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_binary(
    name = "proto_input_loader_benchmarks",
    testonly = 1,
    srcs = ["proto_input_loader_benchmarks.cc"],
    deps = [
        ":proto",
        "//arolla/io/proto/testing",
        "//arolla/proto:test_cc_proto",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "proto_input_loader_benchmarks_smoke_test",
    binary = ":proto_input_loader_benchmarks",
)

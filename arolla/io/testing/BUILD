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

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_testonly = 1,
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = True,
    hdrs = ["matchers.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/io",
        "//arolla/qtype",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_binary(
    name = "input_loader_benchmarks",
    testonly = 1,
    srcs = ["input_loader_benchmarks.cc"],
    deps = [
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "matchers_test",
    srcs = ["matchers_test.cc"],
    deps = [
        ":testing",
        "//arolla/io",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

benchmark_smoke_test(
    name = "input_loader_benchmarks_smoke_test",
    binary = ":input_loader_benchmarks",
)

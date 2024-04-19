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

# Tools for testing QExpr

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "benchmarks.cc",
    ],
    hdrs = [
        "benchmarks.h",
        "operator_fixture.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "test_benchmarks",
    testonly = 1,
    srcs = ["test_benchmarks.cc"],
    deps = [
        ":testing",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    binary = ":test_benchmarks",
)

cc_test(
    name = "operator_fixture_test",
    srcs = ["operator_fixture_test.cc"],
    deps = [
        ":testing",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/codegen/qexpr/testing:test_operators_lib",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

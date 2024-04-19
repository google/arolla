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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "ops",
    srcs = [
        "universal_dense_op.h",
    ],
    hdrs = [
        "dense_group_ops.h",
        "dense_ops.h",
        "multi_edge_util.h",
        "util.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "util_test",
    srcs = ["util_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "multi_edge_util_test",
    srcs = ["multi_edge_util_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dense_ops_test",
    srcs = ["dense_ops_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dense_group_ops_test",
    srcs = ["dense_group_ops_test.cc"],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/testing:lib",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = [
        "benchmarks.cc",
    ],
    deps = [
        ":ops",
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_DenseOp_AddFull/32",
    binary = ":benchmarks",
)

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

package(default_visibility = [
    "//arolla/decision_forest/qexpr_operator:__pkg__",
])

licenses(["notice"])

cc_library(
    name = "batched_evaluation",
    srcs = ["batched_forest_evaluator.cc"],
    hdrs = ["batched_forest_evaluator.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/decision_forest",
        "//arolla/decision_forest/pointwise_evaluation",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "batched_forest_evaluator_test",
    srcs = ["batched_forest_evaluator_test.cc"],
    deps = [
        ":batched_evaluation",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/decision_forest/testing",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":batched_evaluation",
        "//arolla/decision_forest",
        "//arolla/decision_forest/testing",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_Prod_IntervalSplits_100/7/1000",
    binary = ":benchmarks",
)

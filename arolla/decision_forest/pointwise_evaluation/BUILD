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
    name = "pointwise_evaluation",
    srcs = [
        "bitmask_builder.cc",
        "bitmask_eval.cc",
        "forest_evaluator.cc",
        "oblivious.cc",
        "single_input_eval.cc",
    ],
    hdrs = [
        "bitmask_builder.h",
        "bitmask_eval.h",
        "bound_split_conditions.h",
        "forest_evaluator.h",
        "oblivious.h",
        "single_input_eval.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":pointwise",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "pointwise",
    hdrs = [
        "pointwise.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

cc_test(
    name = "forest_evaluator_test",
    srcs = ["forest_evaluator_test.cc"],
    deps = [
        ":pointwise_evaluation",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/decision_forest/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "oblivious_test",
    srcs = ["oblivious_test.cc"],
    deps = [
        ":pointwise_evaluation",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "bound_split_conditions_test",
    srcs = ["bound_split_conditions_test.cc"],
    deps = [
        ":pointwise_evaluation",
        "//arolla/decision_forest/split_conditions",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":pointwise",
        ":pointwise_evaluation",
        "//arolla/decision_forest",
        "//arolla/decision_forest/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_Main_IntervalSplits_Pointwise/3/5",
    binary = ":benchmarks",
)

cc_test(
    name = "pointwise_test",
    srcs = ["pointwise_test.cc"],
    deps = [
        ":pointwise",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

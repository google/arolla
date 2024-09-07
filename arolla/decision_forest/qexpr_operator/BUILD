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

cc_library(
    name = "qexpr_operator",
    srcs = [
        "compile_decision_forest_operator.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":lib",
        "//arolla/decision_forest/expr_operator",
        "//arolla/expr/eval",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
    ],
    alwayslink = 1,
)

cc_library(
    name = "lib",
    srcs = [
        "batched_operator.cc",
        "pointwise_operator.cc",
    ],
    hdrs = [
        "batched_operator.h",
        "pointwise_operator.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/decision_forest",
        "//arolla/decision_forest/batched_evaluation",
        "//arolla/decision_forest/pointwise_evaluation",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "pointwise_test",
    srcs = ["pointwise_test.cc"],
    deps = [
        ":lib",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "batched_test",
    srcs = ["batched_test.cc"],
    deps = [
        ":lib",
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

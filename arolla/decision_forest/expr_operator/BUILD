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
    name = "expr_operator",
    srcs = [
        "decision_forest_operator.cc",
        "forest_model.cc",
    ],
    hdrs = [
        "decision_forest_operator.h",
        "forest_model.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/decision_forest",
        "//arolla/expr",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "forest_model_test",
    srcs = ["forest_model_test.cc"],
    deps = [
        ":expr_operator",
        "//arolla/decision_forest",
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/serving:serving_lite",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "decision_forest_operator_test",
    srcs = ["decision_forest_operator_test.cc"],
    deps = [
        ":expr_operator",
        "//arolla/decision_forest",
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array/qtype",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

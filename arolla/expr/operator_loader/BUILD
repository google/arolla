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

# Library for defining and/or loading expr operators.
#

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "operator_loader",
    srcs = [
        "backend_operator.cc",
        "dispatch_operator.cc",
        "dummy_operator.cc",
        "generic_operator.cc",
        "generic_operator_overload_condition.cc",
        "helper.cc",
        "parameter_qtypes.cc",
        "qtype_constraint.cc",
        "qtype_inference.cc",
        "restricted_lambda_operator.cc",
    ],
    hdrs = [
        "backend_operator.h",
        "dispatch_operator.h",
        "dummy_operator.h",
        "generic_operator.h",
        "generic_operator_overload_condition.h",
        "helper.h",
        "parameter_qtypes.h",
        "qtype_constraint.h",
        "qtype_inference.h",
        "restricted_lambda_operator.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "parameter_qtypes_test",
    srcs = [
        "parameter_qtypes_test.cc",
    ],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "qtype_constraint_test",
    srcs = [
        "qtype_constraint_test.cc",
    ],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "qtype_inference_test",
    srcs = [
        "qtype_inference_test.cc",
    ],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "backend_operator_test",
    srcs = [
        "backend_operator_test.cc",
    ],
    deps = [
        ":operator_loader",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dispatch_operator_test",
    srcs = [
        "dispatch_operator_test.cc",
    ],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "restricted_lambda_operator_test",
    srcs = [
        "restricted_lambda_operator_test.cc",
    ],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dummy_operator_test",
    srcs = ["dummy_operator_test.cc"],
    deps = [
        ":operator_loader",
        "//arolla/array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "generic_operator_test",
    srcs = ["generic_operator_test.cc"],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "generic_operator_overload_condition_test",
    srcs = ["generic_operator_overload_condition_test.cc"],
    deps = [
        ":operator_loader",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

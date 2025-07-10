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

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

# PWLCurve is a small piecewise-linear curve evaluation library.
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "dont_lift",
    "float_types",
    "lift_to_optional",
    "operator_libraries",
    "operator_overload_list",
    "with_lifted_by",
)
load(
    "//arolla/qexpr/operators/array:array.bzl",
    "lift_to_array",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "lift_to_dense_array",
)
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

licenses(["notice"])

cc_library(
    name = "pwlcurve",
    srcs = ["curves.cc"],
    hdrs = ["curves.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_double_conversion//:double-conversion",
    ],
)

cc_test(
    name = "curves_test",
    srcs = ["curves_test.cc"],
    deps = [
        ":pwlcurve",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "curves_benchmark",
    testonly = 1,
    srcs = ["curves_benchmark.cc"],
    deps = [
        ":pwlcurve",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "curves_benchmark_smoke_test",
    binary = ":curves_benchmark",
)

cc_library(
    name = "lib",
    srcs = ["curve_operator.cc"],
    hdrs = ["curve_operator.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":pwlcurve",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

operator_libraries(
    name = "operator_create_curve",
    operator_name = "curves._create_curve",
    overloads =
        operator_overload_list(
            hdrs = ["curve_operator.h"],
            arg_lists = [
                ["::arolla::Bytes"],
                [
                    "int32_t",
                    "::arolla::DenseArray<float>",
                    "::arolla::DenseArray<float>",
                ],
            ],
            build_target_groups = ["on_dense_arrays"],
            op_class = "::arolla::pwlcurve::CreateCurveOp",
            deps = [":lib"],
        ),
)

operator_libraries(
    name = "operator_eval_curve",
    operator_name = "curves._eval_curve",
    overloads = with_lifted_by(
        [
            lift_to_optional,
            lift_to_dense_array,
            lift_to_array,
        ],
        operator_overload_list(
            hdrs = ["curve_operator.h"],
            arg_lists = [(
                dont_lift("::arolla::pwlcurve::CurvePtr"),
                f,
            ) for f in float_types],
            op_class = "::arolla::pwlcurve::EvalCurveOp",
            deps = [":lib"],
        ),
    ),
)

cc_test(
    name = "curve_operator_test",
    srcs = ["curve_operator_test.cc"],
    deps = [
        ":lib",
        ":operator_create_curve",
        ":operator_eval_curve",
        ":pwlcurve",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

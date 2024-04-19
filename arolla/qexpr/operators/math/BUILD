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

# Math operators for RLv2

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "binary_args",
    "bool_type",
    "float_types",
    "lift_to_optional",
    "make_optional_type",
    "n_ary_args",
    "numeric_types",
    "operator_libraries",
    "operator_overload",
    "operator_overload_list",
    "unary_args",
    "with_lifted_by",
)
load(
    "//arolla/qexpr/operators/array:array.bzl",
    "lift_to_array",
    "make_array_type",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "lift_to_dense_array",
    "make_dense_array_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    # go/keep-sorted start
    ":operator_abs",
    ":operator_add",
    ":operator_add4",
    ":operator_atan",
    ":operator_ceil",
    ":operator_cos",
    ":operator_divide",
    ":operator_exp",
    ":operator_expm1",
    ":operator_floor",
    ":operator_floordiv",
    ":operator_fmod",
    ":operator_is_finite",
    ":operator_is_inf",
    ":operator_is_nan",
    ":operator_log",
    ":operator_log10",
    ":operator_log1p",
    ":operator_log2",
    ":operator_log_sigmoid",
    ":operator_logit",
    ":operator_maximum",
    ":operator_minimum",
    ":operator_mod",
    ":operator_multiply",
    ":operator_neg",
    ":operator_pow",
    ":operator_round",
    ":operator_searchsorted_impl",
    ":operator_sigmoid",
    ":operator_sign",
    ":operator_sin",
    ":operator_sinh",
    ":operator_subtract",
    ":operator_symlog1p",
    # go/keep-sorted end
]

# Registers all operators defined in the package.
cc_library(
    name = "math",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_lib_list,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_metadata" for lib in operator_lib_list],
)

# Operators that may be useful to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_plain" for lib in operator_lib_list] +
           [lib + "_on_optionals" for lib in operator_lib_list],
)

# Operators that may be useful to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_on_dense_arrays" for lib in operator_lib_list],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_on_arrays" for lib in operator_lib_list],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "arithmetic.h",
        "batch_arithmetic.h",
        "binary_search.h",
        "math.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@eigen",
    ],
)

all_lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

# Arithmetic operators.
operator_libraries(
    name = "operator_add",
    operator_name = "math.add",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::AddOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_add4",
    operator_name = "math._add4",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = n_ary_args(4, numeric_types),
            op_class = "::arolla::AddOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_subtract",
    operator_name = "math.subtract",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::SubtractOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_multiply",
    operator_name = "math.multiply",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::MultiplyOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_floordiv",
    operator_name = "math.floordiv",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::FloorDivOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_mod",
    operator_name = "math.mod",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::ModOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_divide",
    operator_name = "math.divide",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(float_types),
            op_class = "::arolla::DivideOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_fmod",
    operator_name = "math.fmod",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(float_types),
            op_class = "::arolla::FmodOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_sign",
    operator_name = "math.sign",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(numeric_types),
            op_class = "::arolla::SignOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_neg",
    operator_name = "math.neg",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(numeric_types),
            op_class = "::arolla::NegOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_abs",
    operator_name = "math.abs",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(numeric_types),
            op_class = "::arolla::AbsOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_is_inf",
    operator_name = "math.is_inf",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(numeric_types),
            op_class = "::arolla::IsInfOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_is_finite",
    operator_name = "math.is_finite",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(numeric_types),
            op_class = "::arolla::IsFiniteOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_is_nan",
    operator_name = "math.is_nan",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(numeric_types),
            op_class = "::arolla::IsNanOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_pow",
    operator_name = "math._pow",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = binary_args(float_types),
            op_class = "::arolla::PowOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_ceil",
    operator_name = "math.ceil",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::CeilOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_floor",
    operator_name = "math.floor",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::FloorOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_round",
    operator_name = "math.round",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::RoundOp",
            deps = [":lib"],
        ),
    ),
)

# Math Operators

operator_libraries(
    name = "operator_log",
    operator_name = "math.log",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::LogOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_log2",
    operator_name = "math.log2",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::Log2Op",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_log10",
    operator_name = "math.log10",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::Log10Op",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_log1p",
    operator_name = "math.log1p",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::Log1pOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_symlog1p",
    operator_name = "math.symlog1p",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::Symlog1pOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_exp",
    operator_name = "math.exp",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::ExpOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_sigmoid",
    operator_name = "math.sigmoid",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = n_ary_args(3, float_types),
            op_class = "::arolla::SigmoidOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_logit",
    operator_name = "math.logit",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::LogitOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_log_sigmoid",
    operator_name = "math.log_sigmoid",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::LogSigmoidOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_expm1",
    operator_name = "math.expm1",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::Expm1Op",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_maximum",
    operator_name = "math.maximum",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::MaxOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_minimum",
    operator_name = "math.minimum",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["arithmetic.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::MinOp",
            deps = [":lib"],
        ),
    ),
)

# Trigonometric operators.
operator_libraries(
    name = "operator_sin",
    operator_name = "math.trig.sin",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::SinOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_cos",
    operator_name = "math.trig.cos",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::CosOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_sinh",
    operator_name = "math.trig.sinh",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::SinhOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_atan",
    operator_name = "math.trig.atan",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["math.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::AtanOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_searchsorted_impl",
    operator_name = "math._searchsorted_impl",
    overloads = [
        operator_overload(
            hdrs = ["binary_search.h"],
            args = [
                haystack_type,
                needle_type,
                make_optional_type(bool_type),
            ],
            build_target_groups = [build_target_group],
            op_class = "::arolla::SearchSortedOp",
            deps = [":lib"],
        )
        for v in numeric_types
        for haystack_type, needle_type, build_target_group in [
            (
                make_dense_array_type(v),
                v,
                "plain",
            ),
            (
                make_dense_array_type(v),
                make_optional_type(v),
                "on_optionals",
            ),
            (
                make_dense_array_type(v),
                make_dense_array_type(v),
                "on_dense_arrays",
            ),
            (
                make_dense_array_type(v),
                make_array_type(v),
                "on_arrays",
            ),
        ]
    ],
)

# Tests

cc_test(
    name = "arithmetic_test",
    srcs = ["arithmetic_test.cc"],
    deps = [
        ":lib",
        ":math",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "batch_arithmetic_test",
    srcs = ["batch_arithmetic_test.cc"],
    deps = [
        ":lib",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "binary_search_test",
    srcs = ["binary_search_test.cc"],
    deps = [
        ":lib",
        "//arolla/dense_array",
        "//arolla/memory",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "math_test",
    srcs = ["math_test.cc"],
    deps = [
        ":math",
        ":operator_exp",
        ":operator_expm1",
        ":operator_log",
        ":operator_log1p",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":lib",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

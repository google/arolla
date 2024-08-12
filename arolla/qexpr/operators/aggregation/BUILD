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

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "accumulator_overload",
    "bool_type",
    "float_types",
    "lift_by",
    "make_optional_type",
    "numeric_types",
    "operator_libraries",
    "scalar_types",
    "string_types",
    "unit_type",
)
load(
    "//arolla/qexpr/operators/array:array.bzl",
    "array_accumulator_lifters",
    "lift_accumulator_to_array_with_edge",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "dense_array_accumulator_lifters",
    "lift_accumulator_to_dense_array_with_edge",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    # go/keep-sorted start
    ":operator_agg_all",
    ":operator_agg_any",
    ":operator_agg_count",
    ":operator_agg_inverse_cdf",
    ":operator_agg_logical_all",
    ":operator_agg_logical_any",
    ":operator_agg_max",
    ":operator_agg_mean",
    ":operator_agg_median",
    ":operator_agg_min",
    ":operator_agg_prod",
    ":operator_agg_sum",
    ":operator_collapse",
    ":operator_cum_count",
    ":operator_cum_max",
    ":operator_cum_min",
    ":operator_cum_sum",
    ":operator_dense_rank",
    ":operator_inverse_mapping",
    ":operator_ordinal_rank",
    ":operator_string_agg_join",
    ":operator_take_over",
    ":operator_weighted_average",
    ":operator_weighted_cdf",
    # go/keep-sorted end
]

# Registers all operators defined in the package.
cc_library(
    name = "aggregation",
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
    hdrs = ["group_op_accumulators.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

accumulator_lifters = array_accumulator_lifters + dense_array_accumulator_lifters

operator_libraries(
    name = "operator_agg_count",
    operator_name = "array._count",
    overloads = lift_by(
        [
            # Versions for *_to_scalar edges are defined separately for performance reasons.
            lift_accumulator_to_array_with_edge,
            lift_accumulator_to_dense_array_with_edge,
        ],
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::SimpleCountAggregator",
                child_args = [unit_type],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_cum_count",
    operator_name = "array._cum_count",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::CountPartialAccumulator",
                child_args = [unit_type],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_agg_any",
    operator_name = "core._any",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::AnyAggregator",
                child_args = [unit_type],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_agg_all",
    operator_name = "core._all",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::AllAggregator",
                child_args = [unit_type],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_agg_sum",
    operator_name = "math._sum",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::SumAggregator<" + value_type + ">",
                child_args = [value_type],
                init_args = [make_optional_type(value_type)],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_agg_mean",
    operator_name = "math._mean",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::MeanAggregator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in float_types
        ],
    ),
)

operator_libraries(
    name = "operator_agg_prod",
    operator_name = "math._prod",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::ProdAggregator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_cum_sum",
    operator_name = "math.cum_sum",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::SumPartialAccumulator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_agg_inverse_cdf",
    operator_name = "math._inverse_cdf",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::InverseCdfAccumulator<" + value_type + ">",
                child_args = [value_type],
                # TODO: Switch to double once we have weak float support in serving.
                init_args = ["float"],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_weighted_average",
    operator_name = "math._weighted_average",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::WeightedAverageAccumulator<" + value_type + ">",
                child_args = [
                    value_type,
                    value_type,
                ],
                deps = [":lib"],
            )
            for value_type in float_types
        ],
    ),
)

operator_libraries(
    name = "operator_agg_min",
    operator_name = "math._min",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::MinAggregator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_cum_min",
    operator_name = "math.cum_min",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::MinPartialAccumulator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_agg_max",
    operator_name = "math._max",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::MaxAggregator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_cum_max",
    operator_name = "math.cum_max",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::MaxPartialAccumulator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_agg_logical_all",
    operator_name = "bool._logical_all",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::LogicalAllAggregator",
                child_args = ["bool"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_agg_logical_any",
    operator_name = "bool._logical_any",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::LogicalAnyAggregator",
                child_args = ["bool"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_collapse",
    operator_name = "array._collapse",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::CollapseAccumulator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in scalar_types
        ],
    ),
)

operator_libraries(
    name = "operator_inverse_mapping",
    operator_name = "array._inverse_mapping",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::InverseMappingAccumulator",
                child_args = ["int64_t"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_string_agg_join",
    operator_name = "strings._agg_join",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::StringJoinAggregator<" + value_type + ">",
                child_args = [value_type],
                init_args = [value_type],
                deps = [":lib"],
            )
            for value_type in string_types
        ],
    ),
)

operator_libraries(
    name = "operator_take_over",
    operator_name = "array._take_over",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::ArrayTakeOverAccumulator<" + value_type + ">",
                child_args = [
                    value_type,
                    "int64_t",
                ],
                deps = [":lib"],
            )
            for value_type in scalar_types
        ],
    ),
)

operator_libraries(
    name = "operator_weighted_cdf",
    operator_name = "math._weighted_cdf",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::WeightedCDFAccumulator<" + value_type + "," + value_type + ">",
                child_args = [
                    value_type,
                    value_type,
                ],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

operator_libraries(
    name = "operator_ordinal_rank",
    operator_name = "array._ordinal_rank",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::OrdinalRankAccumulator<%s, %s>" % (value_type, "int64_t"),
                child_args = [
                    value_type,
                    "int64_t",
                ],
                init_args = ["bool"],
                deps = [":lib"],
            )
            for value_type in scalar_types
        ],
    ),
)

operator_libraries(
    name = "operator_dense_rank",
    operator_name = "array._dense_rank",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::DenseRankAccumulator<" + value_type + ">",
                child_args = [value_type],
                init_args = ["bool"],
                deps = [":lib"],
            )
            for value_type in numeric_types + string_types + [bool_type]
        ],
    ),
)

operator_libraries(
    name = "operator_agg_median",
    operator_name = "math._median",
    overloads = lift_by(
        accumulator_lifters,
        [
            accumulator_overload(
                hdrs = ["group_op_accumulators.h"],
                acc_class = "::arolla::MedianAggregator<" + value_type + ">",
                child_args = [value_type],
                deps = [":lib"],
            )
            for value_type in numeric_types
        ],
    ),
)

cc_test(
    name = "aggregation_test",
    srcs = ["aggregation_test.cc"],
    deps = [
        ":lib",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

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

# DenseArray operators

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "binary_args",
    "float_types",
    "make_optional_type",
    "make_optional_types",
    "operator_family",
    "operator_libraries",
    "operator_overload",
    "operator_overload_list",
    "scalar_types",
    "unit_type",
    "with_lifted_by",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "dense_array_edge_type",
    "dense_array_scalar_edge_type",
    "lift_to_dense_array",
    "make_dense_array_type",
)
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

# All value types of DenseArray<T>.
dense_array_value_types = scalar_types

operator_lib_list = [
    # go/keep-sorted start
    ":operator_agg_count_to_scalar",
    ":operator_array_from_indices_and_values",
    ":operator_array_shape_of",
    ":operator_array_shape_size",
    ":operator_concat",
    ":operator_const_with_shape",
    ":operator_edge_child_shape",
    ":operator_edge_compose_dense_array",
    ":operator_edge_from_mapping",
    ":operator_edge_from_shape",
    ":operator_edge_from_sizes",
    ":operator_edge_from_split_points",
    ":operator_edge_pair_left",
    ":operator_edge_pair_right",
    ":operator_edge_parent_shape",
    ":operator_edge_resize_groups_child_side",
    ":operator_edge_resize_groups_parent_side",
    ":operator_edge_sizes",
    ":operator_expand",
    ":operator_group_by",
    ":operator_has",
    ":operator_interleave",
    ":operator_iota",
    ":operator_mapping",
    ":operator_presence_and",
    ":operator_presence_not",
    ":operator_presence_or",
    ":operator_present_indices",
    ":operator_present_values",
    ":operator_randint_with_shape",
    ":operator_resize_array_shape",
    ":operator_select",
    ":operator_slice",
    ":operator_take_over_over",
    ":operator_unique",
    # go/keep-sorted end
]

# Registers all operators defined in the package.
cc_library(
    name = "dense_array",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_lib_list + [
        ":operator_make_dense_array",
        # Listing separately so clients can depend on "on_scalars" version without duplication.
        ":operator_at_on_scalars",
        ":operator_at_on_dense_arrays",
    ],
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_metadata" for lib in operator_lib_list] + [
        ":operator_at_metadata",
        ":operator_make_dense_array_metadata",
    ],
)

cc_library(
    name = "lib",
    srcs = [
        "array_ops.cc",
        "factory_ops.cc",
        "interleave.cc",
    ],
    hdrs = [
        "array_ops.h",
        "edge_ops.h",
        "factory_ops.h",
        "group_lifter.h",
        "interleave.h",
        "lifter.h",
        "logic_ops.h",
        "random.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/array_like",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

operator_family(
    name = "operator_interleave",
    hdrs = ["interleave.h"],
    op_family_class = "::arolla::InterleaveToDenseArrayOperatorFamily",
    op_family_name = "array._interleave_to_dense_array",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_presence_and",
    operator_name = "core.presence_and",
    overloads = operator_overload_list(
        hdrs = ["logic_ops.h"],
        arg_lists = [
            (
                make_dense_array_type(t),
                make_dense_array_type(unit_type),
            )
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayPresenceAndOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_presence_not",
    operator_name = "core.presence_not._builtin",
    overloads = operator_overload_list(
        hdrs = ["logic_ops.h"],
        arg_lists = [
            (make_dense_array_type(t),)
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayPresenceNotOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_presence_or",
    operator_name = "core.presence_or",
    overloads = operator_overload_list(
        hdrs = ["logic_ops.h"],
        arg_lists = [
            (
                make_dense_array_type(t),
                make_dense_array_type(t),
            )
            for t in dense_array_value_types
        ] + [
            (
                make_dense_array_type(t),
                make_optional_type(t),
            )
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayPresenceOrOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_has",
    operator_name = "core.has._array",
    overloads = operator_overload_list(
        hdrs = ["logic_ops.h"],
        arg_lists = [
            (make_dense_array_type(t),)
            for t in dense_array_value_types
            if t != unit_type
        ],
        op_class = "::arolla::DenseArrayHasOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_agg_count_to_scalar",
    operator_name = "array._count",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(
            "::arolla::DenseArray<::arolla::Unit>",
            "::arolla::DenseArrayGroupScalarEdge",
        )],
        op_class = "::arolla::DenseArrayCountOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_take_over_over",
    operator_name = "array._take_over_over",
    overloads = operator_overload_list(
        hdrs = [
            "arolla/dense_array/ops/multi_edge_util.h",
            "arolla/dense_array/qtype/types.h",
            "arolla/qexpr/operators/aggregation/group_op_accumulators.h",
        ],
        arg_lists = [
            (
                make_dense_array_type(t),
                "::arolla::DenseArray<int64_t>",
                "::arolla::DenseArrayEdge",
                "::arolla::DenseArrayEdge",
            )
            for t in dense_array_value_types
        ],
        op_class = "::arolla::ArrayTakeOverOverOp<::arolla::DenseArrayMultiEdgeUtil>",
        deps = [
            ":lib",
            "//arolla/dense_array/ops",
            "//arolla/dense_array/qtype",
            "//arolla/qexpr/operators/aggregation:lib",
        ],
    ),
)

operator_libraries(
    name = "operator_edge_from_split_points",
    operator_name = "edge.from_split_points",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::DenseArray<int64_t>",)],
        op_class = "::arolla::DenseArrayEdgeFromSplitPointsOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_resize_groups_child_side",
    operator_name = "edge._resize_groups_child_side",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::DenseArrayEdge", "int64_t", "::arolla::DenseArray<int64_t>"),
            ("::arolla::DenseArrayGroupScalarEdge", "int64_t", "::arolla::DenseArray<int64_t>"),
            ("::arolla::DenseArrayEdge", "::arolla::DenseArray<int64_t>", "::arolla::DenseArray<int64_t>"),
        ],
        op_class = "::arolla::DenseArrayEdgeResizeGroupsChildSide",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_resize_groups_parent_side",
    operator_name = "edge.resize_groups_parent_side",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::DenseArrayEdge", "int64_t"),
            ("::arolla::DenseArrayGroupScalarEdge", "int64_t"),
            ("::arolla::DenseArrayEdge", "::arolla::DenseArray<int64_t>"),
        ],
        op_class = "::arolla::DenseArrayEdgeResizeGroupsParentSide",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_shape",
    operator_name = "edge.from_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::DenseArrayShape",)],
        op_class = "::arolla::DenseArrayEdgeFromShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_sizes",
    operator_name = "edge.from_sizes",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::DenseArray<int64_t>",)],
        op_class = "::arolla::DenseArrayEdgeFromSizesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_mapping",
    operator_name = "edge.from_mapping",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(
            "::arolla::DenseArray<int64_t>",
            "int64_t",
        )],
        op_class = "::arolla::DenseArrayEdgeFromMappingOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_pair_left",
    operator_name = "edge.pair_left",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::DenseArray<int64_t>",)],
        op_class = "::arolla::DenseArrayEdgePairLeftOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_pair_right",
    operator_name = "edge.pair_right",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::DenseArray<int64_t>",)],
        op_class = "::arolla::DenseArrayEdgePairRightOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_mapping",
    operator_name = "edge.mapping",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            (dense_array_edge_type,),
            (dense_array_scalar_edge_type,),
        ],
        op_class = "::arolla::DenseArrayEdgeMappingOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_child_shape",
    operator_name = "edge.child_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::DenseArrayEdge",),
            ("::arolla::DenseArrayGroupScalarEdge",),
        ],
        op_class = "::arolla::DenseArrayEdgeChildShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_parent_shape",
    operator_name = "edge.parent_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::DenseArrayEdge",),
            ("::arolla::DenseArrayGroupScalarEdge",),
        ],
        op_class = "::arolla::DenseArrayEdgeParentShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_expand",
    operator_name = "array._expand",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(
                        make_dense_array_type(v),
                        "::arolla::DenseArrayEdge",
                    ) for v in dense_array_value_types] +
                    [(
                        make_optional_type(v),
                        "::arolla::DenseArrayGroupScalarEdge",
                    ) for v in dense_array_value_types],
        op_class = "::arolla::DenseArrayExpandOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_array_shape_of",
    operator_name = "core._array_shape_of",
    overloads = operator_overload_list(
        hdrs = ["factory_ops.h"],
        arg_lists = [
            [make_dense_array_type(unit_type)],
        ],
        op_class = "::arolla::DenseArrayShapeOfOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_array_shape_size",
    operator_name = "array.array_shape_size",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = ["::arolla::DenseArrayShape"],
        op_class = "::arolla::DenseArrayShapeSizeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_resize_array_shape",
    operator_name = "array.resize_array_shape",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = ("::arolla::DenseArrayShape", "int64_t"),
        op_class = "::arolla::DenseArrayResizeShapeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_group_by",
    operator_name = "edge._group_by",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            (
                make_dense_array_type(value),
                edge,
            )
            for value in dense_array_value_types
            for edge in [
                dense_array_edge_type,
                dense_array_scalar_edge_type,
            ]
        ],
        op_class = "::arolla::DenseArrayGroupByOp",
        deps = [
            ":lib",
        ],
    ),
)

operator_libraries(
    name = "operator_edge_sizes",
    operator_name = "edge.sizes",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            (dense_array_edge_type,),
            (dense_array_scalar_edge_type,),
        ],
        op_class = "::arolla::DenseArrayEdgeSizesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_const_with_shape",
    operator_name = "core.const_with_shape._array_shape",
    overloads = operator_overload_list(
        hdrs = ["factory_ops.h"],
        arg_lists = [
            ("::arolla::DenseArrayShape", v)
            for v in (dense_array_value_types + make_optional_types(dense_array_value_types))
        ],
        op_class = "::arolla::DenseArrayConstWithShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_iota",
    operator_name = "array._iota",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = ["::arolla::DenseArrayShape"],
        op_class = "::arolla::DenseArrayIotaOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_slice",
    operator_name = "array.slice",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (
                make_dense_array_type(v),
                "int64_t",
                "int64_t",
            )
            for v in dense_array_value_types
        ],
        op_class = "::arolla::DenseArraySliceOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_concat",
    operator_name = "array.concat",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (
                make_dense_array_type(v),
                make_dense_array_type(v),
            )
            for v in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayConcatOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_at",
    operator_name = "array.at",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(
            make_dense_array_type(v),
            i,
        ) for v in dense_array_value_types for i in [
            "int64_t",
            "::arolla::OptionalValue<int64_t>",
        ]],
        build_target_groups = ["on_scalars"],
        op_class = "::arolla::DenseArrayAtOp",
        deps = [":lib"],
    ) + operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(
            make_dense_array_type(v),
            make_dense_array_type("int64_t"),
        ) for v in scalar_types],
        build_target_groups = ["on_dense_arrays"],
        op_class = "::arolla::DenseArrayAtOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_present_indices",
    operator_name = "array._present_indices",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(make_dense_array_type(unit_type),)],
        op_class = "::arolla::DenseArrayPresentIndicesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_present_values",
    operator_name = "array.present_values",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (make_dense_array_type(t),)
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayPresentValuesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_array_from_indices_and_values",
    operator_name = "array.from_indices_and_values",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (
                make_dense_array_type("int64_t"),
                make_dense_array_type(t),
                "int64_t",
            )
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayFromIndicesAndValues",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_randint_with_shape",
    operator_name = "array.randint_with_shape",
    overloads = operator_overload_list(
        hdrs = ["random.h"],
        arg_lists = [
            ("::arolla::DenseArrayShape", "int64_t", "int64_t", "int64_t"),
        ],
        op_class = "::arolla::RandIntWithDenseArrayShape",
        deps = [":lib"],
    ),
)

operator_family(
    name = "operator_make_dense_array",
    hdrs = ["factory_ops.h"],
    op_family_class = "::arolla::MakeDenseArrayOperatorFamily",
    op_family_name = "array.make_dense_array",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_unique",
    operator_name = "array.unique",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (make_dense_array_type(t),)
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArrayUniqueOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_select",
    operator_name = "array.select",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (
                make_dense_array_type(t),
                make_dense_array_type(unit_type),
            )
            for t in dense_array_value_types
        ],
        op_class = "::arolla::DenseArraySelectOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "test_operator_add",
    testonly = 1,
    operator_name = "test.add",
    overloads = with_lifted_by(
        [lift_to_dense_array],
        operator_overload_list(
            hdrs = ["arolla/codegen/qexpr/testing/test_operators.h"],
            arg_lists = binary_args(float_types),
            op_class = "::arolla::testing::AddOp",
            deps = ["//arolla/codegen/qexpr/testing:test_operators_lib"],
        ),
    ),
)

operator_family(
    name = "operator_edge_compose_dense_array",
    hdrs = ["edge_ops.h"],
    op_family_class = "::arolla::DenseArrayEdgeComposeOperatorFamily",
    op_family_name = "edge.compose._dense_array",
    deps = [":lib"],
)

cc_test(
    name = "agg_ops_test",
    srcs = ["agg_ops_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dense_array_test",
    srcs = ["dense_array_test.cc"],
    deps = [
        ":lib",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/testing:lib",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_ops_test",
    srcs = ["array_ops_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation",
        "//arolla/qexpr/testing",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "edge_ops_test",
    srcs = ["edge_ops_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "factory_ops_test",
    srcs = ["factory_ops_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "logic_ops_test",
    srcs = ["logic_ops_test.cc"],
    deps = [
        ":dense_array",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

bzl_library(
    name = "lifter_bzl",
    srcs = ["lifter.bzl"],
    deps = ["//arolla/codegen/qexpr:register_operator_bzl"],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":lib",
        ":test_operator_add",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_DenseArrayAddFull/1/2",
    binary = ":benchmarks",
)

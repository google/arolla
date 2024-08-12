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

# Array operators

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "make_optional_type",
    "make_optional_types",
    "operator_family",
    "operator_libraries",
    "operator_overload",
    "operator_overload_list",
    "scalar_types",
    "unit_type",
)
load(
    "//arolla/qexpr/operators/array:array.bzl",
    "array_deps",
    "array_edge_type",
    "array_hdrs",
    "array_scalar_edge_type",
    "lift_to_array",
    "make_array_op",
    "make_array_type",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "make_dense_array_type",
)
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

# All value types of Array<T>.
array_value_types = scalar_types

operator_lib_list = [
    # go/keep-sorted start
    ":operator_agg_count_to_scalar",
    ":operator_array_from_indices_and_values",
    ":operator_array_shape_of",
    ":operator_array_shape_size",
    ":operator_as_array",
    ":operator_as_dense_array",
    ":operator_as_dense_array_edge",
    ":operator_at",
    ":operator_concat",
    ":operator_const_with_shape",
    ":operator_edge_child_shape",
    ":operator_edge_compose_array",
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
    name = "array",
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

cc_library(
    name = "lib",
    srcs = ["array_ops.cc"],
    hdrs = [
        "array_ops.h",
        "edge_ops.h",
        "factory_ops.h",
        "lifter.h",
        "logic_ops.h",
        "random.h",
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
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/array_like",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

operator_libraries(
    name = "operator_presence_and",
    operator_name = "core.presence_and",
    overloads = [
        operator_overload(
            hdrs = array_hdrs + [
                "arolla/qexpr/operators/core/logic_operators.h",
                "arolla/qexpr/operators/dense_array/logic_ops.h",
            ],
            args = (
                make_array_type(t),
                make_array_type(unit_type),
            ),
            build_target_groups = ["on_arrays"],
            op_class = make_array_op(
                "::arolla::DenseArrayPresenceAndOp",
                "::arolla::PresenceAndOp",
                liftable_args = (t, unit_type),
            ),
            deps = array_deps + [
                "//arolla/qexpr/operators/core:lib",
            ],
        )
        for t in array_value_types
    ],
)

operator_libraries(
    name = "operator_presence_not",
    operator_name = "core.presence_not._builtin",
    overloads = [
        lift_to_array(
            operator_overload(
                hdrs = [
                    "arolla/qexpr/operators/core/logic_operators.h",
                ],
                args = [make_optional_type(t)],
                op_class = "::arolla::PresenceNotOp",
                deps = [
                    "//arolla/qexpr/operators/core:lib",
                ],
            ),
        )
        for t in array_value_types
    ],
)

operator_libraries(
    name = "operator_presence_or",
    operator_name = "core.presence_or",
    overloads = operator_overload_list(
        hdrs = ["logic_ops.h"],
        arg_lists = [(
            make_array_type(t),
            make_array_type(t),
        ) for t in array_value_types],
        op_class = "::arolla::ArrayPresenceOrOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_has",
    operator_name = "core.has._array",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (make_array_type(t),)
            for t in array_value_types
            if t != unit_type
        ],
        op_class = "::arolla::ArrayHasOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_agg_count_to_scalar",
    operator_name = "array._count",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(
            "::arolla::Array<::arolla::Unit>",
            "::arolla::ArrayGroupScalarEdge",
        )],
        op_class = "::arolla::ArrayCountOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_take_over_over",
    operator_name = "array._take_over_over",
    overloads = operator_overload_list(
        hdrs = [
            "arolla/array/multi_edge_util.h",
            "arolla/array/qtype/types.h",
            "arolla/qexpr/operators/aggregation/group_op_accumulators.h",
        ],
        arg_lists = [
            (
                make_array_type(t),
                "::arolla::Array<int64_t>",
                "::arolla::ArrayEdge",
                "::arolla::ArrayEdge",
            )
            for t in array_value_types
        ],
        op_class = "::arolla::ArrayTakeOverOverOp<::arolla::ArrayMultiEdgeUtil>",
        deps = [
            ":lib",
            "//arolla/array",
            "//arolla/array/qtype",
            "//arolla/qexpr/operators/aggregation:lib",
        ],
    ),
)

operator_libraries(
    name = "operator_edge_from_split_points",
    operator_name = "edge.from_split_points",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::Array<int64_t>",)],
        op_class = "::arolla::ArrayEdgeFromSplitPointsOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_shape",
    operator_name = "edge.from_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::ArrayShape",)],
        op_class = "::arolla::ArrayEdgeFromShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_sizes",
    operator_name = "edge.from_sizes",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::Array<int64_t>",)],
        op_class = "::arolla::ArrayEdgeFromSizesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_mapping",
    operator_name = "edge.from_mapping",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::Array<int64_t>", "int64_t")],
        op_class = "::arolla::ArrayEdgeFromMappingOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_pair_left",
    operator_name = "edge.pair_left",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::Array<int64_t>",)],
        op_class = "::arolla::ArrayEdgePairLeftOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_pair_right",
    operator_name = "edge.pair_right",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [("::arolla::Array<int64_t>",)],
        op_class = "::arolla::ArrayEdgePairRightOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_mapping",
    operator_name = "edge.mapping",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            (array_edge_type,),
            (array_scalar_edge_type,),
        ],
        op_class = "::arolla::ArrayEdgeMappingOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_child_shape",
    operator_name = "edge.child_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::ArrayEdge",),
            ("::arolla::ArrayGroupScalarEdge",),
        ],
        op_class = "::arolla::ArrayEdgeChildShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_parent_shape",
    operator_name = "edge.parent_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::ArrayEdge",),
            ("::arolla::ArrayGroupScalarEdge",),
        ],
        op_class = "::arolla::ArrayEdgeParentShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_expand",
    operator_name = "array._expand",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(
                        make_array_type(v),
                        "::arolla::ArrayEdge",
                    ) for v in array_value_types] +
                    [(
                        make_optional_type(v),
                        "::arolla::ArrayGroupScalarEdge",
                    ) for v in array_value_types],
        op_class = "::arolla::ArrayExpandOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_array_shape_of",
    operator_name = "core._array_shape_of",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = (
            make_array_type(unit_type),
        ),
        op_class = "::arolla::ArrayShapeOfOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_as_dense_array",
    operator_name = "array._as_dense_array",
    overloads = operator_overload_list(
        hdrs = ["factory_ops.h"],
        arg_lists = [
            [make_array_type(v)]
            for v in array_value_types
        ],
        op_class = "::arolla::ArrayAsDenseArrayOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_as_dense_array_edge",
    operator_name = "edge._as_dense_array_edge",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ["::arolla::ArrayEdge"],
            ["::arolla::ArrayGroupScalarEdge"],
        ],
        op_class = "::arolla::ArrayEdgeAsDenseArrayOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_as_array",
    operator_name = "array._as_array",
    overloads = operator_overload_list(
        hdrs = ["factory_ops.h"],
        arg_lists = [
            [make_dense_array_type(v)]
            for v in scalar_types
        ],
        op_class = "::arolla::ArrayFromDenseArrayOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_array_shape_size",
    operator_name = "array.array_shape_size",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = ["::arolla::ArrayShape"],
        op_class = "::arolla::ArrayShapeSizeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_resize_array_shape",
    operator_name = "array.resize_array_shape",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = ("::arolla::ArrayShape", "int64_t"),
        op_class = "::arolla::ArrayResizeShapeOp",
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
                make_array_type(value),
                edge,
            )
            for value in array_value_types
            for edge in [
                array_edge_type,
                array_scalar_edge_type,
            ]
        ],
        op_class = "::arolla::ArrayGroupByOp",
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
            (array_edge_type,),
            (array_scalar_edge_type,),
        ],
        op_class = "::arolla::ArrayEdgeSizesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_const_with_shape",
    operator_name = "core.const_with_shape._array_shape",
    overloads = operator_overload_list(
        hdrs = ["factory_ops.h"],
        arg_lists = [
            ("::arolla::ArrayShape", v)
            for v in (array_value_types + make_optional_types(array_value_types))
        ],
        op_class = "::arolla::ArrayConstWithShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_iota",
    operator_name = "array._iota",
    overloads = [operator_overload(
        hdrs = ["factory_ops.h"],
        args = ["::arolla::ArrayShape"],
        op_class = "::arolla::ArrayIotaOp",
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
                make_array_type(v),
                "int64_t",
                "int64_t",
            )
            for v in array_value_types
        ],
        op_class = "::arolla::ArraySliceOp",
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
                make_array_type(v),
                make_array_type(v),
            )
            for v in array_value_types
        ],
        op_class = "::arolla::ArrayConcatOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_at",
    operator_name = "array.at",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(
            make_array_type(v),
            i,
        ) for v in array_value_types for i in [
            "int64_t",
            "::arolla::OptionalValue<int64_t>",
        ]],
        op_class = "::arolla::ArrayAtOp",
        deps = [":lib"],
    ) + operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(
            make_array_type(v),
            make_array_type("int64_t"),
        ) for v in scalar_types],
        op_class = "::arolla::ArrayAtOp",
        deps = [":lib"],
    ) + operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(
            make_dense_array_type(v),
            make_array_type("int64_t"),
        ) for v in scalar_types],
        op_class = "::arolla::ArrayAtOp",
        deps = [":lib"],
    ) + operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(
            make_array_type(v),
            make_dense_array_type("int64_t"),
        ) for v in scalar_types],
        op_class = "::arolla::ArrayAtOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_present_indices",
    operator_name = "array._present_indices",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [(make_array_type(unit_type),)],
        op_class = "::arolla::ArrayPresentIndicesOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_present_values",
    operator_name = "array.present_values",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (make_array_type(t),)
            for t in array_value_types
        ],
        op_class = "::arolla::ArrayPresentValuesOp",
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
                make_array_type("int64_t"),
                make_array_type(t),
                "int64_t",
            )
            for t in array_value_types
        ],
        op_class = "::arolla::ArrayFromIndicesAndValues",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_randint_with_shape",
    operator_name = "array.randint_with_shape",
    overloads = operator_overload_list(
        hdrs = ["random.h"],
        arg_lists = [
            ("::arolla::ArrayShape", "int64_t", "int64_t", "int64_t"),
        ],
        op_class = "::arolla::RandIntWithArrayShape",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_resize_groups_parent_side",
    operator_name = "edge.resize_groups_parent_side",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::ArrayEdge", "int64_t"),
            ("::arolla::ArrayGroupScalarEdge", "int64_t"),
            ("::arolla::ArrayEdge", "::arolla::Array<int64_t>"),
        ],
        op_class = "::arolla::ArrayEdgeResizeGroupsParentSide",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_resize_groups_child_side",
    operator_name = "edge._resize_groups_child_side",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            ("::arolla::ArrayEdge", "::arolla::Array<int64_t>", "::arolla::Array<int64_t>"),
            ("::arolla::ArrayEdge", "int64_t", "::arolla::Array<int64_t>"),
            ("::arolla::ArrayGroupScalarEdge", "int64_t", "::arolla::Array<int64_t>"),
        ],
        op_class = "::arolla::ArrayEdgeResizeGroupsChildSide",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_unique",
    operator_name = "array.unique",
    overloads = operator_overload_list(
        hdrs = ["array_ops.h"],
        arg_lists = [
            (make_array_type(t),)
            for t in array_value_types
        ],
        op_class = "::arolla::ArrayUniqueOp",
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
                make_array_type(t),
                make_array_type(unit_type),
            )
            for t in array_value_types
        ],
        op_class = "::arolla::ArraySelectOp",
        deps = [":lib"],
    ),
)

operator_family(
    name = "operator_edge_compose_array",
    hdrs = ["edge_ops.h"],
    op_family_class = "::arolla::ArrayEdgeComposeOperatorFamily",
    op_family_name = "edge.compose._array",
    deps = [":lib"],
)

cc_test(
    name = "agg_ops_test",
    srcs = ["agg_ops_test.cc"],
    deps = [
        ":array",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_test",
    srcs = ["array_test.cc"],
    deps = [
        ":lib",
        ":operator_presence_and",
        "//arolla/array",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/testing:lib",
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
        ":array",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "edge_ops_test",
    srcs = ["edge_ops_test.cc"],
    deps = [
        ":array",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "factory_ops_test",
    srcs = ["factory_ops_test.cc"],
    deps = [
        ":array",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "logic_ops_test",
    srcs = ["logic_ops_test.cc"],
    deps = [
        ":array",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "benchmarks",
    testonly = 1,
    srcs = ["benchmarks.cc"],
    deps = [
        ":lib",
        "//arolla/array",
        "//arolla/dense_array",
        "//arolla/dense_array/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    benchmarks = "BM_PresenceOr_Sparse_Sparse/320",
    binary = ":benchmarks",
)

bzl_library(
    name = "array_bzl",
    srcs = ["array.bzl"],
    deps = ["//arolla/codegen/qexpr:register_operator_bzl"],
)

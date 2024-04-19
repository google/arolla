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
    "binary_args",
    "bool_type",
    "bytes_type",
    "lift_to_optional",
    "make_optional_type",
    "make_optional_types",
    "numeric_types",
    "operator_family",
    "operator_libraries",
    "operator_overload_list",
    "scalar_types",
    "text_type",
    "uint64_type",
    "unary_args",
    "unit_type",
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

operator_libs_lifted_everywhere = [
    ":operator_mask_equal",
    ":operator_mask_less",
    ":operator_mask_less_equal",
    ":operator_mask_not_equal",
    ":operator_to_float32",
    ":operator_to_float64",
    ":operator_to_int32",
    ":operator_to_int64",
    ":operator_to_uint64",
    ":operator_to_bool",
    ":operator_where",
]

operator_libs_plain = [
    ":operator_get_optional_value",
    ":operator_has",
    ":operator_to_optional",
    ":operator_presence_and",
    ":operator_presence_and_or",
    ":operator_presence_not",
    ":operator_presence_or",
    ":operator_edge_child_shape",
    ":operator_edge_from_shape",
    ":operator_edge_parent_shape",
    ":operator_edge_sizes",
]

all_operator_libs = (
    operator_libs_lifted_everywhere +
    operator_libs_plain + [
        ":operator_presence_or_varargs",
        ":operator_fake_short_circuit_where_for_type_casting",
        ":operator_with_assertion",
    ]
)

optional_unit_type = make_optional_type(unit_type)

# Registers all operators defined in the package.
cc_library(
    name = "core",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = all_operator_libs,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_metadata" for lib in all_operator_libs],
)

# Operators that may be useful to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        ":operator_fake_short_circuit_where_for_type_casting",
        ":operator_with_assertion",
    ] + [
        lib + "_plain"
        for lib in operator_libs_lifted_everywhere +
                   operator_libs_plain
    ] + [
        lib + "_on_optionals"
        for lib in operator_libs_lifted_everywhere
    ],
)

# Operators that may be useful to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_on_dense_arrays" for lib in operator_libs_lifted_everywhere],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_on_arrays" for lib in operator_libs_lifted_everywhere],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    srcs = [
        "logic_operators.cc",
        "utility_operators.cc",
    ],
    hdrs = [
        "cast_operator.h",
        "edge_ops.h",
        "logic_operators.h",
        "utility_operators.h",
        "with_assertion_operator.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

# Internal presence_or implementation accepting scalar varargs. Arguments must
# have common value type; all but final argument must be wrapped with
# OptionalValue. If value type is `Unit`, then final argument must also be
# wrapped with OptionalValue. Result type matches final argument type.
operator_family(
    name = "operator_presence_or_varargs",
    hdrs = ["logic_operators.h"],
    op_family_class = "::arolla::PresenceOrVarargsOperatorFamily",
    op_family_name = "core._presence_or",
    deps = [":lib"],
)

operator_family(
    name = "operator_fake_short_circuit_where_for_type_casting",
    hdrs = ["logic_operators.h"],
    op_family_class = "::arolla::FakeShortCircuitWhereOperatorFamily",
    op_family_name = "core._short_circuit_where",
    deps = [":lib"],
)

operator_family(
    name = "operator_with_assertion",
    hdrs = ["with_assertion_operator.h"],
    op_family_class = "::arolla::WithAssertionOperatorFamily",
    op_family_name = "core.with_assertion",
    deps = [":lib"],
)

# Type casting operators.

operator_libraries(
    name = "operator_to_optional",
    operator_name = "core.to_optional._scalar",
    overloads = operator_overload_list(
        hdrs = ["cast_operator.h"],
        arg_lists = unary_args(scalar_types),
        op_class = "::arolla::ToOptionalOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_get_optional_value",
    operator_name = "core._get_optional_value",
    overloads = operator_overload_list(
        hdrs = ["cast_operator.h"],
        arg_lists = unary_args(make_optional_types(scalar_types)),
        op_class = "::arolla::GetOptionalValueOp",
        deps = [":lib"],
    ),
)

numeric_types_for_casting = numeric_types + [
    bool_type,
    uint64_type,
]

all_lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

operator_libraries(
    name = "operator_to_bool",
    operator_name = "core._to_bool",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["cast_operator.h"],
            arg_lists = [[t] for t in numeric_types_for_casting if t != bool_type],
            op_class = "::arolla::ToBoolOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_to_int32",
    operator_name = "core._to_int32",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["cast_operator.h"],
            arg_lists = [[t] for t in numeric_types_for_casting if t != "int32_t"],
            op_class = "::arolla::CastOp<int32_t>",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_to_int64",
    operator_name = "core._to_int64",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["cast_operator.h"],
            arg_lists = [[t] for t in numeric_types_for_casting if t != "int64_t"],
            op_class = "::arolla::CastOp<int64_t>",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_to_uint64",
    operator_name = "core._to_uint64",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["cast_operator.h"],
            arg_lists = [[t] for t in numeric_types_for_casting if t != uint64_type],
            op_class = "::arolla::CastOp<uint64_t>",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_to_float32",
    operator_name = "core._to_float32",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["cast_operator.h"],
            arg_lists = [[t] for t in numeric_types_for_casting if t != "float"],
            op_class = "::arolla::CastOp<float>",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_to_float64",
    operator_name = "core.to_float64",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["cast_operator.h"],
            arg_lists = unary_args(numeric_types_for_casting),
            op_class = "::arolla::CastOp<double>",
            deps = [":lib"],
        ),
    ),
)

# Logic operators

operator_libraries(
    name = "operator_presence_or",
    operator_name = "core.presence_or",
    overloads = operator_overload_list(
                    hdrs = ["logic_operators.h"],
                    arg_as_function_ids = [1],
                    arg_lists = binary_args(make_optional_types([
                        scalar_type
                        for scalar_type in scalar_types
                        # (optional_unit, optional_unit) belongs to the bootstrap set
                        if scalar_type != unit_type
                    ])),
                    op_class = "::arolla::PresenceOrOp",
                    deps = [":lib"],
                ) +
                operator_overload_list(
                    # Versions with non-optional arguments.
                    hdrs = ["logic_operators.h"],
                    arg_as_function_ids = [1],
                    # TODO: If the first argument is non-optional drop the whole
                    # operator during expr compilation.
                    arg_lists = zip(
                        make_optional_types(scalar_types),
                        scalar_types,
                    ) + zip(
                        scalar_types,
                        make_optional_types(scalar_types),
                    ) + binary_args(scalar_types),
                    op_class = "::arolla::PresenceOrOp",
                    deps = [":lib"],
                ),
)

operator_libraries(
    name = "operator_presence_and",
    operator_name = "core.presence_and",
    overloads = operator_overload_list(
        hdrs = ["logic_operators.h"],
        arg_as_function_ids = [0],
        arg_lists = [
            # TODO: Eliminate no-op version of the operator.
            (t, unit_type)
            for t in scalar_types
        ] + [
            (t, optional_unit_type)
            for t in (scalar_types + make_optional_types(scalar_types))
            # (optional_unit, optional_unit) belongs to the bootstrap set
            if t != optional_unit_type
        ],
        op_class = "::arolla::PresenceAndOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_where",
    operator_name = "core.where",
    overloads = (
        with_lifted_by(
            [
                lift_to_dense_array,
                lift_to_array,
            ],
            operator_overload_list(
                # We lift the version that accepts optionals instead of scalars because we don't
                # want the standard lifter's behavior that returns missing result in case of any
                # missing argument.
                hdrs = ["logic_operators.h"],
                arg_as_function_ids = [
                    1,
                    2,
                ],
                arg_lists = [
                    (
                        optional_unit_type,
                        make_optional_type(t),
                        make_optional_type(t),
                    )
                    for t in scalar_types
                ],
                build_target_groups = ("on_optionals",),
                op_class = "::arolla::WhereOp",
                deps = [":lib"],
            ),
        ) + operator_overload_list(
            hdrs = ["logic_operators.h"],
            arg_as_function_ids = [
                1,
                2,
            ],
            arg_lists = [
                (
                    optional_unit_type,
                    t,
                    t,
                )
                for t in scalar_types
            ],
            build_target_groups = ("plain",),
            op_class = "::arolla::WhereOp",
            deps = [":lib"],
        ) +
        operator_overload_list(
            # We explicitly instantiate (scalar, array, array) versions for performance. Also
            # core._short_circuit_where operator can fall back to them in case when there is
            # nothing to short circuit.
            hdrs = ["logic_operators.h"],
            arg_as_function_ids = [
                1,
                2,
            ],
            arg_lists = [
                (
                    optional_unit_type,
                    make_dense_array_type(t),
                    make_dense_array_type(t),
                )
                for t in scalar_types
            ],
            build_target_groups = ("on_dense_arrays",),
            op_class = "::arolla::WhereOp",
            deps = [":lib"],
        ) + operator_overload_list(
            # We explicitly instantiate (scalar, array, array) versions for performance. Also
            # core._short_circuit_where operator can fall back to them in case when there is
            # nothing to short circuit.
            hdrs = ["logic_operators.h"],
            arg_as_function_ids = [
                1,
                2,
            ],
            arg_lists = [
                (
                    optional_unit_type,
                    make_array_type(t),
                    make_array_type(t),
                )
                for t in scalar_types
            ],
            build_target_groups = ("on_arrays",),
            op_class = "::arolla::WhereOp",
            deps = [":lib"],
        )
    ),
)

operator_libraries(
    name = "operator_presence_and_or",
    operator_name = "core._presence_and_or",
    overloads =
        operator_overload_list(
            hdrs = ["logic_operators.h"],
            arg_as_function_ids = [2],
            arg_lists = [
                (
                    t,
                    optional_unit_type,
                    t,
                )
                for t in scalar_types
            ] + [
                (
                    t,
                    optional_unit_type,
                    make_optional_type(t),
                )
                for t in scalar_types
            ] + [
                (
                    make_optional_type(t),
                    optional_unit_type,
                    t,
                )
                for t in scalar_types
            ] + [
                (
                    make_optional_type(t),
                    optional_unit_type,
                    make_optional_type(t),
                )
                for t in scalar_types
            ],
            op_class = "::arolla::PresenceAndOrOp",
            deps = [":lib"],
        ),
)

operator_libraries(
    name = "operator_has",
    operator_name = "core.has._optional",
    overloads = operator_overload_list(
        hdrs = ["logic_operators.h"],
        arg_lists = [
            (make_optional_type(scalar_type),)
            for scalar_type in scalar_types
            if scalar_type != unit_type
        ],
        op_class = "::arolla::HasOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_presence_not",
    operator_name = "core.presence_not._builtin",
    overloads = operator_overload_list(
        hdrs = ["logic_operators.h"],
        arg_lists = [
            (make_optional_type(scalar_type),)
            for scalar_type in scalar_types
            # (optional_unit) belongs to the bootstrap set
            if scalar_type != unit_type
        ],
        op_class = "::arolla::PresenceNotOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_mask_equal",
    operator_name = "core.equal",
    overloads = [
        overload
        for overload in with_lifted_by(
            [
                lift_to_optional,
                lift_to_dense_array,
                lift_to_array,
            ],
            operator_overload_list(
                hdrs = ["logic_operators.h"],
                arg_lists = binary_args(scalar_types),
                op_class = "::arolla::MaskEqualOp",
                deps = [":lib"],
            ),
        )
        # (int64_t, int64_t) belongs to the bootstrap set
        if tuple(overload.args) != ("int64_t", "int64_t")
    ],
)

operator_libraries(
    name = "operator_mask_not_equal",
    operator_name = "core.not_equal",
    overloads = with_lifted_by(
        [
            lift_to_optional,
            lift_to_dense_array,
            lift_to_array,
        ],
        operator_overload_list(
            hdrs = ["logic_operators.h"],
            arg_lists = binary_args(scalar_types),
            op_class = "::arolla::MaskNotEqualOp",
            deps = [":lib"],
        ),
    ),
)

ordered_types = numeric_types + [
    "bool",
    bytes_type,
    text_type,
]

operator_libraries(
    name = "operator_mask_less",
    operator_name = "core.less",
    overloads = with_lifted_by(
        [
            lift_to_optional,
            lift_to_dense_array,
            lift_to_array,
        ],
        operator_overload_list(
            hdrs = ["logic_operators.h"],
            arg_lists = binary_args(ordered_types),
            op_class = "::arolla::MaskLessOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_mask_less_equal",
    operator_name = "core.less_equal",
    overloads = with_lifted_by(
        [
            lift_to_optional,
            lift_to_dense_array,
            lift_to_array,
        ],
        operator_overload_list(
            hdrs = ["logic_operators.h"],
            arg_lists = binary_args(ordered_types),
            op_class = "::arolla::MaskLessEqualOp",
            deps = [":lib"],
        ),
    ),
)

# Tests

cc_test(
    name = "logic_operators_test",
    srcs = [
        "logic_operators_test.cc",
    ],
    deps = [
        ":core",
        ":lib",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "utility_operators_test",
    srcs = [
        "utility_operators_test.cc",
    ],
    deps = [
        ":core",
        ":lib",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "cast_operator_test",
    srcs = [
        "cast_operator_test.cc",
    ],
    deps = [
        ":core",
        ":lib",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

scalar_shape = "::arolla::ScalarShape"

optional_scalar_shape = "::arolla::OptionalScalarShape"

scalar_to_scalar_edge = "::arolla::ScalarToScalarEdge"

operator_libraries(
    name = "operator_edge_child_shape",
    operator_name = "edge.child_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(scalar_to_scalar_edge,)],
        op_class = "::arolla::EdgeChildShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_from_shape",
    operator_name = "edge.from_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [
            (scalar_shape,),
            (optional_scalar_shape,),
        ],
        op_class = "::arolla::EdgeFromShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_parent_shape",
    operator_name = "edge.parent_shape",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(scalar_to_scalar_edge,)],
        op_class = "::arolla::EdgeParentShapeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_edge_sizes",
    operator_name = "edge.sizes",
    overloads = operator_overload_list(
        hdrs = ["edge_ops.h"],
        arg_lists = [(scalar_to_scalar_edge,)],
        op_class = "::arolla::EdgeSizesOp",
        deps = [":lib"],
    ),
)

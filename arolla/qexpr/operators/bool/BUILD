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

# Arolla three-valued bool logic operators
# go/rl2-bool

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "binary_args",
    "lift_to_optional",
    "make_optional_type",
    "operator_libraries",
    "operator_overload",
    "operator_overload_list",
    "scalar_types",
    "with_lifted_by",
)
load(
    "//arolla/qexpr/operators/array:array.bzl",
    "lift_to_array",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "lift_to_dense_array",
    "make_dense_array_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    ":operator_equal",
    ":operator_not_equal",
    ":operator_less",
    ":operator_less_equal",
    ":operator_logical_if",
    ":operator_logical_and",
    ":operator_logical_not",
    ":operator_logical_or",
]

# Registers all operators defined in the package.
cc_library(
    name = "bool",
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
    deps = [lib + "_plain" for lib in operator_lib_list] + [
        ":operator_equal_on_optionals",
        ":operator_less_equal_on_optionals",
        ":operator_less_on_optionals",
        ":operator_logical_not_on_optionals",
        ":operator_not_equal_on_optionals",
    ],
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
        "comparison.h",
        "logic.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
    ],
)

lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

operator_libraries(
    name = "operator_equal",
    operator_name = "bool.equal",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["comparison.h"],
            arg_lists = binary_args(scalar_types),
            op_class = "::arolla::EqualOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_not_equal",
    operator_name = "bool.not_equal",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["comparison.h"],
            arg_lists = binary_args(scalar_types),
            op_class = "::arolla::NotEqualOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_less",
    operator_name = "bool.less",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["comparison.h"],
            arg_lists = binary_args(scalar_types),
            op_class = "::arolla::LessOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_less_equal",
    operator_name = "bool.less_equal",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["comparison.h"],
            arg_lists = binary_args(scalar_types),
            op_class = "::arolla::LessEqualOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_logical_not",
    operator_name = "bool.logical_not",
    overloads = with_lifted_by(
        lifters,
        [operator_overload(
            hdrs = ["logic.h"],
            args = ["bool"],
            op_class = "::arolla::LogicalNotOp",
            deps = [":lib"],
        )],
    ),
)

operator_libraries(
    name = "operator_logical_and",
    operator_name = "bool.logical_and",
    overloads = with_lifted_by(
        [
            lift_to_dense_array,
            lift_to_array,
        ],
        [operator_overload(
            hdrs = ["logic.h"],
            args = [
                "::arolla::OptionalValue<bool>",
                "::arolla::OptionalValue<bool>",
            ],
            op_class = "::arolla::LogicalAndOp",
            deps = [":lib"],
        )],
    ) + [operator_overload(
        hdrs = ["logic.h"],
        args = [
            "bool",
            "bool",
        ],
        op_class = "::arolla::LogicalAndOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_logical_or",
    operator_name = "bool.logical_or",
    overloads = with_lifted_by(
        [
            lift_to_dense_array,
            lift_to_array,
        ],
        [operator_overload(
            hdrs = ["logic.h"],
            args = [
                "::arolla::OptionalValue<bool>",
                "::arolla::OptionalValue<bool>",
            ],
            op_class = "::arolla::LogicalOrOp",
            deps = [":lib"],
        )],
    ) + [operator_overload(
        hdrs = ["logic.h"],
        args = [
            "bool",
            "bool",
        ],
        op_class = "::arolla::LogicalOrOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_logical_if",
    operator_name = "bool.logical_if",
    overloads =
        operator_overload_list(
            hdrs = ["logic.h"],
            arg_as_function_ids = [
                1,
                2,
                3,
            ],
            arg_lists = [(
                make_optional_type("bool"),
                t,
                t,
                t,
            ) for t in scalar_types],
            op_class = "::arolla::LogicalIfOp",
            deps = [":lib"],
        ) + with_lifted_by(
            [
                lift_to_dense_array,
                lift_to_array,
            ],
            operator_overload_list(
                hdrs = ["logic.h"],
                arg_as_function_ids = [
                    1,
                    2,
                    3,
                ],
                arg_lists = [(
                    make_optional_type("bool"),
                    make_optional_type(t),
                    make_optional_type(t),
                    make_optional_type(t),
                ) for t in scalar_types],
                op_class = "::arolla::LogicalIfOp",
                deps = [":lib"],
            ),
        ) + operator_overload_list(
            # Specialization for DenseArray condition and scalar values. This pattern is
            # pretty common and broadcasting true_/false_/missing_value to match
            # condition shape appears to be too expensive.
            hdrs = ["logic.h"],
            arg_lists = [(
                make_dense_array_type("bool"),
                make_optional_type(t),
                make_optional_type(t),
                make_optional_type(t),
            ) for t in scalar_types],
            build_target_groups = ["on_dense_arrays"],
            op_class = "::arolla::LogicalIfOp",
            deps = [":lib"],
        ),
)

# Tests

cc_test(
    name = "comparison_test",
    srcs = [
        "comparison_test.cc",
    ],
    deps = [
        ":bool",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "logic_test",
    srcs = [
        "logic_test.cc",
    ],
    deps = [
        ":bool",
        ":lib",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

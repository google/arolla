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

# Internal test operator definitions

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "binary_args",
    "dont_lift",
    "lift_to_optional",
    "n_ary_args",
    "numeric_types",
    "operator_family",
    "operator_libraries",
    "operator_overload_list",
    "unary_args",
    "with_lifted_by",
)
load(
    "//arolla/codegen/qexpr/testing:vector3_types.bzl",
    "lift_to_vector3",
)

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "test_operators_lib",
    testonly = 1,
    hdrs = [
        "id_operator_family.h",
        "test_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "test_operators",
    testonly = 1,
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":operator_add",
        ":operator_add3",
        ":operator_dotprod",
        ":operator_eq",
        ":operator_id",
        ":operator_mul",
        ":operator_negate",
        ":operator_neq",
        ":operator_pi",
        ":operator_vector3",
        ":operator_vector_components",
    ],
    alwayslink = 1,
)

add_overloads = operator_overload_list(
    hdrs = ["test_operators.h"],
    arg_lists = binary_args(numeric_types),
    op_class = "::arolla::testing::AddOp",
    deps = [":test_operators_lib"],
)

operator_libraries(
    name = "operator_add",
    testonly = 1,
    operator_name = "test.add",
    overloads = with_lifted_by(
        [
            lift_to_vector3,
            lift_to_optional,
        ],
        add_overloads,
    ),
)

operator_libraries(
    name = "operator_add_dont_lift",
    testonly = 1,
    operator_name = "test.add_dont_lift",
    overloads = with_lifted_by(
        [
            lift_to_optional,
        ],
        operator_overload_list(
            hdrs = ["test_operators.h"],
            arg_lists = [
                [
                    "int",
                    dont_lift("int"),
                ],
            ],
            op_class = "::arolla::testing::AddOp",
            deps = [":test_operators_lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_add_with_context",
    testonly = 1,
    operator_name = "test.add_with_context",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = binary_args(numeric_types),
        op_class = "::arolla::testing::AddWithContextOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_add_with_status_or",
    testonly = 1,
    operator_name = "test.add_with_status_or",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = binary_args(numeric_types),
        op_class = "::arolla::testing::AddWithStatusOrOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_add_with_arg_as_function",
    testonly = 1,
    operator_name = "test.add_with_arg_as_function",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_as_function_ids = [1],
        arg_lists = binary_args(numeric_types),
        op_class = "::arolla::testing::AddWithArgAsFunction",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_add_with_all_args_as_function",
    testonly = 1,
    operator_name = "test.add_with_all_args_as_function",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_as_function_ids = [
            0,
            1,
        ],
        arg_lists = binary_args(numeric_types),
        op_class = "::arolla::testing::AddWithArgAsFunction",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_add3",
    testonly = 1,
    operator_name = "test.add3",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = n_ary_args(3, numeric_types),
        op_class = "::arolla::testing::Add3Op",
        deps = [":test_operators_lib"],
    ),
)

# Operator in a nested namespace for tests.
operator_libraries(
    name = "operator_nested_add",
    testonly = 1,
    operator_name = "test.nested_namespace.Add",
    overloads = with_lifted_by(
        [lift_to_vector3],
        add_overloads,
    ),
)

operator_libraries(
    name = "operator_mul",
    testonly = 1,
    operator_name = "test.mul",
    overloads = with_lifted_by(
        [lift_to_vector3],
        operator_overload_list(
            hdrs = ["test_operators.h"],
            arg_lists = binary_args(numeric_types),
            op_class = "::arolla::testing::MulOp",
            deps = [":test_operators_lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_negate",
    testonly = 1,
    operator_name = "test.negate",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = unary_args(numeric_types),
        op_class = "::arolla::testing::NegateOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_eq",
    testonly = 1,
    operator_name = "test.eq",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = binary_args(numeric_types),
        op_class = "::arolla::testing::EqOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_neq",
    testonly = 1,
    operator_name = "test.neq",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = binary_args(numeric_types),
        op_class = "::arolla::testing::NeqOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_vector3",
    testonly = 1,
    operator_name = "test.vector3",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = [
            [
                "float",
                "float",
                "float",
            ],
            [
                "double",
                "double",
                "double",
            ],
        ],
        op_class = "::arolla::testing::Vector3Op",
        deps = [":test_operators_lib"],
    ),
)

vector3_types = [
    "::arolla::testing::Vector3<float>",
    "::arolla::testing::Vector3<double>",
]

operator_libraries(
    name = "operator_dotprod",
    testonly = 1,
    operator_name = "test.dot_prod",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = binary_args(vector3_types),
        op_class = "::arolla::testing::DotProdOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_vector_components",
    testonly = 1,
    operator_name = "test.vector_components",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = unary_args(vector3_types),
        op_class = "::arolla::testing::VectorComponentsOp",
        deps = [":test_operators_lib"],
    ),
)

operator_libraries(
    name = "operator_pi",
    testonly = 1,
    operator_name = "test.pi",
    overloads = operator_overload_list(
        hdrs = ["test_operators.h"],
        arg_lists = [()],
        op_class = "::arolla::testing::PiOp",
        deps = [":test_operators_lib"],
    ),
)

operator_family(
    name = "operator_id",
    testonly = 1,
    hdrs = ["id_operator_family.h"],
    op_family_class = "::arolla::testing::IdOperatorFamily",
    op_family_name = "test.id",
    deps = [":test_operators_lib"],
)

cc_test(
    name = "registered_operators_test",
    srcs = ["registered_operators_test.cc"],
    deps = [
        ":operator_add",
        ":operator_add_dont_lift",
        ":operator_add_metadata",
        ":operator_add_with_all_args_as_function_metadata",
        ":operator_add_with_arg_as_function_metadata",
        ":operator_add_with_context_metadata",
        ":operator_add_with_status_or_metadata",
        ":operator_dotprod",
        ":operator_dotprod_metadata",
        ":operator_id",
        ":operator_id_metadata",
        ":operator_negate",
        ":operator_nested_add",
        ":operator_vector_components",
        ":test_operators_lib",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

# Verify targets created with tags (:operator_add_plain and
# :operator_add_vector3) contain the same set of operator
cc_test(
    name = "registered_operators_test_with_tags",
    srcs = ["registered_operators_test.cc"],
    deps = [
        ":operator_add_dont_lift",
        ":operator_add_metadata",
        ":operator_add_on_optionals",
        ":operator_add_on_vector3",
        ":operator_add_plain",
        ":operator_add_with_all_args_as_function_metadata",
        ":operator_add_with_arg_as_function_metadata",
        ":operator_add_with_context_metadata",
        ":operator_add_with_status_or_metadata",
        ":operator_dotprod",
        ":operator_dotprod_metadata",
        ":operator_id",
        ":operator_id_metadata",
        ":operator_negate",
        ":operator_nested_add",
        ":operator_vector_components",
        ":test_operators_lib",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

# Verify that binary can depend on both wildcard (:operator_add) and
# concrete (:operator_add_i32_and_i32) operator targets simultaneously.
cc_test(
    name = "registered_operators_test_with_duplicated_deps",
    srcs = ["registered_operators_test.cc"],
    deps = [
        ":operator_add",
        ":operator_add_dont_lift",
        ":operator_add_i32_and_i32",
        ":operator_add_metadata",
        ":operator_add_plain",
        ":operator_add_with_all_args_as_function_metadata",
        ":operator_add_with_arg_as_function_metadata",
        ":operator_add_with_context_metadata",
        ":operator_add_with_status_or_metadata",
        ":operator_dotprod",
        ":operator_dotprod_metadata",
        ":operator_id",
        ":operator_id_metadata",
        ":operator_negate",
        ":operator_nested_add",
        ":operator_vector_components",
        ":test_operators_lib",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

bzl_library(
    name = "vector3_types_bzl",
    srcs = ["vector3_types.bzl"],
    deps = ["//arolla/codegen/qexpr:register_operator_bzl"],
)

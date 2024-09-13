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
    "dict_key_types",
    "dict_types",
    "dont_lift",
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
    "make_dense_array_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

lifted_operator_libs = [
    ":operator_get",
    ":operator_contains",
]

all_operator_libs = (
    lifted_operator_libs + [
        ":operator_make",
        ":operator_keys",
    ]
)

# Registers all operators defined in the package.
cc_library(
    name = "dict",
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
        lib + "_plain"
        for lib in lifted_operator_libs
    ] + [
        lib + "_on_optionals"
        for lib in lifted_operator_libs
    ] + [
        ":operator_keys",
        ":operator_make_on_dense_arrays",
    ],
)

# Operators that may be useful to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_on_dense_arrays" for lib in lifted_operator_libs],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_on_arrays" for lib in lifted_operator_libs],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "dict_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype/dict",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

operator_libraries(
    name = "operator_make",
    operator_name = "dict._make_key_to_row_dict",
    overloads = operator_overload_list(
        hdrs = ["dict_operators.h"],
        arg_lists = [
            (make_dense_array_type(k),)
            for k in dict_key_types
        ],
        build_target_groups = ["on_dense_arrays"],
        op_class = "::arolla::MakeKeyToRowDictOp",
        deps = [":lib"],
    ),
)

all_lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

operator_libraries(
    name = "operator_get",
    operator_name = "dict._get_row",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["dict_operators.h"],
            arg_lists = [(
                dont_lift(dict_type),
                key,
            ) for key, dict_type in dict_types],
            op_class = "::arolla::DictGetRowOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_contains",
    operator_name = "dict._contains",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["dict_operators.h"],
            arg_lists = [(
                dont_lift(dict_type),
                key,
            ) for key, dict_type in dict_types],
            op_class = "::arolla::DictContainsOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_keys",
    operator_name = "dict._keys",
    overloads = operator_overload_list(
        hdrs = ["dict_operators.h"],
        arg_lists = [(dict_type,) for _, dict_type in dict_types],
        op_class = "::arolla::DictKeysOp",
        deps = [":lib"],
    ),
)

cc_test(
    name = "dict_operators_test",
    srcs = [
        "dict_operators_test.cc",
    ],
    deps = [
        ":dict",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

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

# Tools for random operators
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
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
)

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

lifted_operator_lib_list = [
    ":operator_cityhash",
]

operator_lib_list = lifted_operator_lib_list

# Registers all operators defined in the package.
cc_library(
    name = "random",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = operator_lib_list,
    alwayslink = 1,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [lib + "_metadata" for lib in operator_lib_list],
    alwayslink = 1,
)

# Operators that may be useful to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        lib + "_plain"
        for lib in operator_lib_list
    ] + [
        lib + "_on_optionals"
        for lib in operator_lib_list
    ],
)

# Operators that may be useful to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        lib + "_on_dense_arrays"
        for lib in lifted_operator_lib_list
    ],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        lib + "_on_arrays"
        for lib in lifted_operator_lib_list
    ],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "random.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/strings:string_view",
        "@com_google_cityhash//:cityhash",
    ],
)

all_lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

operator_libraries(
    name = "operator_cityhash",
    operator_name = "random._cityhash",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["random.h"],
            arg_lists = [[
                "::arolla::Text",
                dont_lift("int64_t"),
            ]],
            op_class = "::arolla::CityHashOp",
            deps = [":lib"],
        ),
    ),
)

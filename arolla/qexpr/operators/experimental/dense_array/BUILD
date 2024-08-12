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

# DenseArray-specific implementations of experimental operators

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "float_types",
    "operator_libraries",
    "operator_overload_list",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "dense_array_edge_type",
    "make_dense_array_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    ":operator_agg_moving_average",
    ":operator_ewma",
]

# Registers all operators defined in the package.
cc_library(
    name = "dense_array",
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

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "timeseries.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

# Time-series operators
operator_libraries(
    name = "operator_agg_moving_average",
    operator_name = "experimental.agg_moving_average",
    overloads = operator_overload_list(
        hdrs = ["timeseries.h"],
        arg_lists = [(
            make_dense_array_type(v),
            "int64_t",
            dense_array_edge_type,
        ) for v in float_types],
        op_class = "::arolla::AggMovingAverageOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_ewma",
    operator_name = "experimental.ewma",
    overloads = operator_overload_list(
        hdrs = ["timeseries.h"],
        arg_lists = [
            [
                make_dense_array_type(v),
                "double",
                "bool",
                "bool",
            ]
            for v in float_types
        ],
        op_class = "::arolla::ExponentialWeightedMovingAverageOp",
        deps = [":lib"],
    ),
)

# Tests

cc_test(
    name = "timeseries_test",
    srcs = [
        "timeseries_test.cc",
    ],
    deps = [
        ":dense_array",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/core",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

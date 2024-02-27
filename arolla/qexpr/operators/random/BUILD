load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

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

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

lifted_operator_lib_list = [
    ":operator_cityhash",
]

operator_lib_list = lifted_operator_lib_list

# Registers all operators defined in the package.
cc_library(
    name = "random",
    deps = operator_lib_list,
    alwayslink = 1,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    deps = [lib + "_metadata" for lib in operator_lib_list],
    alwayslink = 1,
)

# Operators that may be useful to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
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
    tags = ["keep_dep"],
    deps = [
        lib + "_on_dense_arrays"
        for lib in lifted_operator_lib_list
    ],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
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

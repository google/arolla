# Bitwise operators for RLv2

load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "binary_args",
    "integral_types",
    "lift_to_optional",
    "operator_libraries",
    "operator_overload_list",
    "unary_args",
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
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

operator_lib_list = [
    ":operator_bitwise_and",
    ":operator_bitwise_or",
    ":operator_bitwise_xor",
    ":operator_invert",
]

# Registers all operators defined in the package.
cc_library(
    name = "bitwise",
    tags = ["keep_dep"],
    deps = operator_lib_list,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    tags = ["keep_dep"],
    deps = [lib + "_metadata" for lib in operator_lib_list],
)

# Operators that may be useful to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
    tags = ["keep_dep"],
    deps = [lib + "_plain" for lib in operator_lib_list] +
           [lib + "_on_optionals" for lib in operator_lib_list],
)

# Operators that may be useful to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    tags = ["keep_dep"],
    deps = [lib + "_on_dense_arrays" for lib in operator_lib_list],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    tags = ["keep_dep"],
    deps = [lib + "_on_arrays" for lib in operator_lib_list],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "bitwise.h",
    ],
    visibility = ["//visibility:public"],
)

lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

operator_libraries(
    name = "operator_bitwise_and",
    operator_name = "bitwise.bitwise_and",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["bitwise.h"],
            arg_lists = binary_args(integral_types),
            op_class = "::arolla::BitwiseAndOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_bitwise_or",
    operator_name = "bitwise.bitwise_or",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["bitwise.h"],
            arg_lists = binary_args(integral_types),
            op_class = "::arolla::BitwiseOrOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_bitwise_xor",
    operator_name = "bitwise.bitwise_xor",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["bitwise.h"],
            arg_lists = binary_args(integral_types),
            op_class = "::arolla::BitwiseXorOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_invert",
    operator_name = "bitwise.invert",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["bitwise.h"],
            arg_lists = unary_args(integral_types),
            op_class = "::arolla::InvertOp",
            deps = [":lib"],
        ),
    ),
)

# Tests.
cc_test(
    name = "bitwise_test",
    srcs = ["bitwise_test.cc"],
    deps = [
        ":bitwise",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

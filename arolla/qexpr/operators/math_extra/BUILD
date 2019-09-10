# Extra math operator implementations, that may not be included into a standard package because of
# dependency sizes.

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "binary_args",
    "float_types",
    "lift_to_optional",
    "n_ary_args",
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
    ":operator_t_distribution_inverse_cdf",
    ":operator_beta_distribution_inverse_cdf",
    ":operator_normal_distribution_inverse_cdf",
]

cc_library(
    name = "math_extra",
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
        for lib in operator_lib_list
    ],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    tags = ["keep_dep"],
    deps = [
        lib + "_on_arrays"
        for lib in operator_lib_list
    ],
)

cc_library(
    name = "lib",
    hdrs = ["cdf.h"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@boost//:math",
"@boost//:lexical_cast",
    ],
)

# Oprator implementations

lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

operator_libraries(
    name = "operator_t_distribution_inverse_cdf",
    operator_name = "math.t_distribution_inverse_cdf",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["cdf.h"],
            arg_lists = binary_args(float_types),
            op_class = "::arolla::TDistributionInverseCdfOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_beta_distribution_inverse_cdf",
    operator_name = "math.beta_distribution_inverse_cdf",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["cdf.h"],
            arg_lists = n_ary_args(3, float_types),
            op_class = "::arolla::BetaDistributionInverseCdfOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_normal_distribution_inverse_cdf",
    operator_name = "math.normal_distribution_inverse_cdf",
    overloads = with_lifted_by(
        lifters,
        operator_overload_list(
            hdrs = ["cdf.h"],
            arg_lists = unary_args(float_types),
            op_class = "::arolla::NormalDistributionInverseCdfOp",
            deps = [":lib"],
        ),
    ),
)

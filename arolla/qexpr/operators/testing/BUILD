# Internal test operator definitions

load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "accumulator_overload",
    "binary_args",
    "float_types",
    "lift_accumulator_to_scalar",
    "lift_by",
    "operator_family",
    "operator_libraries",
    "operator_overload_list",
    "unary_args",
)

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    deps = [
        ":operator_agg_sum",
        ":operator_average",
        ":operator_fail",
        ":operator_rank_values",
        ":operator_vector2d_get_x",
        ":operator_vector2d_get_y",
        ":operator_vector2d_make",
    ],
    alwayslink = 1,
)

cc_library(
    name = "lib",
    testonly = 1,
    srcs = ["vector2d.cc"],
    hdrs = [
        "accumulators.h",
        "testing_operators.h",
        "vector2d.h",
    ],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

vector2d_types = [
    "::arolla::Vector2D<float>",
    "::arolla::Vector2D<double>",
]

operator_libraries(
    name = "operator_vector2d_make",
    testonly = 1,
    operator_name = "test.vector2d.make_vector2d",
    overloads = operator_overload_list(
        hdrs = ["vector2d.h"],
        arg_lists = binary_args(float_types),
        op_class = "::arolla::MakeVector2DOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_vector2d_get_x",
    testonly = 1,
    operator_name = "test.vector2d.get_x",
    overloads = operator_overload_list(
        hdrs = ["vector2d.h"],
        arg_lists = unary_args(vector2d_types),
        op_class = "::arolla::GetXOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_vector2d_get_y",
    testonly = 1,
    operator_name = "test.vector2d.get_y",
    overloads = operator_overload_list(
        hdrs = ["vector2d.h"],
        arg_lists = unary_args(vector2d_types),
        op_class = "::arolla::GetYOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_agg_sum",
    testonly = 1,
    operator_name = "test.agg_sum",
    overloads = lift_by(
        [lift_accumulator_to_scalar],
        [
            accumulator_overload(
                hdrs = ["accumulators.h"],
                acc_class = "::arolla::testing::AggSumAccumulator<int>",
                child_args = ["int"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_average",
    testonly = 1,
    operator_name = "test.average",
    overloads = lift_by(
        [lift_accumulator_to_scalar],
        [
            accumulator_overload(
                hdrs = ["accumulators.h"],
                acc_class = "::arolla::testing::AverageAccumulator",
                child_args = ["float"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_rank_values",
    testonly = 1,
    operator_name = "test.rank_values",
    overloads = lift_by(
        [lift_accumulator_to_scalar],
        [
            accumulator_overload(
                hdrs = ["accumulators.h"],
                acc_class = "::arolla::testing::RankValuesAccumulator<int>",
                child_args = ["int"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_family(
    name = "operator_fail",
    testonly = 1,
    hdrs = ["testing_operators.h"],
    op_family_class = "::arolla::FailOpFamily",
    op_family_name = "test.fail",
    deps = [":lib"],
)

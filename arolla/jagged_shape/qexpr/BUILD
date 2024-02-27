load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "operator_libraries",
    "operator_overload",
    "qtype_type",
)

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

operator_lib_list = [
    # go/keep-sorted start
    ":operator_jagged_get_edge_qtype",
    ":operator_jagged_is_jagged_shape_qtype",
    ":operator_jagged_make_jagged_shape_qtype",
    # go/keep-sorted end
]

cc_library(
    name = "qexpr",
    tags = ["keep_dep"],
    deps = operator_lib_list,
)

cc_library(
    name = "lib",
    srcs = [],
    hdrs = [
        "qtype_operator.h",
        "shape_operators.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/jagged_shape/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/sequence",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

operator_libraries(
    name = "operator_jagged_make_jagged_shape_qtype",
    operator_name = "jagged.make_jagged_shape_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type,),
        op_class = "::arolla::MakeJaggedShapeQTypeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_is_jagged_shape_qtype",
    operator_name = "jagged.is_jagged_shape_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type,),
        op_class = "::arolla::IsJaggedShapeQTypeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_get_edge_qtype",
    operator_name = "jagged.get_edge_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type,),
        op_class = "::arolla::GetEdgeQType",
        deps = [":lib"],
    )],
)

load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "expr_operator",
    srcs = [
        "decision_forest_operator.cc",
        "forest_model.cc",
    ],
    hdrs = [
        "decision_forest_operator.h",
        "forest_model.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/decision_forest",
        "//arolla/expr",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "forest_model_test",
    srcs = ["forest_model_test.cc"],
    deps = [
        ":expr_operator",
        "//arolla/util:status_backport",
        "//arolla/decision_forest",
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/serving:serving_lite",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "decision_forest_operator_test",
    srcs = ["decision_forest_operator_test.cc"],
    deps = [
        ":expr_operator",
        "//arolla/decision_forest",
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array/qtype",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

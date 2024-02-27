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
    name = "qtype",
    srcs = ["qtype.cc"],
    hdrs = ["qtype.h"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/qtype",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = ["qtype_test.cc"],
    deps = [
        ":qtype",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/dense_array",
        "//arolla/jagged_shape/dense_array/qtype",
        "//arolla/jagged_shape/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

# DenseArray specializations for jagged shapes.

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
    name = "dense_array",
    srcs = ["jagged_shape.cc"],
    hdrs = ["jagged_shape.h"],
    deps = [
        "//arolla/dense_array",
        "//arolla/jagged_shape",
        "//arolla/jagged_shape/util",
        "//arolla/util",
    ],
)

# TODO: describe this package.

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
    name = "default",
    srcs = ["default_optimizer.cc"],
    hdrs = ["default_optimizer.h"],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr/optimization",
        "//arolla/expr/optimization/peephole_optimizations",
        "//arolla/util",
        "@com_google_absl//absl/status:statusor",
    ],
)

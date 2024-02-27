# Example of custom QType definition.
#
# See python/arolla/examples/custom_qtype/ for the Python part.

load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "complex",
    srcs = ["complex.cc"],
    hdrs = ["complex.h"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "complex_test",
    srcs = ["complex_test.cc"],
    deps = [
        ":complex",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "complex_operators",
    srcs = ["complex_operators.cc"],
    deps = [
        ":complex",
        "//arolla/util:status_backport",
        "//arolla/optools",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
    ],
    alwayslink = 1,
)

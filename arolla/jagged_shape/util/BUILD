load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla/jagged_shape:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "util",
    srcs = ["repr.cc"],
    hdrs = ["repr.h"],
    deps = [
        "//arolla/util",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "repr_test",
    srcs = ["repr_test.cc"],
    deps = [
        ":util",
        "@com_google_googletest//:gtest_main",
    ],
)

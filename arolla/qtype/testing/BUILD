# QType-related tools for testing

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
    name = "testing",
    testonly = 1,
    hdrs = [
        "qtype.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = ["qtype_test.cc"],
    deps = [
        ":testing",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "dummy_types",
    testonly = 1,
    srcs = [
        "dummy_types.cc",
    ],
    hdrs = [
        "dummy_types.h",
    ],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
    ],
)

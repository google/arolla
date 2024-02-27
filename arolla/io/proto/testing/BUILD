load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_testonly = 1,
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "benchmark_util.cc",
    ],
    hdrs = [
        "benchmark_util.h",
    ],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto:test_benchmark_extension_cc_proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

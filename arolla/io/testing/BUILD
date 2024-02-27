load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(
    default_applicable_licenses = [
    ],
    default_testonly = 1,
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = True,
    hdrs = ["matchers.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/io",
        "//arolla/qtype",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_binary(
    name = "input_loader_benchmarks",
    testonly = 1,
    srcs = ["input_loader_benchmarks.cc"],
    deps = [
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
    ],
)

cc_test(
    name = "matchers_test",
    srcs = ["matchers_test.cc"],
    deps = [
        ":testing",
        "//arolla/io",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

benchmark_smoke_test(
    name = "input_loader_benchmarks_smoke_test",
    binary = ":input_loader_benchmarks",
)

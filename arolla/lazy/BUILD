# This library introduces a "lazy" type representing a value with
# deferred/on-demand computation.

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
    name = "lazy",
    srcs = [
        "lazy.cc",
        "lazy_qtype.cc",
    ],
    hdrs = [
        "lazy.h",
        "lazy_qtype.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "lazy_test",
    srcs = [
        "lazy_test.cc",
    ],
    deps = [
        ":lazy",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "lazy_qtype_test",
    srcs = [
        "lazy_qtype_test.cc",
    ],
    deps = [
        ":lazy",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "integration_test",
    srcs = [
        "integration_test.cc",
    ],
    deps = [
        ":lazy",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/lazy/operators",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

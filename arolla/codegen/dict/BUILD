# literal and type extensions for python/arolla/expr/codegen

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
    name = "dict",
    srcs = ["dict_codegen_literal.cc"],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
    alwayslink = 1,
)

cc_test(
    name = "dict_codegen_literal_test",
    srcs = ["dict_codegen_literal_test.cc"],
    deps = [
        ":dict",
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

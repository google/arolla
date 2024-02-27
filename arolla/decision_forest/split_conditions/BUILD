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
    name = "split_conditions",
    hdrs = [
        "interval_split_condition.h",
        "set_of_values_split_condition.h",
    ],
    deps = [
        "//arolla/decision_forest",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "split_conditions_test",
    srcs = ["split_conditions_test.cc"],
    deps = [
        ":split_conditions",
        "//arolla/decision_forest",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

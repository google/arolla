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
    name = "decision_forest",
    srcs = ["decision_forest.cc"],
    hdrs = [
        "decision_forest.h",
        "split_condition.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "decision_forest_test",
    srcs = ["decision_forest_test.cc"],
    deps = [
        ":decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

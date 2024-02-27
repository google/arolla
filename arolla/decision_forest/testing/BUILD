load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//arolla/decision_forest:__subpackages__"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "test_util.cc",
    ],
    hdrs = [
        "test_util.h",
    ],
    deps = [
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "test_util_test",
    srcs = ["test_util_test.cc"],
    deps = [
        ":testing",
        "//arolla/decision_forest",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_googletest//:gtest_main",
    ],
)

# Properties of the standard Arolla QTypes.

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
    name = "standard_type_properties",
    srcs = [
        "common_qtype.cc",
        "properties.cc",
    ],
    hdrs = [
        "common_qtype.h",
        "properties.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "properties_test",
    srcs = ["properties_test.cc"],
    deps = [
        ":standard_type_properties",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "common_qtype_test",
    srcs = ["common_qtype_test.cc"],
    deps = [
        ":standard_type_properties",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

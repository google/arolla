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
    name = "naming",
    srcs = [
        "policy.cc",
        "protopath_id.cc",
        "table.cc",
    ],
    hdrs = [
        "policy.h",
        "protopath_id.h",
        "table.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/util",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "table_test",
    srcs = ["table_test.cc"],
    deps = [
        ":naming",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "policy_test",
    srcs = ["policy_test.cc"],
    deps = [
        ":naming",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "protopath_id_test",
    srcs = ["protopath_id_test.cc"],
    deps = [
        ":naming",
        "//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

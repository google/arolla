# Utilities for proto descriptors and QTypes.

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
    name = "qtype",
    srcs = [
        "proto_qtype_map.cc",
    ],
    hdrs = [
        "proto_qtype_map.h",
    ],
    deps = [
        "//arolla/proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protoc",
    ],
)

cc_test(
    name = "proto_qtype_map_test",
    srcs = ["proto_qtype_map_test.cc"],
    deps = [
        ":qtype",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protoc",
    ],
)

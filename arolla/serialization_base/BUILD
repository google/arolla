# Base serialization facilities for arolla expressions and values.
#
# Decoder and encoder defined here know virtually nothing about actual value
# serialization. The decoder explicitly depends on ExprOperator qtype, but
# provides no serialization format for ExprOperators.
#
# All concrete types and serialization formats need to be provided externally.
#

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

proto_library(
    name = "base_proto",
    srcs = [
        "base.proto",
    ],
)

cc_proto_library(
    name = "base_cc_proto",
    deps = [
        ":base_proto",
    ],
)

cc_library(
    name = "serialization_base",
    srcs = [
        "decode.cc",
        "encode.cc",
    ],
    hdrs = [
        "decode.h",
        "encode.h",
    ],
    deps = [
        ":base_cc_proto",
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "decode_test",
    srcs = [
        "decode_test.cc",
    ],
    deps = [
        ":base_cc_proto",
        ":serialization_base",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "encode_test",
    srcs = [
        "encode_test.cc",
    ],
    deps = [
        ":base_cc_proto",
        ":serialization_base",
        "//arolla/expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

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
    name = "array_codec_proto",
    srcs = [
        "array_codec.proto",
    ],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "array_codec_cc_proto",
    deps = [
        ":array_codec_proto",
    ],
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    visibility = [":__subpackages__"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

# Serialization codecs for jagged dense array shapes.

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
    name = "serialization_codecs",
    srcs = [
        "decoder.cc",
        "encoder.cc",
    ],
    hdrs = [],
    deps = [
        ":codec_name",
        ":jagged_shape_codec_cc_proto",
        "//arolla/util:status_backport",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/jagged_shape/dense_array",
        "//arolla/jagged_shape/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/serialization:decode",
        "//arolla/serialization:encode",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

cc_library(
    name = "codec_name",
    hdrs = ["codec_name.h"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

proto_library(
    name = "jagged_shape_codec_proto",
    srcs = ["jagged_shape_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "jagged_shape_codec_cc_proto",
    deps = [":jagged_shape_codec_proto"],
)

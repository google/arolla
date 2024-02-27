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
    name = "encoders",
    srcs = [
        "dense_array_encoder.cc",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/serialization:encode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/dense_array:codec_name",
        "//arolla/serialization_codecs/dense_array:dense_array_codec_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
    alwayslink = True,
)

# See tests in python/arolla/s11n/testing
cc_test(
    name = "dense_array_encoder_test",
    srcs = ["dense_array_encoder_test.cc"],
    deps = [
        ":encoders",
        "//arolla/util:status_backport",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/serialization:encode",
        "//arolla/serialization_codecs/dense_array:dense_array_codec_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

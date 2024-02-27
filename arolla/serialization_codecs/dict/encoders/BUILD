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

# See tests in python/arolla/s11n/testing
cc_library(
    name = "encoders",
    srcs = ["dict_encoder.cc"],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/serialization:encode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/dict:codec_name",
        "//arolla/serialization_codecs/dict:dict_codec_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
)

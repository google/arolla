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

# NOTE: Consider splitting this target to subtarget to provide
# better control on what codecs get included to the binary.

# See tests in python/arolla/s11n/testing
cc_library(
    name = "decoders",
    srcs = [
        "operator_decoder.cc",
        "optional_decoder.cc",
        "scalar_decoder.cc",
        "tuple_decoder.cc",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/expr/operator_loader",
        "//arolla/expr/operators/while_loop",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/serialization:decode",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs/generic:codec_name",
        "//arolla/serialization_codecs/generic:operator_codec_cc_proto",
        "//arolla/serialization_codecs/generic:optional_codec_cc_proto",
        "//arolla/serialization_codecs/generic:scalar_codec_cc_proto",
        "//arolla/serialization_codecs/generic:tuple_codec_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

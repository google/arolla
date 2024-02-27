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

# Public deserialization API.
#
# You need to depend on decoder codecs to use it, e.g.
# //arolla/serialization_codecs:all_decoders.
#
cc_library(
    name = "decode",
    srcs = [
        "decode.cc",
    ],
    hdrs = [
        "decode.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

# Public serialization API.
#
# You need to depend on encoder codecs to use it, e.g.
# //arolla/serialization_codecs:all.
#
cc_library(
    name = "encode",
    srcs = [
        "encode.cc",
    ],
    hdrs = [
        "encode.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "serialization_test",
    srcs = ["serialization_test.cc"],
    deps = [
        ":decode",
        ":encode",
        "//arolla/util:status_backport",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/serialization_codecs:all",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

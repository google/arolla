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
    name = "sequence",
    srcs = [
        "mutable_sequence.cc",
        "sequence.cc",
        "sequence_qtype.cc",
    ],
    hdrs = [
        "mutable_sequence.h",
        "sequence.h",
        "sequence_qtype.h",
    ],
    deps = [
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "mutable_sequence_test",
    srcs = [
        "mutable_sequence_test.cc",
    ],
    deps = [
        ":sequence",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "sequence_test",
    srcs = [
        "sequence_test.cc",
    ],
    deps = [
        ":sequence",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "sequence_qtype_test",
    srcs = [
        "sequence_qtype_test.cc",
    ],
    deps = [
        ":sequence",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

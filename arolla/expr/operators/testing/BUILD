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
    name = "testing",
    testonly = True,
    srcs = ["register_operators.cc"],
    tags = ["keep_dep"],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/qexpr/operators/testing:lib",
        "//arolla/qtype",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

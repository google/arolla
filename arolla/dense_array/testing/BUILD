load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_testonly = 1,
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    srcs = ["bound_operators.cc"],
    hdrs = [
        "bound_operators.h",
        "util.h",
    ],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/util",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

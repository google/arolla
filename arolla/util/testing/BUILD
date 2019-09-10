load("@rules_python//python:py_binary.bzl", "py_binary")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(
    default_applicable_licenses = [
    ],
    default_testonly = 1,
    default_visibility = ["//visibility:private"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "status_matchers_backport.cc",
    ],
    hdrs = [
        "equals_proto.h",
        "repr_token_eq.h",
        "status_matchers_backport.h",
    ],
    visibility = [
        "//python/arolla/expr/codegen/testing:__subpackages__",
        "//arolla:internal",
    ],
    deps = [
        "@com_google_googletest//:gtest",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protoc",
        "//arolla/util",
        "//arolla/util:status_backport",
    ],
)

py_binary(
    name = "run_benchmark_smoke_test",
    srcs = ["run_benchmark_smoke_test.py"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl_py//absl:app",
        "@com_google_absl_py//absl/flags",
    ],
)

exports_files(["run_smoke_test.sh"])

bzl_library(
    name = "testing_bzl",
    srcs = ["testing.bzl"],
    visibility = ["//visibility:public"],
    deps = ["//arolla/codegen:utils_bzl"],
)

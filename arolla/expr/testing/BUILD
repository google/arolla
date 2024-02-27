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
    srcs = [
        "annotations.cc",
        "annotations.h",
        "invoke.cc",
        "invoke.h",
        "matchers.h",
    ],
    hdrs = [
        "testing.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators:bootstrap",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest",
    ],
)

cc_library(
    name = "test_operators",
    srcs = [
        "register_operators.cc",
        "test_operators.cc",
    ],
    hdrs = [
        "test_operators.h",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/all",
        "//arolla/expr/operators/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

cc_test(
    name = "invoke_test",
    srcs = ["invoke_test.cc"],
    deps = [
        ":testing",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

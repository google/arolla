load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/operator_package:operator_package.bzl",
    "arolla_cc_embed_operator_package",
)


package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "all",
    tags = ["keep_dep"],
    deps = [
        ":standard_cpp",
        ":standard_operator_package",
        "//arolla/serialization_codecs/generic/decoders",
    ],
)

cc_library(
    name = "standard_cpp",
    srcs = [
        "standard_cpp.cc",
    ],
    deps = [
        "//arolla/util:status_backport",
        "//arolla/expr/operators",
        "//arolla/expr/operators/strings",
        "//arolla/util",
        "@com_google_absl//absl/status",
    ],
    alwayslink = 1,
)

# We use this target only in copybara/gerrit environment.
arolla_cc_embed_operator_package(
    name = "standard_operator_package_snapshot",
    srcs = [
        "standard.operator_package.pb2.snapshot",
    ],
    priority_name = "kRegisterExprOperatorsStandard",
    visibility = ["//visibility:private"],
)

alias(
    name = "standard_operator_package",
    actual = ":standard_operator_package_snapshot",
    visibility = ["//visibility:private"],
)


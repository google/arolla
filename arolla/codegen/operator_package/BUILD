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

exports_files(["operator_package.cc.jinja2"])

bzl_library(
    name = "operator_package_bzl",
    srcs = ["operator_package.bzl"],
    deps = ["//arolla/codegen:utils_bzl"],
)

arolla_cc_embed_operator_package(
    name = "arolla_cc_embed_operator_package_test",
    testonly = True,
    srcs = [
        "test.bin",
    ],
    priority_name = "kLowest",
)


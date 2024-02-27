load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")
load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package_group(
    name = "internal",
    packages = [
        # make sure we can define codegen models in the directories without explicit dependency.
        "-//python/arolla/expr/codegen/testing/...",
        "//python/arolla/...",
        "//arolla/...",
    ],
)

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

exports_files(["LICENSE"])

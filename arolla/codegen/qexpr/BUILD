# Tools to generate QExpr operators

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

exports_files(
    [
        "simple_operator.cc.jinja2",
        "simple_operator_metadata.cc.jinja2",
        "operator_family_registration.cc.jinja2",
        "operator_family_metadata.cc.jinja2",
    ],
)

# Flag to enable debug information generation for QExpr operators. It can be
# useful for detailed stacktraces and benchmarking.
# Usage: pass
#   --//arolla/codegen/qexpr:include_operators_debug_info=true
# to your blaze command.
bool_flag(
    name = "include_operators_debug_info",
    build_setting_default = False,
    visibility = ["//visibility:public"],
)

config_setting(
    name = "include_operators_debug_info_enabled",
    flag_values = {
        ":include_operators_debug_info": "true",
    },
)

bzl_library(
    name = "register_operator_bzl",
    srcs = ["register_operator.bzl"],
    deps = [
        "//arolla/codegen:utils_bzl",
    ],
)

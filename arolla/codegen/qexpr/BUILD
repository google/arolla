# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Tools to generate QExpr operators

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@bazel_skylib//rules:common_settings.bzl", "bool_flag")

package(default_visibility = ["//visibility:public"])

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

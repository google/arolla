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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load(
    "//arolla/codegen/operator_package:operator_package.bzl",
    "arolla_cc_embed_operator_package",
)

package(default_visibility = ["//visibility:public"])

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

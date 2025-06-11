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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("//py/arolla/optools:optools.bzl", "arolla_cc_operator_package")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

arolla_cc_operator_package(
    name = "operators",
    arolla_initializer = {
        "name": "arolla_operators/derived_qtype",
        "deps": ["arolla_operators/derived_qtype:bootstrap"],
    },
    snapshot = "//py/arolla/derived_qtype/operators:operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [":bootstrap"],
)

cc_library(
    name = "bootstrap",
    srcs = ["bootstrap.cc"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//arolla:internal"],
    deps = [
        "//arolla/util",
        "@com_google_absl//absl/status",
    ],
    alwayslink = True,
)

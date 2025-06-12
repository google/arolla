# Copyright 2025 Google LLC
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

# Arolla operators available only in Python.

load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

cc_library(
    name = "py",
    visibility = ["//arolla:internal"],
    deps = [
        ":operator_py_call",
    ],
    alwayslink = True,
)

cc_library(
    name = "operator_py_call",
    srcs = ["operator_py_call.cc"],
    deps = [
        "//arolla/expr",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

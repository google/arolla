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

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")
load("//py/arolla/optools:optools.bzl", "arolla_operator_package_snapshot")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "lazy",
    srcs = ["lazy.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":clib",
        "//py/arolla",
    ],
)

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = ["arolla.lazy.lazy"],
    visibility = ["//arolla:internal"],
    deps = [":lazy"],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    deps = [
        "//arolla/lazy",
        "//arolla/lazy/operators:qexpr",
        "//arolla/qtype",
        "//py/arolla/abc:py_abc",
        "//py/arolla/abc:pybind11_utils",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
)

py_test(
    name = "lazy_test",
    srcs = ["lazy_test.py"],
    deps = [
        ":lazy",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

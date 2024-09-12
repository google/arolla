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

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "gf",
    srcs = ["gf.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":gf127",
        "//py/arolla",
    ],
)

py_test(
    name = "gf_test",
    srcs = ["gf_test.py"],
    deps = [
        ":gf",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

arolla_pybind_extension(
    name = "gf127",
    srcs = ["gf127.cc"],
    pytype_deps = ["//py/arolla"],
    pytype_srcs = ["gf127.pyi"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/abc:pybind11_utils",
        "@com_google_absl//absl/base:no_destructor",
    ],
)

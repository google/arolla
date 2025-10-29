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

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

licenses(["notice"])

py_library(
    name = "derived_qtype",
    srcs = ["derived_qtype.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/arolla",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    dynamic_deps = ["//py/arolla/dynamic_deps:derived_qtype_so"],
    pytype_deps = [
        "//py/arolla",
        "//py/arolla/abc",
    ],
    pytype_srcs = ["clib.pyi"],
    deps = [
        "//arolla/derived_qtype",
        "//arolla/derived_qtype/operators",
        "//arolla/derived_qtype/s11n",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/abc:pybind11_utils",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/strings:str_format",
    ],
)

py_test(
    name = "labeled_qtype_repr_test",
    srcs = ["labeled_qtype_repr_test.py"],
    deps = [
        ":derived_qtype",
        "//py:python_path",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

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

package(default_visibility = ["//py/arolla/s11n:__subpackages__"])

licenses(["notice"])

py_library(
    name = "s11n",
    srcs = ["s11n.py"],
    visibility = ["//arolla:internal"],
    deps = [
        ":clib",
        "//arolla/serialization_base:base_py_proto",
        "//py/arolla/abc",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization:decode",
        "//arolla/serialization:encode",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:all",
        "//py/arolla/abc:pybind11_utils",
    ],
)

py_test(
    name = "s11n_test",
    srcs = ["s11n_test.py"],
    deps = [
        ":s11n",
        "//arolla/serialization_base:base_py_proto",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

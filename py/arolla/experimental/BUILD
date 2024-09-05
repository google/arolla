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

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "experimental",
    srcs = [
        "auto_eval_view.py",
        "dataset.py",
        "dense_array_numpy_conversion.py",
        "eval_util.py",
        "numpy_conversion.py",
        "reload.py",
        "table.py",
    ],
    tags = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":clib",
        "//py/arolla",
        requirement("numpy"),
    ],
)

py_test(
    name = "eval_util_test",
    srcs = ["eval_util_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "auto_eval_view_test",
    srcs = ["auto_eval_view_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "dataset_test",
    srcs = ["dataset_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "reload_test",
    srcs = ["reload_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "table_test",
    srcs = ["table_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "dense_array_numpy_conversion_test",
    srcs = ["dense_array_numpy_conversion_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "numpy_conversion_test",
    srcs = ["numpy_conversion_test.py"],
    deps = [
        ":experimental",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_deps = ["//py/arolla"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//py/arolla/abc:pybind11_utils",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

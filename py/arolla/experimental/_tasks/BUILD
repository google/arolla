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

"""Experimental API for working with cancellable tasks in Python.

This module provides the internal implementation. Please import the public API
instead: `from arolla.experimental import tasks`
"""

load("@pybind11_bazel//:build_defs.bzl", "pybind_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//py/arolla/experimental:__subpackages__"])

licenses(["notice"])

py_library(
    name = "callbacks",
    srcs = ["callbacks.py"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/arolla",
    ],
)

py_test(
    name = "callbacks_test",
    srcs = ["callbacks_test.py"],
    deps = [
        ":callbacks",
        ":clib",
        ":testing_clib",
        "//py:python_path",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

pybind_library(
    name = "python_callback_bridge",
    srcs = ["python_callback_bridge.cc"],
    hdrs = ["python_callback_bridge.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//py/arolla/py_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/synchronization",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        ":python_callback_bridge",
        "//arolla/util",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
    ],
)

arolla_pybind_extension(
    name = "testing_clib",
    testonly = True,
    srcs = ["testing_clib.cc"],
    deps = [
        ":python_callback_bridge",
        "@com_google_absl//absl/time",
    ],
)

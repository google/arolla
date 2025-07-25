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

# C++ helpers for Python C API.

load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_test")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

cc_library(
    name = "py_utils",
    srcs = [
        "py_cancellation_controller.cc",
        "py_utils.cc",
    ],
    hdrs = [
        "py_cancellation_controller.h",
        "py_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":error_converter_registry",
        ":py_object_ptr_impl",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

cc_library(
    name = "error_converter_registry",
    srcs = ["error_converter_registry.cc"],
    hdrs = ["error_converter_registry.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "error_converter_registry_test",
    srcs = ["error_converter_registry_test.cc"],
    deps = [
        ":error_converter_registry",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "py_object_ptr_impl",
    hdrs = ["py_object_ptr_impl.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

cc_test(
    name = "py_object_ptr_impl_test",
    srcs = ["py_object_ptr_impl_test.cc"],
    deps = [
        ":py_object_ptr_impl",
        "@com_google_googletest//:gtest_main",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

pybind_extension(
    name = "testing_clib",
    testonly = True,
    srcs = ["testing_clib.cc"],
    deps = [
        ":py_utils",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
    ],
)

py_test(
    name = "py_utils_test",
    srcs = ["py_utils_test.py"],
    deps = [
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

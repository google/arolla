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

# C++ helpers for Python C API.

load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

cc_library(
    name = "py_utils",
    srcs = [
        "py_object_as_status_payload.cc",
        "py_utils.cc",
    ],
    hdrs = [
        "py_object_as_status_payload.h",
        "py_utils.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":py_object_ptr_impl",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

cc_library(
    name = "py_object_ptr_impl",
    hdrs = [
        "py_object_ptr_impl.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

cc_test(
    name = "py_object_ptr_impl_test",
    srcs = [
        "py_object_ptr_impl_test.cc",
    ],
    deps = [
        ":py_object_ptr_impl",
        "@com_google_googletest//:gtest_main",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

pybind_extension(
    name = "testing_clib",
    srcs = ["testing_clib.cc"],
    deps = [
        ":py_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@pybind11_abseil//pybind11_abseil:status_casters",
    ],
)

py_test(
    name = "py_utils_test",
    srcs = ["py_utils_test.py"],
    data = ["@pybind11_abseil//pybind11_abseil:status.so"],
    deps = [
        ":testing_clib",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

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

# QType: extensible type system for Arolla evaluation.

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//py/arolla/types:__subpackages__"])

licenses(["notice"])

py_library(
    name = "qtype",
    srcs = [
        "array_qtypes.py",
        "boxing.py",
        "casting.py",
        "dense_array_qtypes.py",
        "dict_qtypes.py",
        "optional_qtypes.py",
        "scalar_qtypes.py",
        "scalar_utils.py",
        "sequence_qtypes.py",
        "tuple_qtypes.py",
    ],
    deps = [
        ":clib",
        "//py/arolla/abc",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_deps = ["//py/arolla/abc"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        ":array_boxing",
        ":scalar_boxing",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/dict",
        "//arolla/sequence",
        "//arolla/util",
        "//py/arolla/abc:pybind11_utils",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

py_test(
    name = "boxing_test",
    srcs = [
        "boxing_test.py",
    ],
    deps = [
        ":qtype",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "casting_test",
    srcs = [
        "casting_test.py",
    ],
    deps = [
        ":qtype",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

cc_library(
    name = "scalar_boxing",
    srcs = ["scalar_boxing.cc"],
    hdrs = ["scalar_boxing.h"],
    deps = [
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

cc_library(
    name = "array_boxing",
    srcs = ["array_boxing.cc"],
    hdrs = ["array_boxing.h"],
    deps = [
        ":scalar_boxing",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_test(
    name = "dense_array_qtypes_test",
    srcs = [
        "dense_array_qtypes_test.py",
    ],
    deps = [
        ":qtype",
        ":scalar_utils_test_helpers",
        "//py/arolla/operators:operators_clib",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "optional_qtypes_test",
    srcs = [
        "optional_qtypes_test.py",
    ],
    deps = [
        ":qtype",
        ":scalar_utils_test_helpers",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_qtypes_test",
    srcs = [
        "array_qtypes_test.py",
    ],
    deps = [
        ":qtype",
        ":scalar_utils_test_helpers",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "scalar_qtypes_test",
    srcs = [
        "scalar_qtypes_test.py",
    ],
    deps = [
        ":qtype",
        ":scalar_utils_test_helpers",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "scalar_utils_test",
    srcs = [
        "scalar_utils_test.py",
    ],
    deps = [
        ":qtype",
        ":scalar_utils_test_helpers",
        "//py/arolla/abc",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "scalar_utils_test_helpers",
    testonly = True,
    srcs = [
        "scalar_utils_test_helpers.py",
    ],
    deps = [
        ":qtype",
        requirement("numpy"),
    ],
)

py_test(
    name = "sequence_qtypes_test",
    srcs = [
        "sequence_qtypes_test.py",
    ],
    deps = [
        ":qtype",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "tuple_qtypes_test",
    srcs = [
        "tuple_qtypes_test.py",
    ],
    deps = [
        ":qtype",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dict_qtypes_test",
    srcs = ["dict_qtypes_test.py"],
    deps = [
        ":qtype",
        "//py/arolla/abc",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

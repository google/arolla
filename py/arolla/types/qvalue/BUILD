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

package(default_visibility = ["//py/arolla/types:__subpackages__"])

licenses(["notice"])

py_library(
    name = "qvalue",
    srcs = [
        "array_qvalues.py",
        "backend_operator_qvalues.py",
        "basic_array_helpers.py",
        "dense_array_qvalues.py",
        "dict_qvalues.py",
        "dispatch_operator_qvalues.py",
        "dummy_operator_qvalues.py",
        "generic_operator_qvalues.py",
        "get_nth_operator_qvalues.py",
        "helpers.py",
        "lambda_operator_qvalues.py",
        "optional_qvalues.py",
        "overload_operator_helpers.py",
        "overloaded_operator_qvalues.py",
        "py_function_operator_qvalues.py",
        "qvalue_mixins.py",
        "restricted_lambda_operator_qvalues.py",
        "scalar_qvalues.py",
        "sequence_qvalues.py",
        "slice_qvalues.py",
        "tuple_qvalues.py",
    ],
    deps = [
        ":clib",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    deps = [
        ":py_function_operator",
        "//arolla/expr",
        "//arolla/expr/operator_loader",
        "//arolla/qtype",
        "//py/arolla/abc:pybind11_utils",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

py_test(
    name = "qvalue_mixins_test",
    srcs = [
        "qvalue_mixins_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "scalar_qvalues_test",
    srcs = [
        "scalar_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "optional_qvalues_test",
    srcs = [
        "optional_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dense_array_qvalues_test",
    srcs = [
        "dense_array_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_qvalues_test",
    srcs = [
        "array_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "tuple_qvalues_test",
    srcs = [
        "tuple_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/testing",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "slice_qvalues_test",
    srcs = [
        "slice_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "get_nth_operator_qvalues_test",
    srcs = [
        "get_nth_operator_qvalues_test.py",
    ],
    deps = [
        ":clib",
        ":qvalue",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "lambda_operator_qvalues_test",
    srcs = [
        "lambda_operator_qvalues_test.py",
    ],
    deps = [
        ":clib",
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dict_qvalues_test",
    srcs = [
        "dict_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/operator_tests:utils",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "overloaded_operator_qvalues_test",
    srcs = [
        "overloaded_operator_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dispatch_operator_qvalues_test",
    srcs = [
        "dispatch_operator_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "generic_operator_qvalues_test",
    srcs = [
        "generic_operator_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "overload_operator_helpers_test",
    srcs = [
        "overload_operator_helpers_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/testing",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "restricted_lambda_operator_qvalues_test",
    srcs = [
        "restricted_lambda_operator_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "sequence_qvalues_test",
    srcs = [
        "sequence_qvalues_test.py",
    ],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/testing",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dummy_operator_qvalues_test",
    srcs = ["dummy_operator_qvalues_test.py"],
    deps = [
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/s11n",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "py_function_operator_qvalues_test",
    srcs = ["py_function_operator_qvalues_test.py"],
    deps = [
        ":clib",
        ":qvalue",
        "//py/arolla/abc",
        "//py/arolla/operators:operators_clib",
        "//py/arolla/testing",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

cc_library(
    name = "py_function_operator",
    srcs = ["py_function_operator.cc"],
    hdrs = ["py_function_operator.h"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/operator_loader",
        "//arolla/expr/operators:bootstrap",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

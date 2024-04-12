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

package(default_visibility = ["//py/arolla/types:__subpackages__"])

licenses(["notice"])

py_library(
    name = "qvalue",
    srcs = [
        "array_qvalue.py",
        "backend_operator_qvalue.py",
        "basic_array_helpers.py",
        "dense_array_qvalue.py",
        "dict_qvalue.py",
        "dispatch_operator_qvalue.py",
        "dummy_operator_qvalue.py",
        "generic_operator_qvalue.py",
        "get_nth_operator_qvalue.py",
        "helpers.py",
        "lambda_operator_qvalue.py",
        "optional_qvalue.py",
        "overload_operator_helpers.py",
        "overloaded_operator_qvalue.py",
        "py_function_operator_qvalue.py",
        "qvalue_mixins.py",
        "restricted_lambda_operator_qvalue.py",
        "scalar_qvalue.py",
        "sequence_qvalue.py",
        "slice_qvalue.py",
        "tuple_qvalue.py",
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
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "scalar_qvalue_test",
    srcs = [
        "scalar_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "optional_qvalue_test",
    srcs = [
        "optional_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dense_array_qvalue_test",
    srcs = [
        "dense_array_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_qvalue_test",
    srcs = [
        "array_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        requirement("numpy"),
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "tuple_qvalue_test",
    srcs = [
        "tuple_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "slice_qvalue_test",
    srcs = [
        "slice_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "get_nth_operator_qvalue_test",
    srcs = [
        "get_nth_operator_qvalue_test.py",
    ],
    deps = [
        ":clib",
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "lambda_operator_qvalue_test",
    srcs = [
        "lambda_operator_qvalue_test.py",
    ],
    deps = [
        ":clib",
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "overloaded_operator_qvalue_test",
    srcs = [
        "overloaded_operator_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dispatch_operator_qvalue_test",
    srcs = [
        "dispatch_operator_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "generic_operator_qvalue_test",
    srcs = [
        "generic_operator_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
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
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/testing",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "restricted_lambda_operator_qvalue_test",
    srcs = [
        "restricted_lambda_operator_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "sequence_qvalue_test",
    srcs = [
        "sequence_qvalue_test.py",
    ],
    deps = [
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/testing",
        "//py/arolla/types/qtype",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dummy_operator_qvalue_test",
    srcs = ["dummy_operator_qvalue_test.py"],
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
    name = "py_function_operator_qvalue_test",
    srcs = ["py_function_operator_qvalue_test.py"],
    deps = [
        ":clib",
        ":qvalue",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//py/arolla/abc",
        "//py/arolla/expr",
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

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

# QType: extensible type system for Arolla evaluation.

load("@pip_deps//:requirements.bzl", "requirement")
load("@pybind11_bazel//:build_defs.bzl", "pybind_library")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//py/arolla/abc:__subpackages__"])

licenses(["notice"])

py_library(
    name = "abc",
    srcs = ["abc.py"],
    visibility = ["//arolla:internal"],
    deps = [
        ":lib",
        ":utils",
        "//py:python_path",
    ],
)

py_library(
    name = "lib",
    srcs = [
        "attr.py",
        "aux_binding_policy.py",
        "cancellation.py",
        "expr.py",
        "expr_substitution.py",
        "expr_view.py",
        "expr_visitor.py",
        "operator.py",
        "operator_repr.py",
        "py_object_qtype.py",
        "qexpr.py",
        "qtype.py",
        "signature.py",
    ],
    deps = [
        ":clib",
        ":utils",
        "//py:python_path",
    ],
)

py_library(
    name = "utils",
    srcs = ["utils.py"],
    deps = [
        ":clib",
        ":dummy_numpy",
        "//py:python_path",
    ],
)

py_library(
    name = "dummy_numpy",
    srcs = ["dummy_numpy.py"],
    deps = ["//py:python_path"],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        ":error_converters",
        ":py_abc",
        ":pybind11_utils",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/visitors",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

pybind_library(
    name = "pybind11_utils",
    hdrs = ["pybind11_utils.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":py_abc",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/log:check",
    ],
)

py_test(
    name = "pybind11_utils_test",
    size = "small",
    srcs = ["pybind11_utils_test.py"],
    deps = [
        ":clib",
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

arolla_pybind_extension(
    name = "testing_clib",
    testonly = True,
    srcs = ["testing_clib.cc"],
    deps = [
        ":py_abc",
        ":pybind11_utils",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators:bootstrap",
        "//arolla/qtype",
        "//arolla/util",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
    ],
)

arolla_pybind_extension(
    name = "dummy_types",
    testonly = True,
    srcs = ["dummy_types.cc"],
    deps = [
        ":pybind11_utils",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_library(
    name = "error_converters",
    srcs = ["error_converters.cc"],
    visibility = ["//visibility:private"],
    deps = [
        "//arolla/expr/eval",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//py/arolla/py_utils",
        "//py/arolla/py_utils:error_converter_registry",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
    alwayslink = True,
)

cc_library(
    name = "py_abc",
    srcs = [
        "py_abc_binding_policies.cc",
        "py_attr.cc",
        "py_aux_binding_policy.cc",
        "py_bind.cc",
        "py_cached_eval.cc",
        "py_cancellation.cc",
        "py_compiled_expr.cc",
        "py_eval.cc",
        "py_expr.cc",
        "py_expr_compilation_options.cc",
        "py_expr_quote.cc",
        "py_expr_view.cc",
        "py_fingerprint.cc",
        "py_misc.cc",
        "py_object_qtype.cc",
        "py_operator.cc",
        "py_qtype.cc",
        "py_qvalue.cc",
        "py_qvalue_specialization.cc",
        "py_signature.cc",
    ],
    hdrs = [
        "py_abc_binding_policies.h",
        "py_attr.h",
        "py_aux_binding_policy.h",
        "py_bind.h",
        "py_cached_eval.h",
        "py_cancellation.h",
        "py_compiled_expr.h",
        "py_eval.h",
        "py_expr.h",
        "py_expr_compilation_options.h",
        "py_expr_quote.h",
        "py_expr_view.h",
        "py_fingerprint.h",
        "py_misc.h",
        "py_object_qtype.h",
        "py_operator.h",
        "py_qtype.h",
        "py_qvalue.h",
        "py_qvalue_specialization.h",
        "py_signature.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/expr",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/serialization",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serving",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_test(
    name = "attr_test",
    size = "small",
    srcs = ["attr_test.py"],
    deps = [
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "cancellation_test",
    size = "small",
    srcs = ["cancellation_test.py"],
    deps = [
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "qvalue_test",
    size = "small",
    srcs = ["qvalue_test.py"],
    deps = [
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_test",
    size = "small",
    srcs = ["qtype_test.py"],
    deps = [
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qvalue_specialization_test",
    size = "small",
    srcs = ["qvalue_specialization_test.py"],
    deps = [
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qvalue_pickle_test",
    size = "small",
    srcs = ["qvalue_pickle_test.py"],
    deps = [
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "//py/arolla/s11n",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "make_operator_node_test",
    size = "small",
    srcs = ["make_operator_node_test.py"],
    deps = [
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "expr_test",
    size = "small",
    srcs = ["expr_test.py"],
    deps = [
        ":clib",
        ":dummy_types",
        ":lib",
        ":testing_clib",
        ":utils",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "expr_view_test",
    size = "small",
    srcs = ["expr_view_test.py"],
    deps = [
        ":clib",
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "expr_quote_test",
    size = "small",
    srcs = ["expr_quote_test.py"],
    deps = [
        ":clib",
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "compiled_expr_test",
    size = "small",
    srcs = ["compiled_expr_test.py"],
    deps = [
        ":clib",
        ":lib",
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "invoke_op_test",
    size = "small",
    srcs = ["invoke_op_test.py"],
    deps = [
        ":clib",
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "eval_expr_test",
    size = "small",
    srcs = ["eval_expr_test.py"],
    deps = [
        ":clib",
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "aux_eval_op_test",
    size = "small",
    srcs = ["aux_eval_op_test.py"],
    deps = [
        ":clib",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "signature_test",
    size = "small",
    srcs = ["signature_test.py"],
    deps = [
        ":clib",
        ":lib",
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "aux_binding_policy_test",
    size = "small",
    srcs = ["aux_binding_policy_test.py"],
    deps = [
        ":clib",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "operator_test",
    size = "small",
    srcs = ["operator_test.py"],
    deps = [
        ":clib",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "py_object_qtype_test",
    size = "small",
    srcs = ["py_object_qtype_test.py"],
    deps = [
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "utils_test",
    size = "small",
    srcs = ["utils_test.py"],
    deps = [
        ":lib",
        ":utils",
        "//py:python_path",
        requirement("numpy"),  # buildcleaner: keep
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "expr_visitor_test",
    size = "small",
    srcs = ["expr_visitor_test.py"],
    deps = [
        ":clib",
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "expr_substitution_test",
    size = "small",
    srcs = ["expr_substitution_test.py"],
    deps = [
        ":lib",
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "operator_repr_test",
    size = "small",
    srcs = ["operator_repr_test.py"],
    deps = [
        "//py:python_path",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "expr_pickle_test",
    size = "small",
    srcs = ["expr_pickle_test.py"],
    deps = [
        ":dummy_types",
        ":lib",
        "//py:python_path",
        "//py/arolla/s11n",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "error_converters_test",
    size = "small",
    srcs = ["error_converters_test.py"],
    deps = [
        ":abc",
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

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

load("@rules_python//python:defs.bzl", "py_test")

package(
    default_testonly = 1,
    default_visibility = ["//arolla:internal"],
)

licenses(["notice"])

py_test(
    name = "jagged_make_jagged_shape_qtype_test",
    srcs = ["jagged_make_jagged_shape_qtype_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_is_jagged_shape_qtype_test",
    srcs = ["jagged_is_jagged_shape_qtype_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_rank_test",
    srcs = ["jagged_rank_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_array_shape_from_edges_test",
    srcs = ["jagged_array_shape_from_edges_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_dense_array_shape_from_edges_test",
    srcs = ["jagged_dense_array_shape_from_edges_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_shape_from_edges_test",
    srcs = ["jagged_shape_from_edges_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_get_edge_qtype_test",
    srcs = ["jagged_get_edge_qtype_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_edge_at_test",
    srcs = ["jagged_edge_at_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_remove_dims_test",
    srcs = ["jagged_remove_dims_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_flatten_test",
    srcs = ["jagged_flatten_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_add_dims_test",
    srcs = ["jagged_add_dims_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_edges_test",
    srcs = ["jagged_edges_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_size_test",
    srcs = ["jagged_size_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_is_broadcastable_to_test",
    srcs = ["jagged_is_broadcastable_to_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "jagged_equal_test",
    srcs = ["jagged_equal_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/operator_tests:utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

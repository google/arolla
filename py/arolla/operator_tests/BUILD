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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load(":arolla_operator_test_def.bzl", "arolla_operator_test")

package(
    default_testonly = 1,
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

py_library(
    name = "utils",
    srcs = [
        "backend_test_base.py",
        "dict_test_utils.py",
        "pointwise_test_utils.py",
        "utils.py",
    ],
    deps = [
        ":backend_test_base_flags",
        "//py/arolla",
        "@com_google_absl_py//absl/flags",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "pointwise_test_utils_test",
    srcs = [
        "pointwise_test_utils_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "utils_test",
    srcs = [
        "utils_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

bzl_library(
    name = "arolla_operator_test_def_bzl",
    srcs = ["arolla_operator_test_def.bzl"],
    deps = [
        "//arolla/codegen:utils_bzl",
        "@rules_python//python:defs.bzl",
    ],
)

py_test(
    name = "backend_test_base_test",
    srcs = ["backend_test_base_test.py"],
    deps = [
        ":utils",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/flags",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:flagsaver",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "backend_test_base_flags",
    srcs = ["backend_test_base_flags.py"],
    deps = [
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/flags",
    ],
)

py_test(
    name = "backend_test_base_flags_test",
    srcs = ["backend_test_base_flags_test.py"],
    deps = [
        ":backend_test_base_flags",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:flagsaver",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_expand_test_base",
    srcs = ["array_expand_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_agg_index_test_base",
    srcs = [
        "array_agg_index_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_agg_index_test",
    test_libraries = [":array_agg_index_test_base"],
)

py_test(
    name = "edge_indices_test",
    srcs = [
        "edge_indices_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_collapse_test_base",
    srcs = [
        "array_collapse_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_collapse_test",
    test_libraries = [":array_collapse_test_base"],
)

py_library(
    name = "array_from_indices_and_values_test_base",
    srcs = [
        "array_from_indices_and_values_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_from_indices_and_values_test",
    test_libraries = [":array_from_indices_and_values_test_base"],
)

py_test(
    name = "annotation_name_test",
    srcs = ["annotation_name_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_ordinal_rank_test_base",
    srcs = ["array_ordinal_rank_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_ordinal_rank_test",
    test_libraries = [":array_ordinal_rank_test_base"],
)

py_library(
    name = "array_dense_rank_test_base",
    srcs = ["array_dense_rank_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_dense_rank_test",
    test_libraries = [":array_dense_rank_test_base"],
)

py_library(
    name = "array_cum_count_test_base",
    srcs = ["array_cum_count_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_cum_count_test",
    test_libraries = [":array_cum_count_test_base"],
)

py_library(
    name = "array_unique_test_base",
    srcs = ["array_unique_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_unique_test",
    test_libraries = [":array_unique_test_base"],
)

py_library(
    name = "array_interleave_test_base",
    srcs = ["array_interleave_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "//py/arolla/types",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_interleave_test",
    test_libraries = [":array_interleave_test_base"],
)

py_library(
    name = "edge_from_shape_test_base",
    srcs = ["edge_from_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_from_shape_test",
    test_libraries = [":edge_from_shape_test_base"],
)

py_library(
    name = "edge_from_sizes_test_base",
    srcs = ["edge_from_sizes_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_from_sizes_test",
    test_libraries = [":edge_from_sizes_test_base"],
)

py_library(
    name = "edge_pair_left_test_base",
    srcs = ["edge_pair_left_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_pair_left_test",
    test_libraries = [":edge_pair_left_test_base"],
)

py_library(
    name = "edge_pair_right_test_base",
    srcs = ["edge_pair_right_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_pair_right_test",
    test_libraries = [":edge_pair_right_test_base"],
)

py_library(
    name = "edge_from_split_points_test_base",
    srcs = ["edge_from_split_points_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_from_split_points_test",
    test_libraries = [":edge_from_split_points_test_base"],
)

py_test(
    name = "edge_resize_groups_child_side_test",
    srcs = ["edge_resize_groups_child_side_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "edge_resize_groups_parent_side_test",
    srcs = ["edge_resize_groups_parent_side_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "edge_from_keys_test_base",
    srcs = ["edge_from_keys_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_from_keys_test",
    test_libraries = [":edge_from_keys_test_base"],
)

py_test(
    name = "edge_as_dense_array_edge_test",
    srcs = ["edge_as_dense_array_edge_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "edge_sizes_test_base",
    srcs = [
        "edge_sizes_test.py",
    ],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_sizes_test",
    test_libraries = [":edge_sizes_test_base"],
)

py_test(
    name = "array_size_test",
    srcs = ["array_size_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_at_test_base",
    srcs = ["array_at_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_at_test",
    test_libraries = [":array_at_test_base"],
)

py_test(
    name = "array_slice_test",
    srcs = ["array_slice_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_concat_test",
    srcs = ["array_concat_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "edge_child_size_test",
    srcs = ["edge_child_size_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_take_test_base",
    srcs = ["array_take_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_take_test",
    test_libraries = [":array_take_test_base"],
)

py_library(
    name = "array_count_test_base",
    srcs = ["array_count_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_count_test",
    test_libraries = [":array_count_test_base"],
)

py_library(
    name = "math_inverse_cdf_test_base",
    srcs = ["math_inverse_cdf_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_inverse_cdf_test",
    test_libraries = [":math_inverse_cdf_test_base"],
)

py_library(
    name = "math_sum_test_base",
    srcs = ["math_sum_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_sum_test",
    test_libraries = [":math_sum_test_base"],
)

py_library(
    name = "math_sum_sparse_test_base",
    srcs = ["math_sum_sparse_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_sum_sparse_test",
    test_libraries = [":math_sum_sparse_test_base"],
)

py_library(
    name = "math_softmax_test_base",
    srcs = ["math_softmax_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_softmax_test",
    test_libraries = [":math_softmax_test_base"],
)

py_library(
    name = "math_weighted_average_test_base",
    srcs = ["math_weighted_average_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_weighted_average_test",
    test_libraries = [":math_weighted_average_test_base"],
)

py_library(
    name = "math_var_test_base",
    srcs = ["math_var_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_var_test",
    timeout = "long",
    test_libraries = [":math_var_test_base"],
)

py_library(
    name = "math_std_test_base",
    srcs = ["math_std_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_std_test",
    timeout = "long",
    test_libraries = [":math_std_test_base"],
)

py_library(
    name = "math_skew_test_base",
    srcs = ["math_skew_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_skew_test",
    timeout = "long",
    test_libraries = [":math_skew_test_base"],
)

py_library(
    name = "math_kurtosis_test_base",
    srcs = ["math_kurtosis_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_kurtosis_test",
    timeout = "long",
    test_libraries = [":math_kurtosis_test_base"],
)

py_library(
    name = "math_float_test_base",
    srcs = ["math_float_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_float_test",
    test_libraries = [":math_float_test_base"],
)

py_library(
    name = "math_prod_test_base",
    srcs = ["math_prod_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_prod_test",
    test_libraries = [":math_prod_test_base"],
)

py_library(
    name = "math_mean_test_base",
    srcs = ["math_mean_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_mean_test",
    test_libraries = [":math_mean_test_base"],
)

py_test(
    name = "math_t_distribution_inverse_cdf_test",
    timeout = "long",
    srcs = ["math_t_distribution_inverse_cdf_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_beta_distribution_inverse_cdf_test",
    timeout = "long",
    srcs = ["math_beta_distribution_inverse_cdf_test.py"],
    shard_count = 50,  # Lots of test cases.
    deps = [
        ":utils",
        "//py/arolla",
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_normal_distribution_inverse_cdf_test",
    srcs = ["math_normal_distribution_inverse_cdf_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_inverse_mapping_test_base",
    srcs = ["array_inverse_mapping_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_inverse_mapping_test",
    test_libraries = [":array_inverse_mapping_test_base"],
)

py_test(
    name = "array_as_dense_array_test",
    srcs = ["array_as_dense_array_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "array_make_dense_array_test_base",
    srcs = ["array_make_dense_array_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_make_dense_array_test",
    test_libraries = [":array_make_dense_array_test_base"],
)

py_test(
    name = "array_as_array_test",
    srcs = ["array_as_array_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_shaped_test",
    srcs = ["array_shaped_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "edge_to_scalar_test",
    srcs = ["edge_to_scalar_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "edge_to_single_test_base",
    srcs = ["edge_to_single_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_to_single_test",
    test_libraries = [":edge_to_single_test_base"],
)

py_test(
    name = "array_present_indices_test",
    srcs = ["array_present_indices_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_present_values_test",
    srcs = ["array_present_values_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "experimental_ewma_test",
    srcs = ["experimental_ewma_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "experimental_serial_correlation_test",
    srcs = ["experimental_serial_correlation_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "array_randint_with_shape_test",
    srcs = ["array_randint_with_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_randint_like_test",
    srcs = ["array_randint_like_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "edge_parent_size_test",
    srcs = ["edge_parent_size_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bitwise_invert_test",
    srcs = ["bitwise_invert_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bitwise_bitwise_and_test",
    srcs = ["bitwise_bitwise_and_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bitwise_bitwise_or_test",
    srcs = ["bitwise_bitwise_or_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bitwise_bitwise_xor_test",
    srcs = ["bitwise_bitwise_xor_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "bool_logical_all_test_base",
    srcs = ["bool_logical_all_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_logical_all_test",
    test_libraries = [":bool_logical_all_test_base"],
)

py_library(
    name = "bool_logical_any_test_base",
    srcs = ["bool_logical_any_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_logical_any_test",
    test_libraries = [":bool_logical_any_test_base"],
)

py_library(
    name = "bool_equal_test_base",
    srcs = ["bool_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_equal_test",
    test_libraries = [":bool_equal_test_base"],
)

py_test(
    name = "bool_greater_equal_test",
    srcs = ["bool_greater_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bool_greater_test",
    srcs = ["bool_greater_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "bool_less_equal_test_base",
    srcs = ["bool_less_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_less_equal_test",
    test_libraries = [":bool_less_equal_test_base"],
)

py_library(
    name = "bool_less_test_base",
    srcs = ["bool_less_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_less_test",
    test_libraries = [":bool_less_test_base"],
)

py_library(
    name = "bool_logical_and_test_base",
    srcs = ["bool_logical_and_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_logical_and_test",
    test_libraries = [":bool_logical_and_test_base"],
)

py_library(
    name = "bool_logical_if_test_helper",
    srcs = ["bool_logical_if_test_helper.py"],
    deps = [
        ":utils",
        "//py/arolla",
    ],
)

py_test(
    name = "bool_logical_if_sig_test",
    srcs = ["bool_logical_if_sig_test.py"],
    deps = [
        ":bool_logical_if_test_helper",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "bool_logical_if_val_test_base",
    srcs = ["bool_logical_if_val_test.py"],
    deps = [
        ":bool_logical_if_test_helper",
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_logical_if_val_test",
    timeout = "long",
    test_libraries = [":bool_logical_if_val_test_base"],
)

py_library(
    name = "bool_logical_not_test_base",
    srcs = ["bool_logical_not_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_logical_not_test",
    test_libraries = [":bool_logical_not_test_base"],
)

py_library(
    name = "bool_logical_or_test_base",
    srcs = ["bool_logical_or_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_logical_or_test",
    test_libraries = [":bool_logical_or_test_base"],
)

py_library(
    name = "bool_not_equal_test_base",
    srcs = ["bool_not_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "bool_not_equal_test",
    test_libraries = [":bool_not_equal_test_base"],
)

py_library(
    name = "core_all_test_base",
    srcs = ["core_all_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_all_test",
    test_libraries = [":core_all_test_base"],
)

py_library(
    name = "core_any_test_base",
    srcs = ["core_any_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_any_test",
    test_libraries = [":core_any_test_base"],
)

py_library(
    name = "core_get_nth_test_base",
    srcs = ["core_get_nth_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_get_nth_test",
    test_libraries = [":core_get_nth_test_base"],
)

py_library(
    name = "core_get_first_test_base",
    srcs = ["core_get_first_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

arolla_operator_test(
    name = "core_get_first_test",
    test_libraries = [":core_get_first_test_base"],
)

py_library(
    name = "core_get_second_test_base",
    srcs = ["core_get_second_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

arolla_operator_test(
    name = "core_get_second_test",
    test_libraries = [":core_get_second_test_base"],
)

py_test(
    name = "core_apply_varargs_test",
    srcs = ["core_apply_varargs_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_concat_tuples_test",
    srcs = ["core_concat_tuples_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "namedtuple_get_field_test",
    srcs = ["namedtuple_get_field_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_reduce_tuple_test",
    srcs = ["core_reduce_tuple_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_map_test",
    srcs = ["core_map_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_map_tuple_test",
    srcs = ["core_map_tuple_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "core_cast_test",
    srcs = ["core_cast_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_cast_values_test",
    srcs = ["core_cast_values_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "core_equal_test_base",
    srcs = ["core_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_equal_test",
    test_libraries = [":core_equal_test_base"],
)

py_test(
    name = "core_getattr_test",
    srcs = ["core_getattr_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "core_getitem_test",
    srcs = ["core_getitem_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "core_not_equal_test_base",
    srcs = ["core_not_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_not_equal_test",
    test_libraries = [":core_not_equal_test_base"],
)

py_test(
    name = "core_ones_like_test",
    srcs = ["core_ones_like_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "core_less_test_base",
    srcs = ["core_less_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_less_test",
    test_libraries = [":core_less_test_base"],
)

py_test(
    name = "namedtuple_make_test",
    srcs = ["namedtuple_make_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "core_less_equal_test_base",
    srcs = ["core_less_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_less_equal_test",
    test_libraries = [":core_less_equal_test_base"],
)

py_library(
    name = "core_greater_equal_test_base",
    srcs = ["core_greater_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_greater_equal_test",
    test_libraries = [":core_greater_equal_test_base"],
)

py_library(
    name = "core_greater_test_base",
    srcs = ["core_greater_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_greater_test",
    test_libraries = [":core_greater_test_base"],
)

py_library(
    name = "core_presence_and_test_base",
    srcs = ["core_presence_and_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_presence_and_test",
    test_libraries = [":core_presence_and_test_base"],
)

py_test(
    name = "core_presence_and_or_test",
    srcs = ["core_presence_and_or_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "core_has_test_base",
    srcs = ["core_has_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_has_test",
    test_libraries = [":core_has_test_base"],
)

py_library(
    name = "core_presence_not_test_base",
    srcs = ["core_presence_not_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_presence_not_test",
    test_libraries = [":core_presence_not_test_base"],
)

py_library(
    name = "core_presence_or_test_base",
    srcs = ["core_presence_or_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_presence_or_test",
    test_libraries = [":core_presence_or_test_base"],
)

py_test(
    name = "core_broadcast_like_test",
    srcs = ["core_broadcast_like_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "core_to_bool_test_base",
    srcs = ["core_to_bool_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_to_bool_test",
    test_libraries = [":core_to_bool_test_base"],
)

py_library(
    name = "core_to_int32_test_base",
    srcs = ["core_to_int32_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_to_int32_test",
    test_libraries = [":core_to_int32_test_base"],
)

py_library(
    name = "core_to_int64_test_base",
    srcs = ["core_to_int64_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_to_int64_test",
    test_libraries = [":core_to_int64_test_base"],
)

py_library(
    name = "core_to_optional_test_base",
    srcs = ["core_to_optional_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_to_optional_test",
    test_libraries = [":core_to_optional_test_base"],
)

py_library(
    name = "core_get_optional_value_test_base",
    srcs = ["core_get_optional_value_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_get_optional_value_test",
    test_libraries = [":core_get_optional_value_test_base"],
)

py_library(
    name = "core_to_float32_test_base",
    srcs = ["core_to_float32_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_to_float32_test",
    test_libraries = [":core_to_float32_test_base"],
)

py_library(
    name = "core__to_weak_float_test_base",
    srcs = ["core__to_weak_float_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core__to_weak_float_test",
    test_libraries = [":core__to_weak_float_test_base"],
)

py_library(
    name = "core_to_float64_test_base",
    srcs = ["core_to_float64_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_to_float64_test",
    test_libraries = [":core_to_float64_test_base"],
)

py_library(
    name = "dict_get_test_base",
    srcs = ["dict_get_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict_get_test",
    test_libraries = [":dict_get_test_base"],
)

py_library(
    name = "dict__get_row_test_base",
    srcs = ["dict__get_row_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict__get_row_test",
    test_libraries = [":dict__get_row_test_base"],
)

py_library(
    name = "dict__contains_test_base",
    srcs = ["dict__contains_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict__contains_test",
    test_libraries = [":dict__contains_test_base"],
)

py_library(
    name = "dict_keys_test_base",
    srcs = ["dict_keys_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict_keys_test",
    test_libraries = [":dict_keys_test_base"],
)

py_test(
    name = "dict_values_test",
    srcs = ["dict_values_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_zeros_like_test",
    srcs = ["core_zeros_like_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_const_like_test",
    srcs = ["core_const_like_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_zip_test",
    srcs = ["core_zip_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "derived_qtype_downcast_test",
    srcs = ["derived_qtype_downcast_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/examples/gf",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "derived_qtype_upcast_test",
    srcs = ["derived_qtype_upcast_test.py"],
    deps = [
        "//py/arolla",
        "//py/arolla/examples/gf",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_abs_test_base",
    srcs = ["math_abs_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_abs_test",
    test_libraries = [":math_abs_test_base"],
)

py_library(
    name = "math_add_test_base",
    srcs = ["math_add_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_add_test",
    test_libraries = [":math_add_test_base"],
)

py_library(
    name = "strings_join_test_base",
    srcs = ["strings_join_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_join_test",
    test_libraries = [":strings_join_test_base"],
)

py_library(
    name = "strings_join_with_separator_test_base",
    srcs = ["strings_join_with_separator_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_join_with_separator_test",
    test_libraries = [":strings_join_with_separator_test_base"],
)

py_library(
    name = "math_median_test_base",
    srcs = ["math_median_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_median_test",
    test_libraries = [":math_median_test_base"],
)

py_test(
    name = "math_searchsorted_test",
    srcs = ["math_searchsorted_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_ceil_test_base",
    srcs = ["math_ceil_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_ceil_test",
    test_libraries = [":math_ceil_test_base"],
)

py_test(
    name = "math_correlation_test",
    srcs = ["math_correlation_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_covariance_test",
    srcs = ["math_covariance_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_divide_test_base",
    srcs = ["math_divide_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_divide_test",
    test_libraries = [":math_divide_test_base"],
)

py_test(
    name = "math_expm1_test",
    srcs = ["math_expm1_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_exp_test_base",
    srcs = ["math_exp_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_exp_test",
    test_libraries = [":math_exp_test_base"],
)

py_library(
    name = "math_floordiv_test_base",
    srcs = ["math_floordiv_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_floordiv_test",
    test_libraries = [":math_floordiv_test_base"],
)

py_library(
    name = "math_floor_test_base",
    srcs = ["math_floor_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_floor_test",
    test_libraries = [":math_floor_test_base"],
)

py_test(
    name = "math_fmod_test",
    srcs = ["math_fmod_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_is_close_test",
    timeout = "long",
    srcs = ["math_is_close_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_is_finite_test",
    srcs = ["math_is_finite_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_is_inf_test",
    srcs = ["math_is_inf_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_is_nan_test",
    srcs = ["math_is_nan_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_log10_test",
    srcs = ["math_log10_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_log1p_test",
    srcs = ["math_log1p_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_log2_test",
    srcs = ["math_log2_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_logit_test_base",
    srcs = ["math_logit_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_logit_test",
    test_libraries = [":math_logit_test_base"],
)

py_test(
    name = "math_log_sigmoid_test",
    srcs = ["math_log_sigmoid_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_log_test_base",
    srcs = ["math_log_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_log_test",
    test_libraries = [":math_log_test_base"],
)

py_library(
    name = "math_maximum_test_base",
    srcs = ["math_maximum_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_maximum_test",
    test_libraries = [":math_maximum_test_base"],
)

py_library(
    name = "math_minimum_test_base",
    srcs = ["math_minimum_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_minimum_test",
    test_libraries = [":math_minimum_test_base"],
)

py_library(
    name = "math_mod_test_base",
    srcs = ["math_mod_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_mod_test",
    test_libraries = [":math_mod_test_base"],
)

py_library(
    name = "math_multiply_test_base",
    srcs = ["math_multiply_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_multiply_test",
    test_libraries = [":math_multiply_test_base"],
)

py_library(
    name = "math_neg_test_base",
    srcs = ["math_neg_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_neg_test",
    test_libraries = [":math_neg_test_base"],
)

py_test(
    name = "math_pos_test",
    srcs = ["math_pos_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_pow_test_base",
    srcs = ["math_pow_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_pow_test",
    test_libraries = [":math_pow_test_base"],
)

py_library(
    name = "math_round_test_base",
    srcs = ["math_round_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_round_test",
    test_libraries = [":math_round_test_base"],
)

py_test(
    name = "math_sign_test",
    srcs = ["math_sign_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_subtract_test_base",
    srcs = ["math_subtract_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_subtract_test",
    test_libraries = [":math_subtract_test_base"],
)

py_test(
    name = "math_symlog1p_test",
    srcs = ["math_symlog1p_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_trig_cos_test",
    srcs = ["math_trig_cos_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_trig_sin_test",
    srcs = ["math_trig_sin_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_trig_sinh_test",
    srcs = ["math_trig_sinh_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_trig_atan_test",
    srcs = ["math_trig_atan_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "math_cdf_test",
    srcs = ["math_cdf_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_all_test",
    srcs = ["seq_all_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_any_test",
    srcs = ["seq_any_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_all_equal_test",
    srcs = ["seq_all_equal_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_at_test",
    srcs = ["seq_at_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_map_test",
    srcs = ["seq_map_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_slice_test",
    srcs = ["seq_slice_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_range_test",
    srcs = ["seq_range_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_reduce_test",
    srcs = ["seq_reduce_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_size_test",
    srcs = ["seq_size_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_zip_test",
    srcs = ["seq_zip_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "seq_make_test",
    srcs = ["seq_make_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "strings_lower_test_base",
    srcs = ["strings_lower_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_lower_test",
    test_libraries = [":strings_lower_test_base"],
)

py_library(
    name = "strings_upper_test_base",
    srcs = ["strings_upper_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_upper_test",
    test_libraries = [":strings_upper_test_base"],
)

py_library(
    name = "strings_as_text_test_base",
    srcs = ["strings_as_text_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_as_text_test",
    test_libraries = [":strings_as_text_test_base"],
)

py_library(
    name = "strings_decode_test_base",
    srcs = ["strings_decode_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "strings_static_decode_test",
    srcs = ["strings_static_decode_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_decode_test",
    test_libraries = [":strings_decode_test_base"],
)

py_library(
    name = "strings_encode_test_base",
    srcs = ["strings_encode_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_encode_test",
    test_libraries = [":strings_encode_test_base"],
)

py_library(
    name = "strings_parse_float_data",
    srcs = ["strings_parse_float_data.py"],
)

py_test(
    name = "strings_parse_float32_test",
    srcs = ["strings_parse_float32_test.py"],
    deps = [
        ":strings_parse_float_data",
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "strings_parse_float64_test",
    srcs = ["strings_parse_float64_test.py"],
    deps = [
        ":strings_parse_float_data",
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "strings_parse_int_data",
    srcs = ["strings_parse_int_data.py"],
)

py_test(
    name = "strings_parse_int32_test",
    srcs = ["strings_parse_int32_test.py"],
    deps = [
        ":strings_parse_int_data",
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "strings_parse_int64_test",
    srcs = ["strings_parse_int64_test.py"],
    deps = [
        ":strings_parse_int_data",
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "strings_agg_join_test_base",
    srcs = ["strings_agg_join_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_agg_join_test",
    test_libraries = [":strings_agg_join_test_base"],
)

py_test(
    name = "qtype_broadcast_qtype_like_test",
    srcs = ["qtype_broadcast_qtype_like_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_common_qtype_test",
    srcs = ["qtype_common_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_decay_derived_qtype_test",
    srcs = ["qtype_decay_derived_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "//py/arolla/examples/gf",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype__get_key_to_row_dict_qtype_test",
    srcs = ["qtype__get_key_to_row_dict_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_dict_key_qtype_test",
    srcs = ["qtype_get_dict_key_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_dict_value_qtype_test",
    srcs = ["qtype_get_dict_value_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype__get_key_to_row_dict_key_qtype_test",
    srcs = ["qtype__get_key_to_row_dict_key_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_child_shape_qtype_test",
    srcs = ["qtype_get_child_shape_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_edge_qtype_test",
    srcs = ["qtype_get_edge_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_edge_to_scalar_qtype_test",
    srcs = ["qtype_get_edge_to_scalar_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_field_count_test",
    srcs = ["qtype_get_field_count_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_field_qtype_test",
    srcs = ["qtype_get_field_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_field_qtypes_test",
    srcs = ["qtype_get_field_qtypes_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_parent_shape_qtype_test",
    srcs = ["qtype_get_parent_shape_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_presence_qtype_test",
    srcs = ["qtype_get_presence_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_scalar_qtype_test",
    srcs = ["qtype_get_scalar_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_shape_qtype_test",
    srcs = ["qtype_get_shape_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_value_qtype_test",
    srcs = ["qtype_get_value_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_get_float_qtype_test",
    srcs = ["qtype_get_float_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_array_qtype_test",
    srcs = ["qtype_is_array_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_dense_array_qtype_test",
    srcs = ["qtype_is_dense_array_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_derived_qtype_test",
    srcs = ["qtype_is_derived_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "//py/arolla/examples/gf",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_tuple_qtype_test",
    srcs = ["qtype_is_tuple_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_namedtuple_qtype_test",
    srcs = ["qtype_is_namedtuple_qtype_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_edge_qtype_test",
    srcs = ["qtype_is_edge_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_edge_to_scalar_qtype_test",
    srcs = ["qtype_is_edge_to_scalar_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_scalar_qtype_test",
    srcs = ["qtype_is_scalar_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_sequence_qtype_test",
    srcs = ["qtype_is_sequence_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_optional_qtype_test",
    srcs = ["qtype_is_optional_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_shape_qtype_test",
    srcs = ["qtype_is_shape_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_dict_qtype_test",
    srcs = ["qtype_is_dict_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_make_sequence_qtype_test",
    srcs = ["qtype_make_sequence_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_make_tuple_qtype_test",
    srcs = ["qtype_make_tuple_qtype_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_make_dict_qtype_test",
    srcs = ["qtype_make_dict_qtype_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_slice_tuple_qtype_test",
    srcs = ["qtype_slice_tuple_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_qtype_of_test",
    srcs = ["qtype_qtype_of_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_scalar_qtype_of_test",
    srcs = ["qtype_scalar_qtype_of_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_with_value_qtype_test",
    srcs = ["qtype_with_value_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_expand_test",
    test_libraries = [":array_expand_test_base"],
)

py_library(
    name = "edge_mapping_test_base",
    srcs = ["edge_mapping_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_mapping_test",
    test_libraries = [":edge_mapping_test_base"],
)

py_library(
    name = "edge_from_mapping_test_base",
    srcs = ["edge_from_mapping_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_from_mapping_test",
    test_libraries = [":edge_from_mapping_test_base"],
)

py_library(
    name = "edge_group_by_test_base",
    srcs = ["edge_group_by_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_group_by_test",
    test_libraries = [":edge_group_by_test_base"],
)

py_library(
    name = "edge_group_by2_test_base",
    srcs = ["edge_group_by2_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_group_by2_test",
    test_libraries = [":edge_group_by2_test_base"],
)

py_library(
    name = "edge_from_sizes_or_shape_test_base",
    srcs = ["edge_from_sizes_or_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_from_sizes_or_shape_test",
    test_libraries = [":edge_from_sizes_or_shape_test_base"],
)

py_library(
    name = "edge_child_shape_test_base",
    srcs = ["edge_child_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_child_shape_test",
    test_libraries = [":edge_child_shape_test_base"],
)

py_library(
    name = "edge_compose_test_base",
    srcs = ["edge_compose_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_compose_test",
    test_libraries = [":edge_compose_test_base"],
)

py_library(
    name = "array_array_shape_size_test_base",
    srcs = ["array_array_shape_size_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_array_shape_size_test",
    test_libraries = [":array_array_shape_size_test_base"],
)

py_library(
    name = "array_resize_array_shape_test_base",
    srcs = ["array_resize_array_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_resize_array_shape_test",
    test_libraries = [":array_resize_array_shape_test_base"],
)

py_library(
    name = "array_make_array_shape_test_base",
    srcs = ["array_make_array_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_make_array_shape_test",
    test_libraries = [":array_make_array_shape_test_base"],
)

py_library(
    name = "array_make_dense_array_shape_test_base",
    srcs = ["array_make_dense_array_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_make_dense_array_shape_test",
    test_libraries = [":array_make_dense_array_shape_test_base"],
)

py_library(
    name = "array_select_test_base",
    srcs = ["array_select_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "array_select_test",
    test_libraries = [":array_select_test_base"],
)

py_library(
    name = "core_const_with_shape_test_base",
    srcs = ["core_const_with_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_const_with_shape_test",
    test_libraries = [":core_const_with_shape_test_base"],
)

py_library(
    name = "core_shape_of_test_base",
    srcs = ["core_shape_of_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_shape_of_test",
    test_libraries = [":core_shape_of_test_base"],
)

py_library(
    name = "math_sigmoid_test_base",
    srcs = ["math_sigmoid_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        requirement("scipy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_sigmoid_test",
    timeout = "long",
    shard_count = 50,  # Lots of test cases.
    test_libraries = [":math_sigmoid_test_base"],
)

py_test(
    name = "core_apply_test",
    srcs = ["core_apply_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_coalesce_units_test",
    srcs = ["core_coalesce_units_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_default_if_unspecified_test",
    srcs = ["core_default_if_unspecified_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "core_with_assertion_test_base",
    srcs = ["core_with_assertion_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_with_assertion_test",
    test_libraries = [":core_with_assertion_test_base"],
)

py_library(
    name = "core_where_test_base",
    srcs = ["core_where_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "core_where_test",
    test_libraries = [":core_where_test_base"],
)

py_test(
    name = "core_short_circuit_where_test",
    srcs = ["core_short_circuit_where_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "math_max_test_base",
    srcs = ["math_max_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_max_test",
    test_libraries = [":math_max_test_base"],
)

py_library(
    name = "math_min_test_base",
    srcs = ["math_min_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        requirement("numpy"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_min_test",
    test_libraries = [":math_min_test_base"],
)

py_library(
    name = "dict__make_key_to_row_dict_test_base",
    srcs = ["dict__make_key_to_row_dict_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict__make_key_to_row_dict_test",
    test_libraries = [":dict__make_key_to_row_dict_test_base"],
)

py_library(
    name = "dict__keys_test_base",
    srcs = ["dict__keys_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict__keys_test",
    test_libraries = [":dict__keys_test_base"],
)

py_library(
    name = "dict_make_test_base",
    srcs = ["dict_make_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "dict_make_test",
    test_libraries = [":dict_make_test_base"],
)

py_library(
    name = "math_cum_sum_test_base",
    srcs = ["math_cum_sum_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_cum_sum_test",
    test_libraries = [":math_cum_sum_test_base"],
)

py_library(
    name = "edge_parent_shape_test_base",
    srcs = ["edge_parent_shape_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "edge_parent_shape_test",
    test_libraries = [":edge_parent_shape_test_base"],
)

py_test(
    name = "array_iota_test",
    srcs = ["array_iota_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "strings_printf_test_base",
    srcs = ["strings_printf_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_printf_test",
    test_libraries = [":strings_printf_test_base"],
)

py_library(
    name = "strings_format_test_base",
    srcs = ["strings_format_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_format_test",
    shard_count = 7,  # Quite a lot of test cases.
    test_libraries = [":strings_format_test_base"],
)

py_test(
    name = "random_cityhash_test",
    srcs = ["random_cityhash_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "random_sample_test",
    srcs = ["random_sample_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "random_sample_n_test",
    srcs = ["random_sample_n_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_make_slice_qtype_test",
    srcs = ["qtype_make_slice_qtype_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "qtype_is_slice_qtype_test",
    srcs = ["qtype_is_slice_qtype_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "core_make_slice_test",
    srcs = ["core_make_slice_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "strings_contains_test_base",
    srcs = ["strings_contains_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_contains_test",
    test_libraries = [":strings_contains_test_base"],
)

py_library(
    name = "strings_contains_regex_test_base",
    srcs = ["strings_contains_regex_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_contains_regex_test",
    test_libraries = [":strings_contains_regex_test_base"],
)

py_library(
    name = "strings_extract_regex_test_base",
    srcs = ["strings_extract_regex_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_extract_regex_test",
    test_libraries = [":strings_extract_regex_test_base"],
)

py_library(
    name = "strings_count_test_base",
    srcs = ["strings_count_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_count_test",
    test_libraries = [":strings_count_test_base"],
)

py_library(
    name = "strings_length_test_base",
    srcs = ["strings_length_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_length_test",
    test_libraries = [":strings_length_test_base"],
)

py_library(
    name = "strings_find_test_base",
    srcs = ["strings_find_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_find_test",
    test_libraries = [":strings_find_test_base"],
)

py_library(
    name = "strings_rfind_test_base",
    srcs = ["strings_rfind_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_rfind_test",
    test_libraries = [":strings_rfind_test_base"],
)

py_library(
    name = "strings_replace_test_base",
    srcs = ["strings_replace_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_replace_test",
    test_libraries = [":strings_replace_test_base"],
)

py_library(
    name = "strings_strip_test_base",
    srcs = ["strings_strip_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_strip_test",
    test_libraries = [":strings_strip_test_base"],
)

py_library(
    name = "strings_lstrip_test_base",
    srcs = ["strings_lstrip_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_lstrip_test",
    test_libraries = [":strings_lstrip_test_base"],
)

py_library(
    name = "strings_rstrip_test_base",
    srcs = ["strings_rstrip_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_rstrip_test",
    test_libraries = [":strings_rstrip_test_base"],
)

py_library(
    name = "strings_split_test_base",
    srcs = ["strings_split_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_split_test",
    test_libraries = [":strings_split_test_base"],
)

py_library(
    name = "math_cum_max_test_base",
    srcs = ["math_cum_max_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_cum_max_test",
    test_libraries = [":math_cum_max_test_base"],
)

py_library(
    name = "math_cum_min_test_base",
    srcs = ["math_cum_min_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "math_cum_min_test",
    test_libraries = [":math_cum_min_test_base"],
)

py_test(
    name = "namedtuple_union_test",
    srcs = ["namedtuple_union_test.py"],
    deps = [
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "strings_substr_test_base",
    srcs = ["strings_substr_test.py"],
    deps = [
        ":utils",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

arolla_operator_test(
    name = "strings_substr_test",
    test_libraries = [":strings_substr_test_base"],
)

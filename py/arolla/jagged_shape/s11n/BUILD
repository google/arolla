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

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

py_test(
    name = "s11n_test",
    srcs = ["s11n_test.py"],
    deps = [
        "//arolla/jagged_shape/array/serialization_codecs:jagged_shape_codec_py_proto",
        "//arolla/jagged_shape/dense_array/serialization_codecs:jagged_shape_codec_py_proto",
        "//arolla/serialization_codecs/array:array_codec_py_proto",
        "//arolla/serialization_codecs/dense_array:dense_array_codec_py_proto",
        "//py/arolla",
        "//py/arolla/jagged_shape",
        "//py/arolla/s11n/testing:codec_test_case",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

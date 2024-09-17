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

package(
    default_testonly = 1,
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

py_library(
    name = "codec_test_case",
    srcs = [
        "codec_test_case.py",
    ],
    deps = [
        "//arolla/serialization_base:base_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_protobuf//:protobuf_python",
    ],
)

py_test(
    name = "operator_codec_test",
    srcs = ["operator_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/generic:operator_codec_py_proto",
        "//arolla/serialization_codecs/generic:optional_codec_py_proto",
        "//arolla/serialization_codecs/generic:scalar_codec_py_proto",
        "//py/arolla",
        "//py/arolla/while_loop",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "scalar_codec_test",
    srcs = ["scalar_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/generic:scalar_codec_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "optional_codec_test",
    srcs = ["optional_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/generic:optional_codec_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "tuple_codec_test",
    srcs = ["tuple_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/generic:tuple_codec_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dense_array_codec_test",
    srcs = ["dense_array_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/dense_array:dense_array_codec_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "array_codec_test",
    srcs = ["array_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/array:array_codec_py_proto",
        "//arolla/serialization_codecs/dense_array:dense_array_codec_py_proto",
        "//arolla/serialization_codecs/generic:optional_codec_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dict_codec_test",
    srcs = ["dict_codec_test.py"],
    deps = [
        ":codec_test_case",
        "//arolla/serialization_codecs/dict:dict_codec_py_proto",
        "//arolla/serialization_codecs/generic:scalar_codec_py_proto",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

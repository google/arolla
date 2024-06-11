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
    name = "s11n",
    srcs = ["py_object_s11n.py"],
    deps = [
        ":clib",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    deps = [
        ":py_object_codec",
        "//arolla/qtype",
        "//py/arolla/abc:pybind11_utils",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

py_test(
    name = "py_object_s11n_test",
    srcs = ["py_object_s11n_test.py"],
    deps = [
        ":clib",
        ":py_object_s11n_test_helper",
        ":s11n",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "registered_py_object_codecs_test",
    srcs = ["registered_py_object_codecs_test.py"],
    deps = [
        ":py_object_s11n_test_helper",
        ":registered_py_object_codecs",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "py_object_pickle_codec_test",
    srcs = ["py_object_pickle_codec_test.py"],
    deps = [
        ":py_object_pickle_codec",
        ":py_object_s11n_test_helper",
        ":s11n",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "py_object_reference_codec_test",
    srcs = ["py_object_reference_codec_test.py"],
    tags = ["nooss"],
    deps = [
        ":py_object_reference_codec",
        ":py_object_s11n_test_helper",
        ":s11n",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "py_object_s11n_test_helper",
    testonly = True,
    srcs = ["py_object_s11n_test_helper.py"],
    deps = [":s11n"],
)

py_library(
    name = "registered_py_object_codecs",
    srcs = ["registered_py_object_codecs.py"],
    deps = [":s11n"],
)

py_library(
    name = "py_object_pickle_codec",
    srcs = ["py_object_pickle_codec.py"],
    deps = [
        ":registered_py_object_codecs",
        ":s11n",
    ],
)

py_library(
    name = "py_object_reference_codec",
    srcs = ["py_object_reference_codec.py"],
    deps = [
        ":registered_py_object_codecs",
        ":s11n",
    ],
)

cc_library(
    name = "py_object_codec",
    srcs = [
        "codec_name.h",
        "py_object_decoder.cc",
        "py_object_encoder.cc",
    ],
    hdrs = [
        "py_object_decoder.h",
        "py_object_encoder.h",
    ],
    deps = [
        ":py_object_codec_cc_proto",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs:registry",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
        "//py/arolla/types/qvalue:py_function_operator",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

proto_library(
    name = "py_object_codec_proto",
    srcs = ["py_object_codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "py_object_codec_cc_proto",
    deps = [":py_object_codec_proto"],
)

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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("@rules_python//python:proto.bzl", "py_proto_library")

# Serialization codec for PY_OBJECT type.

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

py_library(
    name = "py_object_codec",
    srcs = [
        "pickle_codec.py",
        "reference_codec.py",
        "registry.py",
        "tools.py",
    ],
    visibility = ["//py/arolla/s11n:__subpackages__"],
    deps = [
        "//py/arolla/abc",
    ],
)

py_test(
    name = "pickle_codec_test",
    srcs = ["pickle_codec_test.py"],
    deps = [
        ":py_object_codec",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "reference_codec_test",
    srcs = ["reference_codec_test.py"],
    deps = [
        ":py_object_codec",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "testing_codecs",
    srcs = ["testing_codecs.py"],
    deps = [
        ":py_object_codec",
        "//py/arolla/abc",
    ],
)

py_test(
    name = "tools_test",
    srcs = ["tools_test.py"],
    deps = [
        ":py_object_codec",
        ":testing_codecs",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

cc_library(
    name = "codec",
    srcs = ["codec.cc"],
    visibility = ["//py/arolla/s11n:__subpackages__"],
    deps = [
        ":codec_cc_proto",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs:registry",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//py/arolla/abc:py_abc",
        "//py/arolla/py_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
    alwayslink = True,
)

py_test(
    name = "codec_test",
    srcs = ["codec_test.py"],
    deps = [
        ":codec_py_proto",
        ":testing_codecs",
        "//arolla/serialization_codecs/generic:scalar_codec_py_proto",
        "//py/arolla",
        "//py/arolla/s11n/testing:codec_test_case",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

proto_library(
    name = "codec_proto",
    srcs = ["codec.proto"],
    deps = ["//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "codec_cc_proto",
    deps = [":codec_proto"],
)

py_proto_library(
    name = "codec_py_proto",
    testonly = True,
    deps = [":codec_proto"],
)

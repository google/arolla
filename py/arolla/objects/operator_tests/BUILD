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

load("@rules_python//python:defs.bzl", "py_test")

package(default_testonly = 1)

licenses(["notice"])

py_test(
    name = "objects_make_object_qtype_test",
    srcs = ["objects_make_object_qtype_test.py"],
    strict_deps = False,
    deps = [
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/objects",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "objects_make_object_test",
    srcs = ["objects_make_object_test.py"],
    strict_deps = False,
    deps = [
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/objects",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "objects_get_object_attr_test",
    srcs = ["objects_get_object_attr_test.py"],
    strict_deps = False,
    deps = [
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/objects",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "objects_get_object_attr_qtype_test",
    srcs = ["objects_get_object_attr_qtype_test.py"],
    strict_deps = False,
    deps = [
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/objects",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "objects_has_object_attr_test",
    srcs = ["objects_has_object_attr_test.py"],
    strict_deps = False,
    deps = [
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/objects",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

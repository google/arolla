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

# utilities for codegeneration

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@bazel_skylib//rules:diff_test.bzl", "diff_test")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")
load(
    "//arolla/codegen:utils.bzl",
    "EVAL_PYTHON_FUNCTION_CALL_DEPS",
    "call_python_function",
    "python_function_call_genrule",
    "read_file_function",
    "render_jinja2_template",
    "write_file_function",
)
load(":utils_test.bzl", "call_python_function_test_suite")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files([
    "eval_python_function_call.py",
    "test_template.jinja2",
])

py_library(
    name = "utils",
    srcs = ["utils.py"],
)

py_library(
    name = "eval_python_function_call_lib",
    srcs = ["eval_python_function_call.py"],
    # Deps are specified in the *.bzl file, so we can use them to avoid calling make_build_script if
    # no new deps are added.
    deps = EVAL_PYTHON_FUNCTION_CALL_DEPS,
)

py_binary(
    name = "eval_python_function_call",
    srcs = ["eval_python_function_call.py"],
    # Deps are specified in the *.bzl file, so we can use them to avoid calling make_build_script if
    # no new deps are added.
    deps = EVAL_PYTHON_FUNCTION_CALL_DEPS,
)

py_library(
    name = "render_jinja2_template",
    srcs = ["render_jinja2_template.py"],
    deps = [
        ":utils",
        "@jinja_archive//:jinja2",
    ],
)

py_test(
    name = "utils_test",
    srcs = ["utils_test.py"],
    deps = [
        ":utils",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

render_jinja2_template(
    name = "test_template_rendered",
    testonly = 1,
    out = "test_template_rendered.txt",
    context = dict(
        data_files = {
            "data_file_1": read_file_function("data_file_1.bin"),
            "data_file_2": read_file_function("data_file_2.bin"),
            "data_file_3": read_file_function("data_file_3.bin"),
        },
        list_of_dicts = [
            dict(
                inner_int = 2,
                inner_string = "second",
                unused_field = 0,
            ),
            dict(
                inner_int = 1543,
                inner_string = "string with {, }, \", ', $ and \\ inside",
            ),
        ],
        scalar_int = 57,
        scalar_string = "fifty seven",
        unused_field = 0,
        very = dict(deep = dict(nested = dict(field = ["it is hard to get there"]))),
    ),
    template = "test_template.jinja2",
)

diff_test(
    name = "test_template_golden_test",
    file1 = ":test_template.golden",
    file2 = ":test_template_rendered",
)

bzl_library(
    name = "utils_bzl",
    srcs = ["utils.bzl"],
    deps = [
        "@bazel_skylib//lib:structs",
    ],
)

call_python_function_test_suite(name = "call_python_function_bzl_test")

py_library(
    name = "test_functions",
    testonly = 1,
    srcs = ["test_functions.py"],
)

python_function_call_genrule(
    name = "python_function_call_test_hello",
    testonly = 1,
    outs = ["python_function_call_test_hello.txt"],
    function = call_python_function(
        "arolla.codegen.test_functions.write_to_file",
        args = [
            call_python_function(
                "arolla.codegen.test_functions.hello",
                args = ["world"],
                deps = [":test_functions"],
            ),
            "$(execpath python_function_call_test_hello.txt)",
        ],
        deps = [":test_functions"],
    ),
)

write_file(
    name = "python_function_call_test_hello_golden",
    testonly = 1,
    out = "python_function_call_test_hello.txt.golden",
    content = ["Hello, world!\n"],
)

diff_test(
    name = "python_function_call_test_hello_golden_test",
    file1 = ":python_function_call_test_hello.txt",
    file2 = ":python_function_call_test_hello.txt.golden",
)

python_function_call_genrule(
    name = "data_file_1_echo",
    outs = ["data_file_1.bin.echo"],
    function = write_file_function(
        "data_file_1.bin.echo",
        read_file_function("data_file_1.bin"),
    ),
)

diff_test(
    name = "data_file_1_echo_golden_test",
    file1 = ":data_file_1.bin",
    file2 = ":data_file_1_echo",
)

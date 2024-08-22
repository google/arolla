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

"""Build macros for generator operator tests with different evaluation backends."""

load("@rules_python//python:defs.bzl", "py_library", "py_test")

load("//arolla/codegen:utils.bzl", "bash_escape")

def arolla_operator_test_with_specified_eval_fn(
        name,
        test_libraries,
        eval_fn_deps,
        eval_fn_path,
        skip_test_patterns = None,
        **test_flags):
    r"""Creates a test for `test_libraries` using the `eval_fn_path` backend.

    Note that the module of containing the eval_fn specified by eval_fn_path is
    responsible for importing and registering all necessary modules required
    for the eval_fn to work.

    Args:
        name: The target name.
        test_libraries: Test bases to be run.
        eval_fn_deps: Dependencies required by `eval_fn_path`.
        eval_fn_path: Path to an expr evaluation function. E.g.
            "arolla.arolla.eval" which points to the default `arolla.eval` function.
        skip_test_patterns: List of test name patterns to skip. The patterns are matched with the
            beginning of the test ID in the form `<test_class>.<test_method>`. E.g.:
                skip_test_patterns = ['.*my_test_method', r'^TestFoo\.test_bar$']
            will:
            1. Skip all test methods of all test classes where the method name contains the
                substring 'my_test_method'.
            2. Skip the specific test method 'TestFoo.test_bar'.
        **test_flags: Additional test flags.
    """
    args = test_flags.pop("args", []) + ["--eval_fn=" + eval_fn_path]
    if skip_test_patterns:
        skip_test_args = [
            '--skip_test_patterns="{}"'.format(bash_escape(pattern))
            for pattern in skip_test_patterns
        ]
        args = args + skip_test_args

    py_test(
        name = name,
        srcs = test_libraries,
        deps = eval_fn_deps + test_libraries,
        args = args,
        **test_flags
    )

def arolla_operator_test(name, test_libraries, skip_test_patterns = None, **test_flags):
    r"""Creates a test for `test_libraries` using arolla.eval as backend.

    This macro is a convenience macro for:

        arolla_operator_test_with_specified_eval_fn(
            name = name,
            test_libraries = test_libraries,
            eval_fn_deps = ["//py/arolla"],
            eval_fn_path = "arolla.arolla.eval",
            **test_flags,
        )

    Args:
        name: The target name.
        test_libraries: Test bases to be run.
        skip_test_patterns: List of test name patterns to skip. The patterns are matched with the
            beginning of the test ID in the form `<test_class>.<test_method>`. E.g.:
                skip_test_patterns = ['.*my_test_method', r'^TestFoo\.test_bar$']
            will:
            1. Skip all test methods of all test classes where the method name contains the
                substring 'my_test_method'.
            2. Skip the specific test method 'TestFoo.test_bar'.
        **test_flags: Additional test flags.
    """
    arolla_operator_test_with_specified_eval_fn(
        name = name,
        test_libraries = test_libraries,
        eval_fn_deps = ["//py/arolla"],
        eval_fn_path = "arolla.arolla.eval",
        skip_test_patterns = skip_test_patterns,
        **test_flags
    )

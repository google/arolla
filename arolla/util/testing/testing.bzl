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

"""Common build rules for Arolla testing."""

load("//arolla/codegen:utils.bzl", "bash_escape_wo_make_vars")

def binary_smoke_test(
        name,
        binary,
        args,
        data = [],
        **kwargs):
    """Test that the binary runs on the given arguments without failing.

    Args:
        name: name of the test.
        binary: cc_test/cc_binary build target.
        args: arguments to pass to the binary.
        data: additional data needed for the test.
        **kwargs: other arguments to forward to the generated sh_test rule."""

    script_args = (["$(rootpath {})".format(binary)] +
                   [bash_escape_wo_make_vars(a) for a in args])

    runner = "//arolla/util/testing:run_smoke_test.sh"
    native.sh_test(
        name = name,
        srcs = [runner],
        args = script_args,
        data = data + [binary],
        **kwargs
    )

def benchmark_smoke_test(
        name,
        binary,
        benchmarks = "all",
        extra_args = None,
        **kwargs):
    """Test that benchmark runs without failing.

    Args:
        name: name of the test.
        binary: cc_test/cc_binary build target with the benchmark.
        benchmarks: benchmarks to run (default: all).
        extra_args: extra arguments to pass to the benchmark binary
        **kwargs: other arguments to forward to the generated sh_test rule."""

    args = [
        "--binary=$(rootpath {})".format(binary),
        "--benchmark_filter={}".format(benchmarks),
    ]

    if extra_args != None:
        for arg in extra_args:
            args.append("--extra_args={}".format(arg))

    binary_smoke_test(
        name = name,
        args = args,
        tags = kwargs.pop("tags", []) + ["nozapfhahn"],
        binary = "//arolla/util/testing:run_benchmark_smoke_test",
        data = [binary],
        **kwargs
    )

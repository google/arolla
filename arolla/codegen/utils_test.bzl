"""Tests for utility functions from utils.bzl."""

load("@bazel_skylib//lib:unittest.bzl", "asserts", "unittest")
load("//arolla/codegen:utils.bzl", "call_python_function")

def _call_python_function_test_impl(ctx):
    env = unittest.begin(ctx)
    factorial = call_python_function(
        "math.factorial",
        [2],
        deps = ["//my/dep", "//my/dep2"],
        data = ["//my/data", "//my/data2"],
    )
    asserts.equals(
        env,
        "math.factorial",
        factorial.fn,
    )
    asserts.equals(
        env,
        [2],
        factorial.args,
    )
    asserts.equals(
        env,
        ["//my/dep", "//my/dep2"],
        factorial.deps,
    )
    asserts.equals(
        env,
        ["//my/data", "//my/data2"],
        factorial.data,
    )
    power = call_python_function(
        "math.pow",
        [factorial, 2],
        deps = ["//pow/dep", "//my/dep2"],
        data = ["//pow/data", "//my/data"],
    )
    asserts.equals(
        env,
        "math.pow",
        power.fn,
    )
    stripped_factorial = struct(
        fn = "math.factorial",
        args = [2],
        __call_python_func__ = 1,
    )
    asserts.equals(
        env,
        [stripped_factorial, 2],
        power.args,
    )
    asserts.equals(
        env,
        ["//my/dep", "//my/dep2", "//pow/dep"],
        power.deps,
    )
    asserts.equals(
        env,
        ["//my/data", "//my/data2", "//pow/data"],
        power.data,
    )
    return unittest.end(env)

call_python_function_test = unittest.make(_call_python_function_test_impl)

def call_python_function_test_suite(name):
    unittest.suite(
        name,
        call_python_function_test,
    )

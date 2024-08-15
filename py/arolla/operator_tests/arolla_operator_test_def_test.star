"""Tests for arolla_operator_test_def.bzl."""

load(
    ":arolla_operator_test_def.bzl",
    "arolla_operator_test",
    "arolla_operator_test_with_specified_eval_fn",
)

arolla_operator_test_with_specified_eval_fn(
    name = "foo_bar_test",
    test_libraries = [":foo_test_base", ":bar_test_base"],
    eval_fn_deps = ["//py/arolla"],
    eval_fn_path = "arolla.arolla.eval",
)

arolla_operator_test_with_specified_eval_fn(
    name = "override_flags_test",
    test_libraries = [":foo_test_base", ":bar_test_base"],
    eval_fn_deps = [":important_deps", ":another_dep"],
    eval_fn_path = "tf.constant",
    python_version = "PY1",
    srcs_version = "PY2",
    some_other_test_flag = ["a", "b", "c"],
)

arolla_operator_test_with_specified_eval_fn(
    name = "with_skipping_test",
    test_libraries = [":foo_test_base", ":bar_test_base"],
    eval_fn_deps = ["//py/arolla"],
    eval_fn_path = "arolla.arolla.eval",
    skip_test_patterns = ["abc", r"foo\.bar", "foo$"],
)

arolla_operator_test_with_specified_eval_fn(
    name = "with_empty_skipping_test",
    test_libraries = [":foo_test_base", ":bar_test_base"],
    eval_fn_deps = ["//py/arolla"],
    eval_fn_path = "arolla.arolla.eval",
    skip_test_patterns = [],
)

arolla_operator_test(
    name = "foo_bar_test_2",
    test_libraries = [":foo_test_base", ":bar_test_base"],
)

arolla_operator_test(
    name = "override_flags_test_2",
    test_libraries = [":foo_test_base", ":bar_test_base"],
    python_version = "PY1",
    srcs_version = "PY2",
    some_other_test_flag = ["a", "b", "c"],
)

arolla_operator_test(
    name = "with_skipping_test_2",
    test_libraries = [":foo_test_base", ":bar_test_base"],
    skip_test_patterns = ["abc", r"foo\\.bar"],
)

expected_output = r"""
pytype_contrib_test(
    name = "foo_bar_test",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = ["--eval_fn=arolla.arolla.eval"],
    deps = [
        ":bar_test_base",
        ":foo_test_base",
        "//py/arolla",
    ],
)

pytype_contrib_test(
    name = "override_flags_test",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = ["--eval_fn=tf.constant"],
    python_version = "PY1",
    some_other_test_flag = [
        "a",
        "b",
        "c",
    ],
    srcs_version = "PY2",
    deps = [
        ":another_dep",
        ":bar_test_base",
        ":foo_test_base",
        ":important_deps",
    ],
)

pytype_contrib_test(
    name = "with_skipping_test",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = [
        "--eval_fn=arolla.arolla.eval",
        "--skip_test_patterns=\"abc\"",
        "--skip_test_patterns=\"foo\\\\.bar\"",
        "--skip_test_patterns=\"foo$$\"",
    ],
    deps = [
        ":bar_test_base",
        ":foo_test_base",
        "//py/arolla",
    ],
)

pytype_contrib_test(
    name = "with_empty_skipping_test",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = ["--eval_fn=arolla.arolla.eval"],
    deps = [
        ":bar_test_base",
        ":foo_test_base",
        "//py/arolla",
    ],
)

pytype_contrib_test(
    name = "foo_bar_test_2",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = ["--eval_fn=arolla.arolla.eval"],
    deps = [
        ":bar_test_base",
        ":foo_test_base",
        "//py/arolla",
    ],
)

pytype_contrib_test(
    name = "override_flags_test_2",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = ["--eval_fn=arolla.arolla.eval"],
    python_version = "PY1",
    some_other_test_flag = [
        "a",
        "b",
        "c",
    ],
    srcs_version = "PY2",
    deps = [
        ":bar_test_base",
        ":foo_test_base",
        "//py/arolla",
    ],
)

pytype_contrib_test(
    name = "with_skipping_test_2",
    srcs = [
        ":bar_test_base",
        ":foo_test_base",
    ],
    args = [
        "--eval_fn=arolla.arolla.eval",
        "--skip_test_patterns=\"abc\"",
        "--skip_test_patterns=\"foo\\\\\\\\.bar\"",
    ],
    deps = [
        ":bar_test_base",
        ":foo_test_base",
        "//py/arolla",
    ],
)
"""

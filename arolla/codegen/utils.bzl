"""Utility functions for creating codegeneration build rules."""

load("@rules_python//python:py_binary.bzl", "py_binary")
load("@bazel_skylib//lib:structs.bzl", "structs")

def bash_escape(s):
    """Escape string for passing into the genrule as bash argument."""
    return s.replace("\\", "\\\\").replace(
        '"',
        '\\"',
    ).replace("`", "\\`").replace(
        "$",
        "$$",
    )

def bash_escape_wo_make_vars(s):
    """Like bash_escape, but keep "make" variables unescaped."""
    return bash_escape(
        s,
    ).replace(
        "$$(location",
        "$(location",
    ).replace(
        "$$(execpath",
        "$(execpath",
    ).replace(
        "$$(rootpath",
        "$(rootpath",
    ).replace(
        "$$@",
        "$@",
    ).replace("$$<", "$<")

def make_build_script(
        name,
        script,
        deps,
        data = [],
        visibility = None,
        testonly = 0,
        local = False,
        tags = []):
    """Simply makes a copy and rename the given script.

    When a tool needs access to a given library, we can usually list that
    dependency in the build file. However, if the tool has run-time dependencies,
    such as when using reflection, we need a way to make those dependencies
    available to the tool at build time.

    To achieve this, this rule copies the tool .py source file and create a
    dynamic py_binary with the dependencies that will be required at run-time.

    For example, protopath uses reflection to list fields inside a protocol
    buffer. The protopath tool itself doesn't have any proto dependencies, but at
    runtime, it will, depending on the protopath definition. We can use this rule
    to dynamically generate a new tool, based on protopath, but with the added
    dependency on the protocol buffer libraries that will be needed during
    execution.

    Args:
      name: A name for the final binary. This name will be modified, and the new
          name returned.
      script: the python binary script (the main)
      deps: dependencies for that script.
      data: data files required for that script.
      visibility: visibility for this script.
      testonly: used for generating test scripts.
      local: forwarded to genrule. If True "local" is also added to `tags`.
      tags: forwarded to genrule and generated py_binary.

    Returns:
      the new name for the generated py_binary rule.
    """
    if visibility == None:
        kwargs = {}
    else:
        kwargs = {"visibility": visibility}
    gen_cpp_genrule_name = "_%s_internal_gen_script" % name
    py_bin_name = "_%s_internal_py_lib" % name
    tags = list(tags)
    if local and "local" not in tags:
        tags.append("local")

    native.genrule(
        name = gen_cpp_genrule_name,
        testonly = testonly,
        srcs = [script],
        outs = ["%s.py" % py_bin_name],
        cmd = "cp $(SRCS) $@",
        visibility = ["//visibility:private"],
        local = local,
        tags = tags,
    )
    py_binary(
        name = py_bin_name,
        testonly = testonly,
        srcs = ["%s.py" % py_bin_name],
        deps = deps,
        data = data,
        # skip par files with `build ...:*` patterns
        tags = tags,
        **kwargs
    )

    return py_bin_name

def _maybe_strip_deps_and_data(fn):
    if not is_python_function_call(fn):
        return fn
    dct = dict(
        fn = fn.fn,
        __call_python_func__ = 1,
    )
    if fn.args:
        dct["args"] = fn.args
    if fn.kwargs:
        dct["kwargs"] = fn.kwargs
    return struct(**dct)

def call_python_function(fn, args, deps, kwargs = {}, data = []):
    """Creates a Python function spec to be called in build time.

    Args:
        fn: fully quallified Python function name.
        args: list of arguments to pass to the function
        deps: tool dependencies that need to be added to the script
        kwargs: dict with kwargs to pass to the function
        data: tool data dependencies that need to be added to the script
    Returns:
      Specially marked struct with `fn`, `args`, `deps` and `data` fields.
      `deps` and `data` also contain information from `args` created by
      `call_python_function`.
    """
    deps = list(deps)
    data = list(data)
    for arg in args + list(kwargs.values()):
        arg_deps, arg_data = _extract_deps_and_data(arg)
        deps += arg_deps
        data += arg_data
    return struct(
        fn = fn,
        args = [_maybe_strip_deps_and_data(arg) for arg in args],
        kwargs = kwargs,
        deps = sorted(depset(deps).to_list()),
        data = sorted(depset(data).to_list()),
        __call_python_func__ = 1,
    )

def is_python_function_call(x):
    """Returns True if x is created by call_python_function macro."""
    return hasattr(x, "__call_python_func__")

def encode_call_python_function(fn):
    """Returns CLI representation of the function and stripping unnecessary info."""
    return bash_escape_wo_make_vars(json.encode(_maybe_strip_deps_and_data(fn)))

# Deps of //arolla/codegen:eval_python_function_call. We can avoid calling
# make_build_script if no new deps are added.
EVAL_PYTHON_FUNCTION_CALL_DEPS = [
    "//arolla/codegen:render_jinja2_template",
    "//arolla/codegen:utils",
    "@com_google_absl_py//absl:app",
    "@com_google_absl_py//absl/flags",
]

def python_function_call_genrule(
        name,
        function,
        outs,
        **kwargs):
    """Executes provided python function that should generate `outs` files.

    Args:
      name: rule name
      function: `call_python_function` struct specification.
        Function must generate files specified in `outs`.
      outs: resulting file names that function must produce.
      **kwargs: Extra arguments passed to all generated rules.
    """
    if not is_python_function_call(function):
        fail("function must be call_python_function")
    script_kwargs = dict(kwargs)
    script_kwargs.pop("visibility", None)
    script_kwargs.pop("tags", None)
    script_kwargs.pop("stamp", None)

    new_deps = [d for d in function.deps if d not in EVAL_PYTHON_FUNCTION_CALL_DEPS]
    if new_deps:
        generator = make_build_script(
            name = name,
            visibility = ["//visibility:private"],
            script = "//arolla/codegen:eval_python_function_call.py",
            deps = depset(function.deps + ["//arolla/codegen:eval_python_function_call_lib"]),
            **script_kwargs
        )
    else:
        generator = "//arolla/codegen:eval_python_function_call"

    native.genrule(
        name = name,
        outs = outs,
        cmd = ("$(execpath {generator}) " +
               '--arolla_call_python_function_spec="{spec}"').format(
            generator = generator,
            spec = encode_call_python_function(function),
        ),
        tools = [generator] + function.data,
        **kwargs
    )

def merge_dicts(*args):
    """Merges multiple dicts into a single dict.

    If two dicts have a key in common, the one at the later position wins.

    Args:
      *args: call_python_function that returns a dict

    Returns:
      call_py_function that returns a merged dict.
    """
    deps = ["//arolla/codegen:utils"]
    data = []
    for a in args:
        deps += a.deps
        data += a.data
    return call_python_function(
        "arolla.codegen.utils.merge_dicts",
        args = list(args),
        deps = deps,
        data = data,
    )

def read_file_function(filename):
    """Returns call_python_function that reads the given filename in build time."""
    return call_python_function(
        "arolla.codegen.utils.read_file",
        ["$(execpath {})".format(filename)],
        deps = ["//arolla/codegen:utils"],
        data = [filename],
    )

def write_file_function(filename, content):
    """Returns call_python_function that writes bytes to the given filename in build time."""
    return call_python_function(
        "arolla.codegen.utils.write_file",
        ["$(execpath {})".format(filename), content],
        deps = ["//arolla/codegen:utils"],
    )

def _extract_deps_and_data(context):
    """Traverses context recursively, extracting deps and data from call_python_function objects."""
    deps = []
    data = []
    stack = [context]

    # Starlark prohibits while loops and recursion, so here is a nasty hack to bypass it.
    finished = False
    for _ in range(1000000):
        if not stack:
            finished = True
            break
        value = stack.pop()

        if is_python_function_call(value):
            deps += value.deps
            data += value.data
            # we do not need to go recursively, because arg already contains
            # all its dependencies.

        elif type(value) == type([]):
            stack += value
        elif type(value) == type(struct()):
            stack += structs.to_dict(value).values()
        elif type(value) == type(dict()):
            stack += value.values()

    if not finished:
        fail("too big context for render_jinja2_template")

    return (sorted(depset(deps).to_list()), sorted(depset(data).to_list()))

def render_jinja2_template(name, template, out, context, srcs = [], **kwargs):
    """Renders jinja2 template using the provided context.

    Args:
      name: rule name
      template: path / rule name for jinja2 template file
      out: resulting file name
      context: jinja2 context to render the template. It should be a dict or
        struct that can contain other dicts/structs, lists, strings, integers,
        floats and booleans.
        The context can also contain call_python_function() macros that will
        be evaluated and replaced with the evaluation results before rendering.
      srcs: extra sources (e.g. jinja2 libraries imported by the template).
      **kwargs: additional kwargs to pass to the generated build rule.
    """
    deps, data = _extract_deps_and_data(context)
    python_function_call_genrule(
        name = name,
        function = call_python_function(
            "arolla.codegen.render_jinja2_template.render_jinja2_template",
            args = [
                "$(execpath {})".format(template),
                context,
                "$(execpath {})".format(out),
            ],
            deps = ["//arolla/codegen:render_jinja2_template"] + deps,
            data = [template] + data + srcs,
        ),
        outs = [out],
        **kwargs
    )

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

"""Generation of evaluator for the expression."""

load(
    "//arolla/codegen:utils.bzl",
    "bash_escape_wo_make_vars",
    "call_python_function",
    "is_python_function_call",
    "make_build_script",
    "python_function_call_genrule",
    "render_jinja2_template",
)
load("//arolla/codegen/io:io.bzl", "input_loader_set", "slot_listener_set")

def _make_compiled_expr_build_script(name, tool_deps, models, **kwargs):
    tool_deps = list(tool_deps)
    for model in models:
        if not is_python_function_call(model):
            fail("model must be `call_python_function`")
        tool_deps += model.deps
        tool_deps = sorted({d: 1 for d in tool_deps}.keys())
    tool_deps = sorted({d: 1 for d in tool_deps}.keys())

    return make_build_script(
        name = name,
        script = "//py/arolla/codegen:compiled_expr_main.py",
        deps = [
            "//py/arolla/codegen:clib",
            "//py/arolla/codegen:compiled_expr_main",
        ] + tool_deps,
        **kwargs
    )

def compiled_exprs(
        name,
        models,
        tool_deps = [],
        tool_data = [],
        tool_flags = {},
        export_named_outputs = False,
        deps = [],
        **kwargs):
    """C++ library providing set of CompiledExpr with name and header file `{name}.h`

    Library contains `len(models)` zero argument functions with `models.keys()` names
    returning `const ::arolla::InplaceCompiledExpr&`.

    ":name.io.textproto" contains ModelInputOutputInfo as a textproto.

    Args:
      name: name of the target and basename of the header and cc file
      models: dictionary from `cpp_function_name` to python function specification
        via `call_python_function`.
        `cpp_function_name` (dictionary key) is fully qualified name of the generated
        CompiledExpr getter function. E.g. ::contrib::x::GetMyCompiledExpr
        `call_python_function` (dictionary value) must specialize a function returning
        `arolla.Expr` with the model.
      tool_deps: list of targets required for the generation tool to run.
          Can contain not included by default dynamic qexpr operators libraries or
          custom optimizations.
          NOTE for Arolla developer: consider updating :clib if an Arolla core operator is missing.
      tool_data: The list of files needed by the tools at runtime.
      tool_flags: (dict) additional flags to pass to the tool
              in the form of {flag_name: flag_value}.
      export_named_outputs: (bool) whenever to generate code for exporting named
          outputs for nodes annotated with `annotation.export`.
      deps: list of the operators and other dependencies to the final cc_library.
      **kwargs: other arguments passed directly to the cc_library.
          testonly=1 is also applied to the all service targets.
          local=True is applied for genrule only
    """
    for model in models.values():
        if not is_python_function_call(model):
            fail("model must be `call_python_function`")
    testonly = kwargs.get("testonly", 0)
    local = kwargs.pop("local", False)
    tags = list(kwargs.pop("tags", []))
    deps = list(deps)

    tool_data = list(tool_data)

    deps += [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/memory",
        "//arolla/qexpr",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
    ]
    deps = sorted({d: 1 for d in deps}.keys())
    for model in models.values():
        tool_data += model.data

    build_target = "//{}:{}".format(native.package_name(), name)

    h_file = name + ".h"
    cc_file = name + ".cc"
    deps_file = name + ".deps"
    io_file = name + ".io.textproto"
    gen_rule = "_" + name + "_gen_cc_and_h"

    compiled_expr_binary = _make_compiled_expr_build_script(
        name = name + "_compiled_expr_binary",
        tool_deps = tool_deps,
        models = models.values(),
        testonly = testonly,
        local = local,
        tags = tags,
    )

    cpp_function_names = " ".join(["--cpp_function_name=" + name for name in models])
    expr_fns = " ".join(
        [
            '--expr_fn="' + bash_escape_wo_make_vars(json.encode(model)) + '"'
            for model in models.values()
        ],
    )

    native.genrule(
        name = gen_rule,
        srcs = depset(tool_data),
        outs = [h_file, cc_file, deps_file, io_file],
        cmd = ("$(execpath :" + compiled_expr_binary + ") " +
               "--output_dir=$(GENDIR)/{package_name} " +
               "{cpp_function_names} " +
               "--build_target={build_target} " +
               "--h_out_file={h_out_file} --cc_out_file={cc_out_file} " +
               "--deps_out_file={deps_out_file} --io_out_file={io_file} " +
               "{expr_fns} " +
               "--export_named_outputs={export_named_outputs} " +
               "{tool_flags}").format(
            package_name = native.package_name(),
            cpp_function_names = cpp_function_names,
            build_target = build_target,
            h_out_file = h_file,
            cc_out_file = cc_file,
            deps_out_file = deps_file,
            io_file = io_file,
            expr_fns = expr_fns,
            export_named_outputs = export_named_outputs,
            tool_flags = bash_escape_wo_make_vars(
                " ".join([
                    "--{}={}".format(k, tool_flags[k])
                    for k in sorted(tool_flags)
                ]),
            ),
        ),
        testonly = testonly,
        local = local,
        tools = [compiled_expr_binary],
        tags = tags,
    )

    native.cc_library(
        name = name,
        srcs = [cc_file],
        hdrs = [h_file],
        deps = deps,
        tags = tags,
        **kwargs
    )

def arolla_codegen_functions(
        name,
        functions,
        tool_deps = [],
        tool_flags = {},
        deps = [],
        **kwargs):
    """Generate C++ functions that evaluates the provided expressions end-to-end.

    NOTE: The rule is not supposed to be used by end users. Use higher-level wrappers instead.

    Args:
      name: rule name.
      functions: a dict from the model name to a call_python_function specification. The function
        must return arolla.codegen.codegen_function.CodegenFunctionSpec.
      tool_deps: additional deps to add to both io and compiled expr generators.
      tool_flags: additional flags to pass to compiled expr generator.
      deps: deps for the generated C++ library.
      **kwargs: other args to pass to the cc_library target. testonly and tags will be also
        propagated to other generated targets.
    """
    testonly = kwargs.pop("testonly", 0)
    tags = list(kwargs.pop("tags", []))
    tool_flags = dict(tool_flags)
    tool_flags["use_iwyu_private"] = True
    tool_deps = tool_deps + ["//py/arolla/codegen:codegen_function"]

    cc_internal_namespace = "{}_{}".format(native.package_name().replace("/", "_").replace("-", "_"), name)

    input_loader_lib = "_{}_input_loaders".format(name)
    input_loader_hdr = "{}/{}.h".format(native.package_name(), input_loader_lib)
    input_loader_set(
        name = input_loader_lib,
        loaders_spec = call_python_function(
            "arolla.codegen.codegen_function.generate_input_loaders_spec",
            args = [cc_internal_namespace, functions],
            deps = tool_deps,
        ),
        deps = deps + [
            # These deps are not included automatically because the proto accessors come from
            # the loaders_spec and not known at the time of building the dependency graph.
            "//arolla/codegen/io:multi_loader",
            "//arolla/dense_array",
        ],
        visibility = ["//visibility:private"],
        tags = tags + ["manual"],
        testonly = testonly,
    )
    slot_listener_lib = "_{}_slot_listeners".format(name)
    slot_listener_hdr = "{}/{}.h".format(native.package_name(), slot_listener_lib)
    slot_listener_set(
        name = slot_listener_lib,
        listeners_spec = call_python_function(
            "arolla.codegen.codegen_function.generate_slot_listeners_spec",
            args = [cc_internal_namespace, functions],
            deps = tool_deps,
        ),
        deps = deps + [
            # These deps are not included automatically because the proto accessors come from
            # the listeners_spec and not known at the time of building the dependency graph.
            "//arolla/codegen/io:multi_loader",
            "//arolla/dense_array",
        ],
        visibility = ["//visibility:private"],
        tags = tags + ["manual"],
        testonly = testonly,
    )

    codegen_lib = "_{}_codegen".format(name)
    codegen_hdr = "{}/{}.h".format(native.package_name(), codegen_lib)
    compiled_exprs(
        name = codegen_lib,
        export_named_outputs = True,
        models = {
            # NOTE: The name must be consistent with the usage in codegen_function.cc.jinja2.
            "::{}::{}_InternalCompiledExpr".format(cc_internal_namespace, model_name): call_python_function(
                "arolla.codegen.codegen_function.get_expr",
                args = [functions, model_name],
                deps = tool_deps,
            )
            for model_name in functions
        },
        tool_flags = tool_flags,
        deps = deps,
        visibility = ["//visibility:private"],
        tags = tags + ["manual"],
        testonly = testonly,
    )

    context = dict(
        header_guard = (native.package_name().replace("/", "_") + "_" + name + "_h_").upper(),
        build_target = "//{}:{}".format(native.package_name(), name),
        full_header_name = "{}/{}.h".format(native.package_name(), name),
        cc_internal_namespace = cc_internal_namespace,
        extra_hdrs = call_python_function(
            "arolla.codegen.codegen_function.get_hdrs",
            args = [functions, [input_loader_hdr, slot_listener_hdr, codegen_hdr]],
            deps = tool_deps,
        ),
        specs = functions,
    )

    generated_h_rule_name = "_{}_function_h".format(name)
    render_jinja2_template(
        name = generated_h_rule_name,
        template = "//py/arolla/codegen:codegen_function.h.jinja2",
        out = name + ".h",
        context = context,
        visibility = ["//visibility:private"],
        tags = tags + ["manual"],
        testonly = testonly,
    )
    generated_cc_rule_name = "_{}_function_cc".format(name)
    render_jinja2_template(
        name = generated_cc_rule_name,
        template = "//py/arolla/codegen:codegen_function.cc.jinja2",
        out = name + ".cc",
        context = context,
        visibility = ["//visibility:private"],
        tags = tags + ["manual"],
        testonly = testonly,
    )

    native.cc_library(
        name = name,
        hdrs = [name + ".h"],
        srcs = [name + ".cc"],
        deps = [
            ":" + input_loader_lib,
            ":" + slot_listener_lib,
            ":" + codegen_lib,
            "@com_google_absl//absl/status",
            "//arolla/io",
            "//arolla/io/proto_types",
            "//arolla/serving",
        ] + deps,
        tags = tags,
        testonly = testonly,
        **kwargs
    )

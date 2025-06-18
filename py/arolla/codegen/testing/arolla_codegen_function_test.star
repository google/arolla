load("//arolla/codegen:utils.bzl", "call_python_function")
load("//py/arolla/codegen:expr.bzl", "arolla_codegen_functions", "compiled_exprs")

arolla_codegen_functions(
    name = "codegen_function",
    testonly = 1,
    functions = {
        "first": call_python_function(
            "arolla.codegen.testing.codegen_function_models.first_function_spec",
            args = [],
            deps = [":codegen_function_models"],
        ),
        "second": call_python_function(
            "arolla.codegen.testing.codegen_function_models.second_function_spec",
            args = [],
            deps = [":codegen_function_models"],
        ),
    },
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
        ":codegen_function_cc_proto",
    ],
)

expected_output = r"""
input_loader_set(
    name = "_codegen_function_input_loaders",
    testonly = 1,
    loaders_spec = struct(
        __call_python_func__ = 1,
        args = [
            "fake_testing_package_codegen_function",
            {
                "first": struct(
                    __call_python_func__ = 1,
                    args = [],
                    data = [],
                    fn = "arolla.codegen.testing.codegen_function_models.first_function_spec",
                    kwargs = {},
                    deps = [":codegen_function_models"],
                ),
                "second": struct(
                    __call_python_func__ = 1,
                    args = [],
                    data = [],
                    fn = "arolla.codegen.testing.codegen_function_models.second_function_spec",
                    kwargs = {},
                    deps = [":codegen_function_models"],
                ),
            },
        ],
        data = [],
        fn = "arolla.codegen.codegen_function.generate_input_loaders_spec",
        kwargs = {},
        deps = [
            ":codegen_function_models",
            "//py/arolla/codegen:codegen_function",
        ],
    ),
    tags = ["manual"],
    visibility = ["//visibility:private"],
    deps = [
        ":codegen_function_cc_proto",
        "//arolla/codegen/io:multi_loader",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

slot_listener_set(
    name = "_codegen_function_slot_listeners",
    testonly = 1,
    listeners_spec = struct(
        __call_python_func__ = 1,
        args = [
            "fake_testing_package_codegen_function",
            {
                "first": struct(
                    __call_python_func__ = 1,
                    args = [],
                    data = [],
                    fn = "arolla.codegen.testing.codegen_function_models.first_function_spec",
                    kwargs = {},
                    deps = [":codegen_function_models"],
                ),
                "second": struct(
                    __call_python_func__ = 1,
                    args = [],
                    data = [],
                    fn = "arolla.codegen.testing.codegen_function_models.second_function_spec",
                    kwargs = {},
                    deps = [":codegen_function_models"],
                ),
            },
        ],
        data = [],
        fn = "arolla.codegen.codegen_function.generate_slot_listeners_spec",
        kwargs = {},
        deps = [
            ":codegen_function_models",
            "//py/arolla/codegen:codegen_function",
        ],
    ),
    tags = ["manual"],
    visibility = ["//visibility:private"],
    deps = [
        ":codegen_function_cc_proto",
        "//arolla/codegen/io:multi_loader",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
    ],
)

genrule(
    name = "__codegen_function_codegen_compiled_expr_binary_internal_gen_script",
    testonly = 1,
    srcs = ["//py/arolla/codegen:compiled_expr_main.py"],
    outs = ["__codegen_function_codegen_compiled_expr_binary_internal_py_lib.py"],
    cmd = "cp $(SRCS) $@",
    local = False,
    tags = ["manual"],
    visibility = ["//visibility:private"],
)

py_binary(
    name = "__codegen_function_codegen_compiled_expr_binary_internal_py_lib",
    testonly = 1,
    srcs = ["__codegen_function_codegen_compiled_expr_binary_internal_py_lib.py"],
    buildpar = False,
    data = [],
    tags = ["manual"],
    use_precompile = False,
    deps = [
        ":codegen_function_models",
        "//py/arolla/codegen:clib",
        "//py/arolla/codegen:codegen_function",
        "//py/arolla/codegen:compiled_expr_main",
    ],
)

genrule(
    name = "__codegen_function_codegen_gen_cc_and_h",
    testonly = 1,
    srcs = depset([]),
    outs = [
        "_codegen_function_codegen.h",
        "_codegen_function_codegen.deps",
        "_codegen_function_codegen.io.textproto",
        "_codegen_function_codegen.cc",
    ],
    cmd = "$(execpath :__codegen_function_codegen_compiled_expr_binary_internal_py_lib) --output_dir=$(GENDIR)/fake/testing/package --cpp_function_name=::fake_testing_package_codegen_function::first_InternalCompiledExpr --cpp_function_name=::fake_testing_package_codegen_function::second_InternalCompiledExpr --build_target=//fake/testing/package:_codegen_function_codegen --h_out_file=_codegen_function_codegen.h --cc_out_file=_codegen_function_codegen.cc --deps_out_file=_codegen_function_codegen.deps --io_out_file=_codegen_function_codegen.io.textproto --expr_fn=\"{\\\"__call_python_func__\\\":1,\\\"args\\\":[{\\\"first\\\":{\\\"__call_python_func__\\\":1,\\\"args\\\":[],\\\"data\\\":[],\\\"deps\\\":[\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.testing.codegen_function_models.first_function_spec\\\",\\\"kwargs\\\":{}},\\\"second\\\":{\\\"__call_python_func__\\\":1,\\\"args\\\":[],\\\"data\\\":[],\\\"deps\\\":[\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.testing.codegen_function_models.second_function_spec\\\",\\\"kwargs\\\":{}}},\\\"first\\\"],\\\"data\\\":[],\\\"deps\\\":[\\\"//py/arolla/codegen:codegen_function\\\",\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.codegen_function.get_expr\\\",\\\"kwargs\\\":{}}\" --expr_fn=\"{\\\"__call_python_func__\\\":1,\\\"args\\\":[{\\\"first\\\":{\\\"__call_python_func__\\\":1,\\\"args\\\":[],\\\"data\\\":[],\\\"deps\\\":[\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.testing.codegen_function_models.first_function_spec\\\",\\\"kwargs\\\":{}},\\\"second\\\":{\\\"__call_python_func__\\\":1,\\\"args\\\":[],\\\"data\\\":[],\\\"deps\\\":[\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.testing.codegen_function_models.second_function_spec\\\",\\\"kwargs\\\":{}}},\\\"second\\\"],\\\"data\\\":[],\\\"deps\\\":[\\\"//py/arolla/codegen:codegen_function\\\",\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.codegen_function.get_expr\\\",\\\"kwargs\\\":{}}\" --export_named_outputs=True --shard_count=1 --use_iwyu_private=True",
    local = False,
    tags = ["manual"],
    tools = ["__codegen_function_codegen_compiled_expr_binary_internal_py_lib"],
)

cc_library(
    name = "_codegen_function_codegen",
    testonly = 1,
    srcs = ["_codegen_function_codegen.cc"],
    hdrs = ["_codegen_function_codegen.h"],
    tags = ["manual"],
    visibility = ["//visibility:private"],
    deps = [
        ":codegen_function_cc_proto",
        "//third_party/absl/base:core_headers",
        "//third_party/absl/base:no_destructor",
        "//third_party/absl/container:flat_hash_map",
        "//third_party/absl/memory",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

render_jinja2_template(
    name = "_codegen_function_function_h",
    testonly = 1,
    out = "codegen_function.h",
    context = {
        "header_guard": "FAKE_TESTING_PACKAGE_CODEGEN_FUNCTION_H_",
        "build_target": "//fake/testing/package:codegen_function",
        "full_header_name": "fake/testing/package/codegen_function.h",
        "cc_internal_namespace": "fake_testing_package_codegen_function",
        "extra_hdrs": struct(
            __call_python_func__ = 1,
            args = [
                {
                    "first": struct(
                        __call_python_func__ = 1,
                        args = [],
                        data = [],
                        fn = "arolla.codegen.testing.codegen_function_models.first_function_spec",
                        kwargs = {},
                        deps = [":codegen_function_models"],
                    ),
                    "second": struct(
                        __call_python_func__ = 1,
                        args = [],
                        data = [],
                        fn = "arolla.codegen.testing.codegen_function_models.second_function_spec",
                        kwargs = {},
                        deps = [":codegen_function_models"],
                    ),
                },
                [
                    "fake/testing/package/_codegen_function_input_loaders.h",
                    "fake/testing/package/_codegen_function_slot_listeners.h",
                    "fake/testing/package/_codegen_function_codegen.h",
                ],
            ],
            data = [],
            fn = "arolla.codegen.codegen_function.get_hdrs",
            kwargs = {},
            deps = [
                ":codegen_function_models",
                "//py/arolla/codegen:codegen_function",
            ],
        ),
        "specs": {
            "first": struct(
                __call_python_func__ = 1,
                args = [],
                data = [],
                fn = "arolla.codegen.testing.codegen_function_models.first_function_spec",
                kwargs = {},
                deps = [":codegen_function_models"],
            ),
            "second": struct(
                __call_python_func__ = 1,
                args = [],
                data = [],
                fn = "arolla.codegen.testing.codegen_function_models.second_function_spec",
                kwargs = {},
                deps = [":codegen_function_models"],
            ),
        },
    },
    tags = ["manual"],
    template = "//py/arolla/codegen:codegen_function.h.jinja2",
    visibility = ["//visibility:private"],
)

render_jinja2_template(
    name = "_codegen_function_function_cc",
    testonly = 1,
    out = "codegen_function.cc",
    context = {
        "header_guard": "FAKE_TESTING_PACKAGE_CODEGEN_FUNCTION_H_",
        "build_target": "//fake/testing/package:codegen_function",
        "full_header_name": "fake/testing/package/codegen_function.h",
        "cc_internal_namespace": "fake_testing_package_codegen_function",
        "extra_hdrs": struct(
            __call_python_func__ = 1,
            args = [
                {
                    "first": struct(
                        __call_python_func__ = 1,
                        args = [],
                        data = [],
                        fn = "arolla.codegen.testing.codegen_function_models.first_function_spec",
                        kwargs = {},
                        deps = [":codegen_function_models"],
                    ),
                    "second": struct(
                        __call_python_func__ = 1,
                        args = [],
                        data = [],
                        fn = "arolla.codegen.testing.codegen_function_models.second_function_spec",
                        kwargs = {},
                        deps = [":codegen_function_models"],
                    ),
                },
                [
                    "fake/testing/package/_codegen_function_input_loaders.h",
                    "fake/testing/package/_codegen_function_slot_listeners.h",
                    "fake/testing/package/_codegen_function_codegen.h",
                ],
            ],
            data = [],
            fn = "arolla.codegen.codegen_function.get_hdrs",
            kwargs = {},
            deps = [
                ":codegen_function_models",
                "//py/arolla/codegen:codegen_function",
            ],
        ),
        "specs": {
            "first": struct(
                __call_python_func__ = 1,
                args = [],
                data = [],
                fn = "arolla.codegen.testing.codegen_function_models.first_function_spec",
                kwargs = {},
                deps = [":codegen_function_models"],
            ),
            "second": struct(
                __call_python_func__ = 1,
                args = [],
                data = [],
                fn = "arolla.codegen.testing.codegen_function_models.second_function_spec",
                kwargs = {},
                deps = [":codegen_function_models"],
            ),
        },
    },
    tags = ["manual"],
    template = "//py/arolla/codegen:codegen_function.cc.jinja2",
    visibility = ["//visibility:private"],
)

cc_library(
    name = "codegen_function",
    testonly = 1,
    srcs = ["codegen_function.cc"],
    hdrs = ["codegen_function.h"],
    tags = [],
    deps = [
        ":_codegen_function_codegen",
        ":_codegen_function_input_loaders",
        ":_codegen_function_slot_listeners",
        ":codegen_function_cc_proto",
        "//third_party/absl/status",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/proto_types",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/aggregation:lib",
        "//arolla/qexpr/operators/core:lib",
        "//arolla/qexpr/operators/dense_array:lib",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/serving",
    ],
)

genrule(
    name = "__codegen_function_deps_internal_gen_script",
    testonly = 1,
    srcs = ["//arolla/codegen:eval_python_function_call.py"],
    outs = ["__codegen_function_deps_internal_py_lib.py"],
    cmd = "cp $(SRCS) $@",
    local = False,
    tags = [],
    visibility = ["//visibility:private"],
)

py_binary(
    name = "__codegen_function_deps_internal_py_lib",
    testonly = 1,
    srcs = ["__codegen_function_deps_internal_py_lib.py"],
    buildpar = False,
    data = [],
    tags = [],
    use_precompile = False,
    visibility = ["//visibility:private"],
    deps = depset([
        "//py/arolla/codegen:codegen_function",
        ":codegen_function_models",
        "//arolla/codegen:eval_python_function_call_lib",
    ]),
)

genrule(
    name = "_codegen_function_deps",
    testonly = 1,
    outs = ["codegen_function.deps"],
    cmd = "$(execpath __codegen_function_deps_internal_py_lib) --arolla_call_python_function_spec=\"{\\\"__call_python_func__\\\":1,\\\"args\\\":[{\\\"first\\\":{\\\"__call_python_func__\\\":1,\\\"args\\\":[],\\\"data\\\":[],\\\"deps\\\":[\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.testing.codegen_function_models.first_function_spec\\\",\\\"kwargs\\\":{}},\\\"second\\\":{\\\"__call_python_func__\\\":1,\\\"args\\\":[],\\\"data\\\":[],\\\"deps\\\":[\\\":codegen_function_models\\\"],\\\"fn\\\":\\\"arolla.codegen.testing.codegen_function_models.second_function_spec\\\",\\\"kwargs\\\":{}}},\\\"$(execpath :_codegen_function_codegen.deps)\\\",\\\"$(execpath codegen_function.deps)\\\"],\\\"fn\\\":\\\"arolla.codegen.codegen_function.generate_deps_file\\\"}\"",
    tags = ["manual"],
    tools = [
        "__codegen_function_deps_internal_py_lib",
        ":_codegen_function_codegen.deps",
    ],
)
"""

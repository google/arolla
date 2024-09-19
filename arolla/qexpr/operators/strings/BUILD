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

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "lift_to_optional",
    "make_optional_type",
    "operator_family",
    "operator_libraries",
    "operator_overload",
    "operator_overload_list",
    "scalar_types",
    "text_type",
    "unary_args",
    "with_lifted_by",
)
load(
    "//arolla/qexpr/operators/array:array.bzl",
    "lift_to_array",
    "make_array_type",
)
load(
    "//arolla/qexpr/operators/dense_array:lifter.bzl",
    "lift_to_dense_array",
    "make_dense_array_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

lifted_operator_lib_list = [
    ":operator_as_text",
    ":operator_bytes_length",
    ":operator_decode",
    ":operator_encode",
    ":operator_lower",
    ":operator_text_length",
    ":operator_upper",
]

operator_lib_list = lifted_operator_lib_list + [
    ":operator_contains",
    ":operator_contains_regex",
    ":operator_count",
    ":operator_extract_regex",
    ":operator_find",
    ":operator_parse_float32",
    ":operator_parse_float64",
    ":operator_parse_int32",
    ":operator_parse_int64",
    ":operator_replace",
    ":operator_rfind",
    ":operator_strip",
    ":operator_rstrip",
    ":operator_lstrip",
    ":operator_substr",
]

operator_family_lib_list = [
    ":operator_printf",
    ":operator_format",
    ":operator_join",
]

scalar_only_operator_list = [
    ":operator_compile_regex",
]

array_only_operator_list = [
    ":operator_split",
]

# Registers all operators defined in the package.
cc_library(
    name = "strings",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = (
        operator_lib_list +
        operator_family_lib_list +
        scalar_only_operator_list +
        array_only_operator_list
    ),
    alwayslink = 1,
)

# Registers metadata for all the operators definxed in the package.
cc_library(
    name = "operators_metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [lib + "_metadata" for lib in (
        operator_lib_list +
        operator_family_lib_list +
        scalar_only_operator_list +
        array_only_operator_list
    )],
    alwayslink = 1,
)

# Operators that may be useful to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_family_lib_list + [
        lib + "_plain"
        for lib in (operator_lib_list + scalar_only_operator_list)
    ] + [
        lib + "_on_optionals"
        for lib in operator_lib_list
    ],
)

# Operators that may be useful to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_family_lib_list + array_only_operator_list + [
        lib + "_on_dense_arrays"
        for lib in lifted_operator_lib_list
    ],
)

# Operators that may be useful to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_family_lib_list + array_only_operator_list + [
        lib + "_on_arrays"
        for lib in lifted_operator_lib_list
    ],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    srcs = [
        "find.cc",
        "format.cc",
        "join.cc",
        "strings.cc",
    ],
    hdrs = [
        "find.h",
        "format.h",
        "join.h",
        "split.h",
        "strings.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/strings",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_double_conversion//:double-conversion",
        "@icu4c",
    ],
)

all_lifters = [
    lift_to_optional,
    lift_to_dense_array,
    lift_to_array,
]

# Operator definitions.
operator_libraries(
    name = "operator_encode",
    operator_name = "strings.encode",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
            ],
            op_class = "::arolla::EncodeOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_as_text",
    operator_name = "strings.as_text",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = unary_args([t for t in scalar_types if t != text_type]),
            op_class = "::arolla::AsTextOp",
            deps = [":lib"],
        ),
    ) + with_lifted_by(
        all_lifters,
        [operator_overload(
            hdrs = ["strings.h"],
            args = [text_type],
            op_class = "::arolla::TextAsTextOp",
            deps = [":lib"],
        )],
    ),
)

operator_libraries(
    name = "operator_decode",
    operator_name = "strings.decode",
    overloads = with_lifted_by(
        all_lifters,
        [operator_overload(
            hdrs = ["strings.h"],
            args = ["::arolla::Bytes"],
            op_class = "::arolla::DecodeOp",
            deps = [":lib"],
        )],
    ),
)

operator_libraries(
    name = "operator_count",
    operator_name = "strings.count",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["find.h"],
            arg_lists = [
                ["::arolla::Text"] * 2,
                ["::arolla::Bytes"] * 2,
            ],
            op_class = "::arolla::SubstringOccurrenceCountOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_contains",
    operator_name = "strings.contains",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["find.h"],
            arg_lists = [
                ["::arolla::Text"] * 2,
                ["::arolla::Bytes"] * 2,
            ],
            op_class = "::arolla::ContainsOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_find",
    operator_name = "strings.find",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["find.h"],
            arg_lists = [
                [
                    s,
                    s,
                    "::arolla::OptionalValue<int64_t>",
                    "::arolla::OptionalValue<int64_t>",
                    "::arolla::OptionalValue<int64_t>",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::FindSubstringOp",
            deps = [":lib"],
        ),
    ) + operator_overload_list(
        hdrs = ["find.h"],
        arg_lists = [
            [
                s,
                s,
                "::arolla::OptionalValue<int64_t>",
                "::arolla::OptionalValue<int64_t>",
                "int64_t",
            ]
            for s in [
                "::arolla::Text",
                "::arolla::Bytes",
            ]
        ],
        op_class = "::arolla::FindSubstringOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_lower",
    operator_name = "strings.lower",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
                [
                    "::arolla::Text",
                    "::arolla::Text",
                ],
            ],
            op_class = "::arolla::LowerOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_upper",
    operator_name = "strings.upper",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
                [
                    "::arolla::Text",
                    "::arolla::Text",
                ],
            ],
            op_class = "::arolla::UpperOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_replace",
    operator_name = "strings.replace",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                [
                    s,
                    s,
                    s,
                    "::arolla::OptionalValue<int32_t>",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::ReplaceOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_strip",
    operator_name = "strings._strip",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                [
                    s,
                    "::arolla::OptionalValue<" + s + ">",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::StripOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_lstrip",
    operator_name = "strings._lstrip",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                [
                    s,
                    "::arolla::OptionalValue<" + s + ">",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::LStripOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_rstrip",
    operator_name = "strings._rstrip",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                [
                    s,
                    "::arolla::OptionalValue<" + s + ">",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::RStripOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_split",
    operator_name = "strings._split",
    overloads = operator_overload_list(
        hdrs = ["split.h"],
        arg_lists = [
            [
                array_type(s),
                "::arolla::OptionalValue<" + s + ">",
            ]
            for s in [
                "::arolla::Text",
                "::arolla::Bytes",
            ]
            for array_type in [
                make_dense_array_type,
                make_array_type,
            ]
        ],
        op_class = "::arolla::SplitOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_rfind",
    operator_name = "strings.rfind",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["find.h"],
            arg_lists = [
                [
                    s,
                    s,
                    "::arolla::OptionalValue<int64_t>",
                    "::arolla::OptionalValue<int64_t>",
                    "::arolla::OptionalValue<int64_t>",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::FindLastSubstringOp",
            deps = [":lib"],
        ),
    ) + operator_overload_list(
        hdrs = ["find.h"],
        arg_lists = [
            [
                s,
                s,
                "::arolla::OptionalValue<int64_t>",
                "::arolla::OptionalValue<int64_t>",
                "int64_t",
            ]
            for s in [
                "::arolla::Text",
                "::arolla::Bytes",
            ]
        ],
        op_class = "::arolla::FindLastSubstringOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_substr",
    operator_name = "strings.substr",
    overloads = with_lifted_by(
        [lift_to_optional],  # lifted to arrays dynamically
        operator_overload_list(
            hdrs = ["find.h"],
            arg_lists = [
                [
                    s,
                    "::arolla::OptionalValue<int64_t>",
                    "::arolla::OptionalValue<int64_t>",
                ]
                for s in [
                    "::arolla::Text",
                    "::arolla::Bytes",
                ]
            ],
            op_class = "::arolla::SubstringOp",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_bytes_length",
    operator_name = "strings.length",
    overloads = with_lifted_by(
        all_lifters,
        [operator_overload(
            hdrs = ["strings.h"],
            args = ("::arolla::Bytes",),
            op_class = "::arolla::BytesLengthOp",
            deps = [":lib"],
        )],
    ),
)

operator_libraries(
    name = "operator_text_length",
    operator_name = "strings.length",
    overloads = with_lifted_by(
        all_lifters,
        [operator_overload(
            hdrs = ["strings.h"],
            args = ("::arolla::Text",),
            op_class = "::arolla::TextLengthOp",
            deps = [":lib"],
        )],
    ),
)

operator_libraries(
    name = "operator_compile_regex",
    operator_name = "strings._compile_regex",
    overloads = [
        operator_overload(
            hdrs = ["strings.h"],
            args = ("::arolla::Text",),
            op_class = "::arolla::CompileRegexOp",
            deps = [":lib"],
        ),
    ],
)

operator_libraries(
    name = "operator_contains_regex",
    operator_name = "strings._contains_regex",
    overloads = [
        operator_overload(
            hdrs = ["strings.h"],
            args = ("::arolla::Text", "::arolla::RegexPtr"),
            build_target_groups = ["plain"],
            op_class = "::arolla::ContainsRegexOp",
            deps = [":lib"],
        ),
        operator_overload(
            hdrs = ["strings.h"],
            args = (
                make_optional_type("::arolla::Text"),
                "::arolla::RegexPtr",
            ),
            build_target_groups = ["on_optionals"],
            op_class = "::arolla::ContainsRegexOp",
            deps = [":lib"],
        ),
    ],
)

operator_libraries(
    name = "operator_extract_regex",
    operator_name = "strings._extract_regex",
    overloads = [
        operator_overload(
            hdrs = ["strings.h"],
            args = ("::arolla::Text", "::arolla::RegexPtr"),
            build_target_groups = ["plain"],
            op_class = "::arolla::ExtractRegexOp",
            deps = [":lib"],
        ),
        operator_overload(
            hdrs = ["strings.h"],
            args = (
                make_optional_type("::arolla::Text"),
                "::arolla::RegexPtr",
            ),
            build_target_groups = ["on_optionals"],
            op_class = "::arolla::ExtractRegexOp",
            deps = [":lib"],
        ),
    ],
)

operator_libraries(
    name = "operator_parse_float32",
    operator_name = "strings.parse_float32",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
                ["::arolla::Bytes"],
            ],
            op_class = "::arolla::StringsParseFloat32",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_parse_float64",
    operator_name = "strings.parse_float64",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
                ["::arolla::Bytes"],
            ],
            op_class = "::arolla::StringsParseFloat64",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_parse_int32",
    operator_name = "strings.parse_int32",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
                ["::arolla::Bytes"],
            ],
            op_class = "::arolla::StringsParseInt32",
            deps = [":lib"],
        ),
    ),
)

operator_libraries(
    name = "operator_parse_int64",
    operator_name = "strings.parse_int64",
    overloads = with_lifted_by(
        all_lifters,
        operator_overload_list(
            hdrs = ["strings.h"],
            arg_lists = [
                ["::arolla::Text"],
                ["::arolla::Bytes"],
            ],
            op_class = "::arolla::StringsParseInt64",
            deps = [":lib"],
        ),
    ),
)

operator_family(
    name = "operator_printf",
    hdrs = ["format.h"],
    op_family_class = "::arolla::PrintfOperatorFamily",
    op_family_name = "strings._printf_bytes",
    deps = [":lib"],
)

operator_family(
    name = "operator_format",
    hdrs = ["format.h"],
    op_family_class = "::arolla::FormatOperatorFamily",
    op_family_name = "strings._format_bytes",
    deps = [":lib"],
)

operator_family(
    name = "operator_join",
    hdrs = ["join.h"],
    op_family_class = "::arolla::JoinOperatorFamily",
    op_family_name = "strings._join_with_separator",
    deps = [":lib"],
)

#
# Unittests

cc_test(
    name = "strings_test",
    srcs = ["strings_test.cc"],
    deps = [
        ":strings",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "join_test",
    srcs = ["join_test.cc"],
    deps = [
        ":lib",
        ":strings",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "format_test",
    srcs = ["format_test.cc"],
    deps = [
        ":lib",
        ":strings",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "operator_family",
)

package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

operator_lib_list = [
    ":operator_seq_range",
]

# Registers all operators defined in the package.
cc_library(
    name = "seq",
    tags = ["keep_dep"],
    deps = operator_lib_list,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    tags = ["keep_dep"],
    deps = [lib + "_metadata" for lib in operator_lib_list],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "sequence_operators.h",
    ],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util:status_backport",
    ],
)

operator_family(
    name = "operator_seq_range",
    hdrs = ["sequence_operators.h"],
    op_family_class = "::arolla::SequenceRangeOpFamily",
    op_family_name = "seq.range",
    deps = [":lib"],
)

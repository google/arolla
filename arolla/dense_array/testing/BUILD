package(
    default_applicable_licenses = [
    ],
    default_testonly = 1,
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    srcs = ["bound_operators.cc"],
    hdrs = [
        "bound_operators.h",
        "util.h",
    ],
    deps = [
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/dense_array",
        "//arolla/dense_array/ops",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/math:lib",
        "//arolla/util",
    ],
)

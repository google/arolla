package(
    default_applicable_licenses = [
    ],
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])

cc_library(
    name = "strings",
    srcs = ["regex.cc"],
    hdrs = ["regex.h"],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "//arolla/qtype",
        "//arolla/util",
        "@com_googlesource_code_re2//:re2",
    ],
)

cc_test(
    name = "regex_test",
    srcs = ["regex_test.cc"],
    deps = [
        ":strings",
        "@com_google_googletest//:gtest_main",
        "@com_google_absl//absl/status",
        "//arolla/util/testing",
        "@com_googlesource_code_re2//:re2",
    ],
)

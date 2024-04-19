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

# Properties of the standard Arolla QTypes.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "standard_type_properties",
    srcs = [
        "common_qtype.cc",
        "properties.cc",
    ],
    hdrs = [
        "common_qtype.h",
        "properties.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "properties_test",
    srcs = ["properties_test.cc"],
    deps = [
        ":standard_type_properties",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "common_qtype_test",
    srcs = ["common_qtype_test.cc"],
    deps = [
        ":standard_type_properties",
        "//arolla/array/qtype",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

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

# QType-related tools for testing

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    hdrs = [
        "qtype.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = ["qtype_test.cc"],
    deps = [
        ":testing",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "dummy_types",
    testonly = 1,
    srcs = [
        "dummy_types.cc",
    ],
    hdrs = [
        "dummy_types.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
    ],
)

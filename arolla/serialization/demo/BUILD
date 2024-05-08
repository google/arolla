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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_binary(
    name = "demo",
    srcs = ["main.cc"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/serialization",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:all_decoders",
        "//arolla/util",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
    ],
)

sh_test(
    name = "demo_smoke_test",
    srcs = [":demo"],
    args = [
        "$(execpaths {})".format(f)
        for f in glob(
            ["testdata/**"],
            exclude = ["testdata/old_codec_name_input.pb"],
        )
    ],
    data = glob(["testdata/**"]),
)

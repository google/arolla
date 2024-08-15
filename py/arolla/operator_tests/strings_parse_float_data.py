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

"""Test data shared between tests for strings.parse_float{32,64}."""

# Test data records with expected outputs.
# The records have the following format: (arg, ..., result)
TEST_DATA = (
    (None, None),
    ("0", 0.0),
    ("+0", 0.0),
    ("-0", -0.0),
    ("+1.5", 1.5),
    ("-1.5", -1.5),
    ("-123.", -123.0),
    ("0755", 755),
    ("00755", 755),
    ("100", 100),
    ("1.5e4", 1.5e4),
    ("1.5e+4", 1.5e4),
    ("-1.5e-4", -1.5e-4),
    ("1.5E4", 1.5e4),
    ("1.5E+4", 1.5e4),
    ("-1.5E-4", -1.5e-4),
    ("nan", float("nan")),
    ("Nan", float("nan")),
    ("NAN", float("nan")),
    ("+nan", float("nan")),
    ("+Nan", float("nan")),
    ("+NAN", float("nan")),
    ("-nan", float("nan")),
    ("-Nan", float("nan")),
    ("-NAN", float("nan")),
    ("inf", float("inf")),
    ("Inf", float("inf")),
    ("INF", float("inf")),
    ("infinity", float("inf")),
    ("Infinity", float("inf")),
    ("INFINITY", float("inf")),
    ("+inf", float("inf")),
    ("+Inf", float("inf")),
    ("+INF", float("inf")),
    ("+infinity", float("inf")),
    ("+Infinity", float("inf")),
    ("+INFINITY", float("inf")),
    ("-inf", float("-inf")),
    ("-Inf", float("-inf")),
    ("-INF", float("-inf")),
    ("-infinity", float("-inf")),
    ("-Infinity", float("-inf")),
    ("-INFINITY", float("-inf")),
)

# Test data records that trigger errors.
# The records have Have the following format: (arg, ...)
ERROR_TEST_DATA = (
    "",
    " ",
    "bad-input",
    "1.x",
    "++1",
    "+-1",
    "-+1",
    "--1",
    " 1",
    "1 ",
    " 1 ",
    "0x1",
    "0X1",
    "0b1",
    "0B1",
    "1_000_000",
    "1'000'000",
    "1+1",
    "3 million",
    "¾",
    "①",
    "五",
)

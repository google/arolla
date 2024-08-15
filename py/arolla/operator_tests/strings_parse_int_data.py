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

"""Test data shared between tests for strings.parse_int{32,64}."""

# Test data records has the following format: (arg, ..., result)
TEST_DATA = (
    (None, None),
    ("0", 0.0),
    ("+0", 0),
    ("-0", 0),
    ("1", 1),
    ("+1", 1),
    ("-1", -1),
    ("-123", -123),
    ("0755", 755),
    ("000755", 755),
    ("100", 100),
    ("10000", 10**4),
    ("1000000", 10**6),
    ("100000000", 10**8),
    ("+100", 100),
    ("+10000", 10**4),
    ("+1000000", 10**6),
    ("+100000000", 10**8),
    ("-100", -100),
    ("-10000", -(10**4)),
    ("-1000000", -(10**6)),
    ("-100000000", -(10**8)),
    (str(-(2**31)), -(2**31)),
    (str(2**31 - 1), 2**31 - 1),
)

# Test data records has the following format: (arg, ..)
ERROR_TEST_DATA = (
    "",
    " ",
    "bad-input",
    "1.",
    "1x",
    "++1",
    "+-1",
    "-+1",
    "--1",
    " 1",
    "1 ",
    "0x1",
    "0X1",
    "0b1",
    "0B1",
    "100000000000000000000000000000",  # overflow
    "1_000_000",
    "1'000'000",
    "1+1",
    "3 million",
    "¾",
    "①",
    "五",
)

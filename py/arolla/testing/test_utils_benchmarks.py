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

"""Benchmarks for python decision forest operations."""

from arolla import arolla
from arolla.testing import detect_qtype_signatures
import google_benchmark

M = arolla.M


@google_benchmark.register
def sigmoid_signature_detection(state):
  while state:
    len(detect_qtype_signatures.detect_qtype_signatures(M.math.sigmoid))


if __name__ == '__main__':
  google_benchmark.main()
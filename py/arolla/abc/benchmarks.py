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

"""Benchmarks for arolla.abc."""

from arolla import arolla
import google_benchmark


@google_benchmark.register
def invoke_add(state):
  one = arolla.int32(1)
  math_add = arolla.M.math.add

  # Warm up compilation cache.
  arolla.abc.invoke_op(math_add, (one, one))

  while state:
    arolla.abc.invoke_op(math_add, (one, one))


@google_benchmark.register
def invoke_add_by_name(state):
  one = arolla.int32(1)

  # Warm up compilation cache.
  arolla.abc.invoke_op('math.add', (one, one))

  while state:
    arolla.abc.invoke_op('math.add', (one, one))


if __name__ == '__main__':
  google_benchmark.main()

# Copyright 2025 Google LLC
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

"""Benchmarks for arolla.py_utils.py_hash."""

from arolla.py_utils import testing_clib
import google_benchmark

_CASES = {
    'ascii': lambda n: b'a' * n,
    'ucs1_mostly_ascii': lambda n: ('a' * max(0, n - 1) + 'é').encode('utf-8'),
    'ucs1_dense': lambda n: ('café' * max(1, n // 4))[:n].encode('utf-8'),
    'ucs2_mostly_ascii': lambda n: ('a' * max(0, n - 1) + 'ї').encode('utf-8'),
    'ucs2_dense': lambda n: ('привет' * max(1, n // 6))[:n].encode('utf-8'),
    'ucs4_mostly_ascii': lambda n: ('a' * max(0, n - 1) + '🎉').encode('utf-8'),
    'ucs4_dense': lambda n: ('🎉' * n).encode('utf-8'),
}


def _register_utf8_benchmarks():
  """Registers UTF-8 hash benchmarks."""
  for name, factory in _CASES.items():
    lengths = [16, 1000, 1_000_000]
    if name == 'ascii':
      lengths = [0] + lengths

    @google_benchmark.register(name=f'py_hash_utf8_{name}')
    @google_benchmark.option.arg_names(['n'])
    @google_benchmark.option.args_product([lengths])
    def bench_fast(state, factory=factory):
      data = factory(state.range(0))
      while state:
        testing_clib.py_hash_utf8(data)

    @google_benchmark.register(name=f'py_hash_utf8_reference_{name}')
    @google_benchmark.option.arg_names(['n'])
    @google_benchmark.option.args_product([lengths])
    def bench_reference(state, factory=factory):
      data = factory(state.range(0))
      while state:
        testing_clib.py_hash_utf8_reference(data)


_register_utf8_benchmarks()


if __name__ == '__main__':
  google_benchmark.main()

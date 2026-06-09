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

"""Benchmarks for arolla.optools."""

import linecache
import uuid
from arolla import arolla
import google_benchmark


def make_expr_factory(node_count):
  """Constructs a function that produces an expr with node_count nodes."""

  lines = [
      'def fn(x):\n',
      '  res = x\n',
  ]
  for _ in range(node_count):
    lines.append('  res = arolla.M.core.presence_and(res, x)\n')
  lines.append('  return res\n')

  filename = f'dummy_{uuid.uuid4().hex}.py'
  linecache.cache[filename] = (len(lines), None, lines, filename)
  # Explicitly compile the code to wire with the linecache by filename.
  code_obj = compile(''.join(lines), filename, 'exec')
  namespace = dict(**globals())
  exec(code_obj, namespace)  # pylint: disable=exec-used
  return namespace['fn']


@google_benchmark.register
@google_benchmark.option.arg_names(['n', 'source_locations'])
@google_benchmark.option.args_product([[1, 10, 100], [False, True]])
def trace_function(state):
  n = state.range(0)
  annotate = bool(state.range(1))
  fn = make_expr_factory(n)
  while state:
    arolla.optools.trace_function(fn, annotate_with_source_locations=annotate)


if __name__ == '__main__':
  google_benchmark.main()

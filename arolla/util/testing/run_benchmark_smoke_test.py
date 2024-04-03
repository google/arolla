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

"""Runs specified benchmarks and fails if they fail."""

import json
import subprocess
import sys

from absl import app
from absl import flags

FLAGS = flags.FLAGS
TIMEOUT = 60 * 15

flags.DEFINE_string(
    'binary', None, help='Benchmarks binary file.', required=True)
flags.DEFINE_string(
    'benchmark_filter',
    'all',
    help='Regexp to pass as --benchmark_filter arg to the binary.')
flags.DEFINE_multi_string(
    'extra_args', [], help='Extra arguments to pass to the binary.')


def main(_):
  binary_args = [
      FLAGS.binary,
      # Set min_time to 1us to run only few (usually 1) iterations.
      '--benchmark_min_time=0.000001s',
      '--benchmark_format=json',
      # disable interleaving to speed up the test
      '--benchmark_enable_random_interleaving=false',
      f'--benchmark_filter={FLAGS.benchmark_filter}',
  ] + FLAGS.extra_args
  process = subprocess.Popen(
      binary_args, stderr=sys.stderr, stdout=subprocess.PIPE)
  stdout, _ = process.communicate(timeout=TIMEOUT)
  report = json.loads(stdout)
  if 'benchmarks' not in report or not report['benchmarks']:
    print(
        f'No benchmarks matched the filter "{FLAGS.benchmark_filter}"',
        file=sys.stderr)
    return 1
  else:
    names = ', '.join(b['name'] for b in report['benchmarks'])
    print(
        f'Successfully ran {len(report["benchmarks"])} benchmarks: {names}',
        file=sys.stderr)
  return 0


if __name__ == '__main__':
  app.run(main)

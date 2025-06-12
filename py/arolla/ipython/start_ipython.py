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

"""IPython-based REPL for Arolla.

Usage:

  %s [<absl-flags options>] [-- <IPython options>]

Example:

  %s -- -c 'print("Hello, World!")'
"""

from absl import app
from absl import flags
from absl import logging as _
import IPython

FLAGS = flags.FLAGS


def main(argv):
  IPython.start_ipython(argv=argv[1:])


if __name__ == '__main__':
  app.run(main)

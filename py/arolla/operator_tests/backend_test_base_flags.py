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

"""Flags used by backend_test_base backends."""

from absl import flags

_EXTRA_FLAGS = flags.DEFINE_multi_string(
    'extra_flags',
    default=[],
    help=(
        'list of additional flags local to specific backends. Should be'
        ' supplied as <flag_name>:<contents>.'
    ),
)


def get_extra_flag(flag_name: str) -> list[str]:
  """Returns a list of values for the given flag_name prefix."""
  res = []
  for extra_flag in _EXTRA_FLAGS.value:
    parts = extra_flag.split(':', maxsplit=1)
    if len(parts) != 2:
      raise ValueError(
          f'invalid `extra_flags` value: {extra_flag!r}. Expected the form'
          ' `<flag_name>:<contents>`'
      )
    if parts[0] == flag_name:
      res.append(parts[1])
  return res

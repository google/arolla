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

"""Test accessor generators."""

from typing import Callable, List, Tuple

from arolla.codegen.io import accessor_generator
from arolla.codegen.io import accessors
from arolla.codegen.io import protopath


def gen_zero_one() -> (
    Callable[[accessor_generator.Config], List[Tuple[str, accessors.Accessor]]]
):
  """Generated test accessors without any arguments."""

  def gen(
      cfg: accessor_generator.Config,
  ) -> List[Tuple[str, accessors.Accessor]]:
    assert cfg.io_cpp_type == '::std::array<int, 10>'
    assert cfg.array_type == 'DenseArray'
    return [
        ('', accessors.path_accessor('[0]', 'zero')),
        ('one', accessors.path_accessor('[1]')),
    ]

  return gen


def gen_from_args(
    *args,
) -> Callable[
    [accessor_generator.Config], List[Tuple[str, accessors.Accessor]]
]:
  """Generated test accessors from arguments."""

  def gen(
      cfg: accessor_generator.Config,
  ) -> List[Tuple[str, accessors.Accessor]]:
    assert cfg.io_cpp_type == '::std::array<int, 10>'
    assert cfg.array_type == 'DenseArray'
    return [
        ('', accessors.path_accessor(f'[{idx}]', f'a_{idx}')) for idx in args
    ]

  return gen


def gen_from_data_file(
    filename,
) -> Callable[
    [accessor_generator.Config], List[Tuple[str, accessors.Accessor]]
]:
  """Generated test accessors from data file."""

  def gen(
      cfg: accessor_generator.Config,
  ) -> List[Tuple[str, accessors.Accessor]]:
    assert cfg.io_cpp_type == '::std::array<int, 10>'
    assert cfg.array_type == 'DenseArray'
    with open(filename, 'rt') as f:
      indices = [int(idx.strip()) for idx in f]
      return [
          (f'f_{idx}', accessors.path_accessor(f'[{idx}]')) for idx in indices
      ]

  return gen


def gen_scalar_accessor_with_default_value() -> (
    Callable[[accessor_generator.Config], List[Tuple[str, accessors.Accessor]]]
):
  """Generated test accessors without any arguments."""

  def gen(
      cfg: accessor_generator.Config,
  ) -> List[Tuple[str, accessors.Accessor]]:
    assert not cfg.is_mutable
    x = protopath.Protopath.parse(
        '/x', cfg.io_cpp_type)
    xf = protopath.Protopath.parse(
        '/inner/inner2/root_reference/x_float', cfg.io_cpp_type)
    a = protopath.Protopath.parse(
        '/inner/root_reference/inner/a', cfg.io_cpp_type)
    xrr = protopath.Protopath.parse(
        '/inner/inner2/root_reference/x', cfg.io_cpp_type)
    return [
        (
            'a',
            a.accessor(),
        ),
        (
            'x0',
            x.scalar_accessor_with_default_value(
                cpp_type='int32_t', default_value_cpp='0'
            ),
        ),
        (
            'x1',
            x.scalar_accessor_with_default_value(
                cpp_type='int32_t', default_value_cpp='1'
            ),
        ),
        (
            'xf3',
            xf.scalar_accessor_with_default_value(
                cpp_type='float', default_value_cpp='3.0f'
            ),
        ),
        (
            'xrr',
            xrr.accessor(),
        ),
        (
            'xf4',
            xf.scalar_accessor_with_default_value(
                cpp_type='float', default_value_cpp='4.0f'
            ),
        ),
    ]

  return gen


def gen_input_loader_set_spec():
  return {
      '::aaa::GetLoader1': {
          'input_cls': '::std::array<int, 10>',
          'hdrs': [],
          'accessors': [
              ('', accessors.path_accessor('[3]', 'a3')),
              ('', accessors.path_accessor('[5]', 'a5')),
          ],
      },
      '::bbb::GetLoader2': {
          'input_cls': '::std::array<float, 5>',
          'hdrs': [],
          'accessors': [
              ('', accessors.path_accessor('[3]', 'a3')),
              ('', accessors.path_accessor('[2]', 'a2')),
          ],
      },
  }


def gen_input_loader_set_input_cls() -> str:
  return 'int'


_listener1 = {
    'output_cls': '::std::array<int, 10>',
    'hdrs': [],
    'accessors': [
        (
            'a3',
            accessors.Accessor(
                '[](auto){return [](int i, ::std::array<int, 10>* out)'
                '{(*out)[3] = i;};}',
                [],
            ),
        ),
        (
            'a5',
            accessors.Accessor(
                '[](auto){return [](int i, ::std::array<int, 10>* out)'
                '{(*out)[5] = i;};}',
                [],
            ),
        ),
    ],
}


_listener2 = {
    'output_cls': '::std::array<float, 5>',
    'hdrs': [],
    'accessors': [
        (
            'a3',
            accessors.Accessor(
                '[](auto){return [](float i, ::std::array<float, 5>* out)'
                '{(*out)[3] = i;};}',
                [],
            ),
        ),
        (
            'a2',
            accessors.Accessor(
                '[](auto){return [](float i, ::std::array<float, 5>* out)'
                '{(*out)[2] = i;};}',
                [],
            ),
        ),
    ],
}


def gen_slot_listener_set_spec():
  return {
      '::aaa::GetListener1': _listener1,
      '::bbb::GetListener2': _listener2,
  }


def gen_sharded_slot_listener_set_spec():
  res = {
      '::aaa::GetShardedListener1': dict(_listener1),
      '::bbb::GetShardedListener2': dict(_listener2),
  }
  for _, spec in res.items():
    spec['sharding'] = {'shard_count': 2}
  return res

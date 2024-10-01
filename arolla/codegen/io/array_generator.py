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

"""Helper functions to generate code filling array."""

import abc
from typing import List, Optional

from arolla.codegen.io import cpp


class ArrayBuilderGenerator(abc.ABC):
  """Interface to generate code for filling array."""

  def __init__(self, name_prefix: str = ''):
    """All variable names will have specified name_prefix."""
    self.name_prefix = name_prefix

  @abc.abstractmethod
  def define_array(self,
                   value_type_name: str,
                   total_size_name: str,
                   buffer_factory_name: str,
                   *,
                   always_present: bool = False) -> str:
    """Defines variables needed for construction.

    Args:
      value_type_name: name of the type alias for value type
      total_size_name: name of the variable storing total size of the array
      buffer_factory_name: name of the variable with pointer to RawBufferFactory
      always_present: if true, array will have no missed values
    """
    raise NotImplementedError

  @abc.abstractmethod
  def push_back_empty_item(self) -> str:
    """Generates code to add empty item to the end of array."""
    raise NotImplementedError

  @abc.abstractmethod
  def push_back_item(self, value: str) -> str:
    """Generates code to add specified value to the end of array."""
    raise NotImplementedError

  @abc.abstractmethod
  def return_array(self) -> str:
    """Generates code returning constructed array."""
    raise NotImplementedError

  @abc.abstractmethod
  def shape_type(self) -> str:
    """Returns C++ type of the Array shape."""
    raise NotImplementedError

  def create_shape(self, size_var: str) -> str:
    return f'{self.shape_type()}{{{size_var}}}'

  @abc.abstractmethod
  def required_includes(self) -> List[cpp.Include]:
    """Returns list of required includes."""
    raise NotImplementedError


# TODO: Optimize (requires changes in the interface)
class _DenseArrayCodeGenerator(ArrayBuilderGenerator):
  """Helper generator for DenseArray construction."""

  def define_array(self,
                   value_type_name: str,
                   total_size_name: str,
                   buffer_factory_name: str,
                   *,
                   always_present: bool = False) -> str:
    del always_present  # not used
    p = self.name_prefix
    return """
using {p}result_type = ::arolla::DenseArray<{value_type_name}>;
typename ::arolla::Buffer<{value_type_name}>::Builder {p}bldr(
    {total_size_name}, {buffer_factory_name});
auto {p}inserter = bldr.GetInserter();
int64_t {p}id = 0;
::arolla::bitmap::AlmostFullBuilder {p}bitmap_bldr(
    {total_size_name}, {buffer_factory_name});
""".format(
        p=p,
        value_type_name=value_type_name,
        total_size_name=total_size_name,
        buffer_factory_name=buffer_factory_name,
    )

  def push_back_empty_item(self) -> str:
    return '{p}bitmap_bldr.AddMissed({p}id++);\n{p}inserter.SkipN(1);\n'.format(
        p=self.name_prefix)

  def push_back_item(self, value: str) -> str:
    return '{p}id++;\n{p}inserter.Add({value});\n'.format(
        value=value, p=self.name_prefix)

  def return_array(self) -> str:
    return """
return {p}result_type{{std::move({p}bldr).Build(),
                       std::move({p}bitmap_bldr).Build()}};
""".format(p=self.name_prefix)

  def shape_type(self) -> str:
    return '::arolla::DenseArrayShape'

  def required_includes(self) -> List[cpp.Include]:
    return [
        cpp.Include('arolla/dense_array/dense_array.h'),
        cpp.Include('arolla/dense_array/qtype/types.h'),
        cpp.Include('arolla/io/proto_types/types.h'),
        cpp.Include('arolla/qtype/qtype.h'),
    ]


def create_generator(array_type: str,
                     name_prefix: str = '') -> Optional[ArrayBuilderGenerator]:
  """Returns builder by given name. Only DenseArray is supported."""
  if not array_type:
    return None
  if array_type == 'DenseArray':
    return _DenseArrayCodeGenerator(name_prefix)
  raise NotImplementedError('Unsupported array type: ' + array_type)

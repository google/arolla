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

"""A simple mutable dataset abstraction."""

from typing import Any as _AnyType
from typing import KeysView as _KeysViewType

from arolla import arolla

_annotation_qtype = arolla.M.annotation.qtype


def _leaf_with_qtype(key: str, value_qtype: arolla.QType):
  """Returns a leaf node with an annotation based on 'with_qtype' operator."""
  return _annotation_qtype(arolla.L[key], value_qtype)


class _LeafProvider:
  """A helper class that provides access to the DataSet's leaves."""

  def __init__(self, leaves: dict[str, arolla.abc.Expr]):
    # Keep reference to the original `leaves` object, so we can see updates.
    self._leaves = leaves

  def __dir__(self) -> _KeysViewType[str]:  # pytype: disable=signature-mismatch  # overriding-return-type-checks
    """Enable autocompletion in Colab."""
    return self._leaves.keys()

  def __repr__(self) -> str:
    return 'Leaves{L.' + ', L.'.join(sorted(self._leaves)) + '}'

  def __getattr__(self, key: str) -> arolla.Expr:
    value = self._leaves.get(key)
    if value is None:
      raise AttributeError(key)
    return _leaf_with_qtype(key, value.qtype)


class DataSet:
  """A simple mutable dataset abstraction."""

  def __init__(self, **leaves: _AnyType):
    """Constructor.

    Args:
      **leaves: Leave values.
    """
    self._leaves = {}
    self._leaf_provider = _LeafProvider(self._leaves)
    for key, value in leaves.items():
      setattr(self, key, value)

  @property
  def vars(self) -> _LeafProvider:
    return self._leaf_provider

  def __dir__(self) -> _KeysViewType[str]:  # pytype: disable=signature-mismatch  # overriding-return-type-checks
    """Enable autocompletion in Colab."""
    return self._leaves.keys()

  def __getattr__(self, key: str):
    """Access to column."""
    if key.startswith('_'):
      return super().__getattr__(key)  # pytype: disable=attribute-error
    if key not in self._leaves:
      raise AttributeError(key)
    return self._leaves[key]

  def __setattr__(self, key: str, value: _AnyType) -> None:
    """Assigns a column."""
    if key.startswith('_'):
      return super().__setattr__(key, value)
    try:
      value = arolla.as_qvalue(value)
    except TypeError:
      raise TypeError(
          'Expected QValue, got {}.'.format(
              arolla.abc.get_type_name(type(value))
          )
      ) from None
    self._leaves[key] = value

  def __delattr__(self, key: str) -> None:
    """Deletes a column."""
    if key.startswith('_'):
      return super().__delattr__(key)  # pytype: disable=attribute-error
    if key not in self._leaves:
      raise AttributeError(key)
    del self._leaves[key]

  def eval(self, expr):
    """Evaluate expression."""
    return arolla.eval(expr, **self._leaves)

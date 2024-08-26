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

"""Module with expression related containers."""

from __future__ import annotations

import collections
import functools
from typing import Collection, Iterator, Mapping

from arolla.abc import abc as arolla_abc
from arolla.expr import builtin_ops


class LeafContainer:
  """Helper container to create leaves.

  Usage example:
    L = LeafContainer()
    leaf = L.key_of_the_leaf
  """

  __slots__ = ()

  __getattribute__ = staticmethod(arolla_abc.leaf)

  def __getitem__(self, leaf_key: str) -> arolla_abc.Expr:
    if not isinstance(leaf_key, str):
      raise TypeError(
          'Leaf key must be str, not {}.'.format(
              arolla_abc.get_type_name(type(leaf_key))
          )
      )
    return arolla_abc.leaf(leaf_key)


class PlaceholderContainer:
  """Helper container to create placeholders.

  Usage example:
    P = PlaceholderContainer()
    placeholder = P.key_of_the_placeholder
  """

  __slots__ = ()

  __getattribute__ = staticmethod(arolla_abc.placeholder)

  def __getitem__(self, placeholder_key: str) -> arolla_abc.Expr:
    if not isinstance(placeholder_key, str):
      raise TypeError(
          'Placeholder key must be str, not {}.'.format(type(placeholder_key))
      )
    return arolla_abc.placeholder(placeholder_key)


def _extract_all_namespaces(name: str) -> Iterator[str]:
  """Yields all namespace: 'a.b.name' -> 'a', 'a.b', 'a.b.name'."""
  if not name:
    return
  i = name.find('.')
  while i != -1:
    yield name[:i]
    i = name.find('.', i + 1)
  yield name


# Cache with namespaces and operator names for OperatorsContainer.__dir__.
_operators_container_cache_revision_id: int = 0
_operators_container_cache_namespaces: frozenset[str] = frozenset()
_operators_container_cache_operators_by_prefix: Mapping[str, list[str]] = (
    collections.defaultdict(list)
)


def _clear_operators_container_cache() -> None:
  """Clears the _operators_container_cache_*."""
  global _operators_container_cache_revision_id
  global _operators_container_cache_namespaces
  global _operators_container_cache_operators_by_prefix
  _operators_container_cache_revision_id = 0
  _operators_container_cache_namespaces = frozenset()
  _operators_container_cache_operators_by_prefix = collections.defaultdict(list)


arolla_abc.cache_clear_callbacks.add(_clear_operators_container_cache)


def _actualize_operators_container_cache() -> None:
  """Actualizes the _operators_container_cache_*."""
  global _operators_container_cache_revision_id
  global _operators_container_cache_namespaces
  global _operators_container_cache_operators_by_prefix
  revision_id = arolla_abc.get_registry_revision_id()
  if revision_id == _operators_container_cache_revision_id:
    return
  namespaces = set()
  operators_by_prefix = collections.defaultdict(list)
  for name in arolla_abc.list_registered_operators():
    ns, sep, op_name = name.rpartition('.')
    namespaces.update(_extract_all_namespaces(ns))
    operators_by_prefix[ns + sep].append(op_name)
  _operators_container_cache_revision_id = revision_id
  _operators_container_cache_namespaces = frozenset(namespaces)
  _operators_container_cache_operators_by_prefix = operators_by_prefix


@functools.lru_cache
def _unsafe_make_registered_operator(
    operator_name: str,
) -> arolla_abc.RegisteredOperator:
  """Returns a proxy to an operator in the registry.

  Note: The key difference from arolla.abc.unsafe_make_registered_operator() is
  that this function caches operator instances, so that they do not need to be
  created repeatedly. We cannot use caching in arolla.abc as qvalue
  specialization might not yet be available there.

  Args:
    operator_name: Operator name.
  """
  return arolla_abc.unsafe_make_registered_operator(operator_name)


arolla_abc.cache_clear_callbacks.add(
    _unsafe_make_registered_operator.cache_clear
)  # subscribe the lru_cache for cleaning


def _new_operators_container(
    prefix: str, visible_namespaces: Collection[str]
) -> OperatorsContainer:
  """Returns a new instance of OperatorsContainer."""
  result = object.__new__(OperatorsContainer)
  result._prefix = prefix  # pylint: disable=protected-access
  result._visible_namespaces = (  # pylint: disable=protected-access
      visible_namespaces
  )
  return result


@functools.lru_cache
def _get_operators_sub_container(
    container: OperatorsContainer, ns: str
) -> OperatorsContainer:
  """Returns a sub-container of a container."""
  if ns not in container._visible_namespaces:  # pylint: disable=protected-access
    raise AttributeError(f'{ns!r} is not present in container')
  return _new_operators_container(ns + '.', container._visible_namespaces)  # pylint: disable=protected-access


arolla_abc.cache_clear_callbacks.add(
    _get_operators_sub_container.cache_clear
)  # subscribe the lru_cache for cleaning


class OperatorsContainer:
  """Helper container to create operations.

  Usage example:
    M = OperatorsContainer()
    plus_node = M.math.add(L.x, L.y)
  """

  _HAS_DYNAMIC_ATTRIBUTES = True

  __slots__ = ('_prefix', '_visible_namespaces')

  _prefix: str
  _visible_namespaces: Collection[str]

  def __new__(cls, *extra_modules) -> OperatorsContainer:
    """Returns an OperatorsContainer for specified modules.

    Builtin operators are always included.

    Args:
      *extra_modules: Operator modules to be supported by the container. Each
        passed module should initialize its operators on import. The module must
        provide get_namespaces() method returning a list of supported
        namespaces, e.g., ['test', 'test1.test2'].
    """
    visible_namespaces = set()
    for module in (builtin_ops,) + extra_modules:
      for ns in module.get_namespaces():
        visible_namespaces.update(_extract_all_namespaces(ns))
    return _new_operators_container('', frozenset(visible_namespaces))

  def __getattr__(
      self, key: str
  ) -> arolla_abc.RegisteredOperator | OperatorsContainer:
    """Returns an operator or a container of operators for an inner namespace.

    Args:
      key: A string key.
    """
    # assert name and '.' not in name  # avoid doing extra work here
    name = self._prefix + key
    if arolla_abc.check_registered_operator_presence(name):
      return _unsafe_make_registered_operator(name)
    return _get_operators_sub_container(self, name)

  def __getitem__(self, key: str) -> arolla_abc.RegisteredOperator:
    """Returns an operator."""
    # assert name  # avoid doing extra work here
    name = self._prefix + key
    if arolla_abc.check_registered_operator_presence(name) and (
        '.' not in key or name.rpartition('.')[0] in self._visible_namespaces
    ):
      return _unsafe_make_registered_operator(name)
    raise LookupError(f'operator {name!r} is not present in container')

  def __dir__(self) -> list[str]:
    """Returns suggestions for the current prefix."""
    _actualize_operators_container_cache()
    result = []
    for ns in self._visible_namespaces:
      if ns.startswith(self._prefix) and ns.find('.', len(self._prefix)) == -1:
        result.append(ns[len(self._prefix) :])
    result += _operators_container_cache_operators_by_prefix.get(
        self._prefix, []
    )
    return result

  def __bool__(self) -> bool:
    """Returns True if the container is non-empty."""
    _actualize_operators_container_cache()
    if _operators_container_cache_operators_by_prefix.get(self._prefix):
      return True
    for ns in self._visible_namespaces:
      if ns.startswith(self._prefix):
        return True
    return False


class _CollectionOfAllNamespaces:
  """A helper type that exposes all namespaces."""

  def __contains__(self, _) -> bool:
    return True

  def __iter__(self) -> Iterator[str]:
    _actualize_operators_container_cache()
    return iter(_operators_container_cache_namespaces)

  def __len__(self):
    raise NotImplementedError


_unsafe_operators_container = _new_operators_container(
    '', _CollectionOfAllNamespaces()
)


def unsafe_operators_container():
  """Returns a container of all loaded operators, regardless of namespace.

  Caution: This operators container does not track the build dependencies.
  This means that it cannot guarantee that all of the operators you need
  will be available. The availability of the operators depends on the current
  environment.
  """
  return _unsafe_operators_container

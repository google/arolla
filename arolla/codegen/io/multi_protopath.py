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

"""Generator for code accessing multiple protobuf fields simuntaniously.

Repeating nested access to the long chain of the fields may be expensive.
This functions are trying to generate code to access many fields simuntaniously.
"""

from __future__ import annotations

import abc
import dataclasses
import enum
from typing import Dict, List, Optional, Tuple, TypeVar, Set

from arolla.codegen.io import accessors
from arolla.codegen.io import cpp
from arolla.codegen.io import protopath

T = TypeVar('T')
V = TypeVar('V')


@dataclasses.dataclass
class ProtopathTreeNodeBase(abc.ABC):
  """Base class for representing collection of protopathes."""
  # parent node in the tree. None only for the root (/).
  parent: Optional['ProtopathTreeNodeBase']
  # list of children nodes in the tree
  children: List['ProtopathTreeNodeBase']
  # Name of the accessor for the leaf name. Must be unique for all leaves.
  # Empty string for intermediate nodes.
  leaf_accessor_name: str = ''
  # Accessor for the leaf. None for intermediate nodes.
  leaf_accessor: Optional[protopath.ProtopathAccessor] = None
  is_size: bool = False

  def is_leaf(self) -> bool:
    # root is not considered as leaf
    return not self.children and self.parent is not None

  def depth_without_fictive(self) -> int:
    """Returns depth of the node excluding fictive nodes."""
    res = 0
    node = self
    while node.parent:
      res += 1 if not node.is_fictive() else 0
      node = node.parent
    return res

  def leaves(self: T) -> List[T]:
    """Return list of leaves in the post order."""
    if self.is_leaf():
      return [self]
    return sum([c.leaves() for c in self.children], [])

  def post_order_nodes(self: T) -> List[T]:
    """Return list of nodes in the post order."""
    return sum([c.post_order_nodes() for c in self.children], []) + [self]

  @abc.abstractmethod
  def access_for_type(self, var_name: str) -> str:
    """Returns type access to find type strarting from given variable."""
    raise NotImplementedError()

  @abc.abstractmethod
  def is_fictive(self) -> bool:
    """Returns true iff node is fictive and doesn't contain any path info."""
    raise NotImplementedError()

  def non_fictive_ancestor(self) -> ProtopathTreeNodeBase:
    """Returns the first non fictive ancestor in the path to the root."""
    node = self
    while node.is_fictive():
      node = node.parent
      assert node is not None
    return node

  @abc.abstractmethod
  def build_fictive_child(self: T, children: List[T]) -> T:
    """Returns new fictive child for the given node."""
    raise NotImplementedError()

  @abc.abstractmethod
  def protopath(self) -> str:
    """Returns protopath corresponed to this node."""
    raise NotImplementedError()

  @property
  def comment(self) -> str:
    if self.is_leaf():
      return f'protopath=`{self.protopath()}` name=`{self.leaf_accessor_name}`'
    else:
      ppath = self.protopath() or 'ROOT'
      msg = f'protopath=`{ppath}`'
      msg += ' fictive' if self.is_fictive() else ''
      return msg

  @property
  def required_includes(self) -> Set[cpp.Include]:
    """Returns set of cpp includes required for the code generation."""
    includes = set()
    leaves = self.leaves()
    if leaves:
      includes.add(cpp.Include('arolla/codegen/io/multi_loader.h'))
    if any((l.is_size for l in leaves)):
      includes.add(cpp.Include('arolla/dense_array/qtype/types.h'))
    for leaf in leaves:
      if leaf.leaf_accessor is None:
        raise ValueError(f'Leaf {leaf} has None leaf_accessor')
      includes = includes.union(leaf.leaf_accessor.required_includes)
    return includes

  def count_children_in_set(self: T, vals: Set[T]) -> int:
    """Returns number of children in the set."""
    return sum(1 for child in self.children if child in vals)

  def count_children_in_dict(self: T, vals: Dict[T, V], value: V) -> int:
    """Returns number of children with a specific value in the dict."""
    return sum(1 for child in self.children if vals.get(child) == value)


TBase = TypeVar('TBase', bound=ProtopathTreeNodeBase)


@dataclasses.dataclass
class SingleValueProtopathTreeNode(ProtopathTreeNodeBase):
  """Tree representing collection of single value protopathes."""
  # parent node in the tree. None only for the root (/).
  parent: Optional['SingleValueProtopathTreeNode']
  # list of children nodes in the tree
  children: List['SingleValueProtopathTreeNode']
  # protopath element to access from the parent.
  # None for the root (/) and fictive nodes.
  path_from_parent: Optional[protopath.SingleValueElement] = None

  def has_size_leaf(self) -> bool:
    """Returns true iff at least one leaf has is_size True."""
    return any(l.is_size for l in self.leaves())

  def has_optional_leaf(self) -> bool:
    """Returns true iff at least one leaf is optional."""
    return any(
        not l.is_size and l.leaf_accessor.default_value_cpp is None
        for l in self.leaves())

  def has_leaf_with_default_value(self) -> bool:
    """Returns true iff at least one leaf is optional."""
    return any(
        not l.is_size and l.leaf_accessor.default_value_cpp is not None
        for l in self.leaves())

  def access_for_type(self, var_name: str) -> str:
    node = self
    path = []
    while node.parent is not None:
      if not node.is_fictive():
        path.append(node)
      node = node.parent
    res = var_name
    for node in reversed(path):
      res = node.path_from_parent.access(res)
    return res

  def is_fictive(self) -> bool:
    """Returns true iff node is fictive and doesn't contain any path info."""
    return self.parent is not None and self.path_from_parent is None

  def build_fictive_child(
      self, children: List['SingleValueProtopathTreeNode']
  ) -> 'SingleValueProtopathTreeNode':
    """Returns new fictive child for the given node."""
    return SingleValueProtopathTreeNode(parent=self, children=children)

  def protopath(self) -> str:
    """Returns protopath corresponed to this node."""
    res = ''
    node = self
    while node.parent is not None:
      if not node.is_fictive():
        # assert is implied by fictive check
        assert node.path_from_parent is not None
        res = node.path_from_parent.protopath_element() + (f'/{res}'
                                                           if res else '')
      node = node.parent
    return res

  def __hash__(self) -> int:
    return hash(id(self))

  def __eq__(self: T, other: T) -> bool:
    return id(self) == id(other)

  def __str__(self):
    return f'{type(self).__name__}({self.access_for_type("input")})'

  def __repr__(self):
    return str(self)


@dataclasses.dataclass
class NodeNumeration:
  """Three ways of enumeration grah nodes."""
  # mapping only leaf nodes in to the id in post order
  leaf2id: Dict[SingleValueProtopathTreeNode, int]
  # mapping only intermediate nodes in to the id in post order
  intermediate2id: Dict[SingleValueProtopathTreeNode, int]
  # mapping all nodes in to the id in post order
  node2id: Dict[SingleValueProtopathTreeNode, int]


def create_node_numeration(root: ProtopathTreeNodeBase) -> NodeNumeration:
  """Creates NodeNumeration for the given graph."""
  result = NodeNumeration({}, {}, {})
  leaf_id = 0
  for i, node in enumerate(root.post_order_nodes()):
    result.node2id[node] = i
    if node.is_leaf():
      result.leaf2id[node] = leaf_id
      leaf_id += 1
    else:
      # subtract number of already visited leaves
      result.intermediate2id[node] = i - leaf_id
  return result


def size_leaf_ids(root: ProtopathTreeNodeBase,
                  node_numeration: NodeNumeration) -> List[int]:
  """Returns list of size leaf ids in post order."""
  return [
      node_numeration.leaf2id[leaf] for leaf in root.leaves() if leaf.is_size
  ]


def define_protopath_tree(protopath_tree: ProtopathTreeNodeBase,
                          numeration: NodeNumeration) -> str:
  """Returns C++ function returning tree in form of vector<vector<size_t>>."""
  protopath_nodes = protopath_tree.post_order_nodes()
  # Initialize each element spearately to reduce stack pressure.
  lines = []
  for i, node in enumerate(protopath_nodes):
    if node.is_leaf():
      # avoid adding empty vector to save on binary size
      continue
    children_ids = (str(numeration.node2id[c]) for c in node.children)
    lines.append(f'  tree[{i}] = {{{",".join(children_ids)}}};')
  lines = '\n'.join(lines)

  return f"""[]() {{
  std::vector<std::vector<size_t>> tree({len(protopath_nodes)});
{lines}
  return tree;
}}()"""


def _add_single_value_path_to_tree(
    root: SingleValueProtopathTreeNode,
    path_elements: List[protopath.SingleValueElement]
) -> SingleValueProtopathTreeNode:
  """Adds path of elements to the tree and returns leaf node."""
  node = root
  for i, path_element in enumerate(path_elements):
    next_node = None
    # It is valid case, when exactly the same protopath is requested
    # with different names
    # We are forcing creation of the new node in that case.
    # Last access step will be done twice. It is fine for such a rare case.
    if i + 1 < len(path_elements):
      for child in node.children:
        if child.path_from_parent == path_element:
          next_node = child
          break
    if next_node is None:
      next_node = SingleValueProtopathTreeNode(
          parent=node, path_from_parent=path_element, children=[])
      node.children.append(next_node)
    node = next_node

  parent = node
  while parent is not None:
    if parent.leaf_accessor_name:
      raise ValueError(
          f'Protopath {parent.access_for_type("input")} is prefix for '
          f'{node.access_for_type("input")}.')
    parent = parent.parent

  return node


# We introduce fictive nodes for nodes with too many children.
# By doing that we have benefit of more hierarchy in order to verify whenever
# node is requested.
# Without that in corner case of completely flat proto, we would be checking
# whenever each input was requested independently.
# Such optimization is important, when many inputs are not requested.
_MAX_CHILD_COUNT = 8


def _split_big_nodes(node: TBase) -> TBase:
  """Split nodes with many children by adding fictive nodes."""
  new_children = []
  for child in node.children:
    new_children.append(_split_big_nodes(child))

  if len(new_children) > _MAX_CHILD_COUNT:
    node.children = []
    for group_start in range(0, len(new_children), _MAX_CHILD_COUNT):
      children_slice = new_children[group_start:group_start + _MAX_CHILD_COUNT]
      fictive_node = node.build_fictive_child(children=children_slice)
      for child in children_slice:
        child.parent = fictive_node
      node.children.append(fictive_node)
  else:
    node.children = new_children

  return node


def extract_single_value_protopath_accessors(
    accessors_list: List[Tuple[str, accessors.Accessor]]
) -> Tuple[List[Tuple[str, accessors.Accessor]], SingleValueProtopathTreeNode]:
  """Extracts single value ProtopathAccessor's from the list into the Tree."""
  root = SingleValueProtopathTreeNode(
      parent=None, path_from_parent=None, children=[])
  new_accessors = []
  for name, accessor in accessors_list:
    new_accessors.append((name, accessor))
    if not isinstance(accessor, protopath.ProtopathAccessor):
      continue
    ppath_obj = protopath.Protopath.parse(accessor.protopath)
    if not ppath_obj.is_single_value:
      continue
    del new_accessors[-1]
    leaf = _add_single_value_path_to_tree(root, ppath_obj.last_path_list)
    leaf.is_size = ppath_obj.is_size
    leaf.leaf_accessor_name = name or accessor.default_name
    leaf.leaf_accessor = accessor

  return new_accessors, _split_big_nodes(root)


# approximate cost in nanoseconds for collecting intermediate result
INTERMEDIATE_RESULT_COLLECTION_COST = 1.3


class IntermediateCollectionInfo(enum.IntEnum):
  """Information about intermediate node to collect."""
  # node need sizes collection, parent need collection of all values
  COLLECT_PARENT_VALUES_AND_SIZES = 0
  COLLECT_VALUES = 1  # node need to collect all intermediate values


@dataclasses.dataclass
class MultiValueProtopathTreeNode(ProtopathTreeNodeBase):
  """Tree representing collection of multi value protopathes."""
  # parent node in the tree. None only for the root (/).
  parent: Optional['MultiValueProtopathTreeNode']

  # list of children nodes in the tree
  children: List['MultiValueProtopathTreeNode']

  # protopath element to access from the parent.
  # At most one is not None. Both None for the root (/) and fictive nodes.
  path_from_parent_single: Optional[protopath.SingleValueElement] = None
  path_from_parent_multi: Optional[protopath.MultipleValueElement] = None

  def access_for_type(self, var_name: str) -> str:
    node = self
    path = []
    while node.parent is not None:
      if not node.is_fictive():
        path.append(node)
      node = node.parent
    res = var_name
    for node in reversed(path):
      if node.path_from_parent_single:
        res = node.path_from_parent_single.access(res)
      elif node.path_from_parent_multi:
        res = node.path_from_parent_multi.access_for_type(res)
    return res

  def is_fictive(self) -> bool:
    """Returns true iff node is fictive and doesn't contain any path info."""
    return (self.parent is not None and self.path_from_parent_single is None and
            self.path_from_parent_multi is None)

  def build_fictive_child(
      self, children: List['MultiValueProtopathTreeNode']
  ) -> 'MultiValueProtopathTreeNode':
    """Returns new fictive child for the given node."""
    return MultiValueProtopathTreeNode(parent=self, children=children)

  def repeated_depth(self) -> int:
    """Returns number of repeated accesses in the path to the root."""
    node = self
    result = 0
    while node is not None:
      if node.path_from_parent_multi is not None:
        result += 1
      node = node.parent
    return result

  def is_repeated(self) -> bool:
    """Returns true iff at least one repeated access in the path to the root."""
    return self.repeated_depth() > 0

  def is_repeated_always_present(self, *, is_mutable: bool = False) -> bool:
    """Returns true iff value is never missed after the first repeated node.

    We are collecting intermediate values into vector<T*>. If there are no
    accesses with `not can_continue_access` after the first repeated access
    (aka [:]), we can rely that all pointes are != nullptr.

    Args:
      is_mutable: whenever accessor is mutable.
    """
    assert self.is_repeated()
    node = self
    while node.is_repeated():
      if node.path_from_parent_single is not None and (
          not node.path_from_parent_single.can_continue_on_miss(
              is_mutable=is_mutable)):
        return False
      node = node.parent
      assert node is not None
    return True

  def is_ancestor_of(self, other: 'MultiValueProtopathTreeNode') -> bool:
    """Returns true iff `self` is parent of `other` or `self` == `other`."""
    node = other
    while node is not None:
      if node == self:
        return True
      node = node.parent
    return False

  def path_from_ancestor(
      self, ancestor: 'MultiValueProtopathTreeNode'
  ) -> List['MultiValueProtopathTreeNode']:
    """Returns path from parent till the given node."""
    assert ancestor.is_ancestor_of(self)
    result = []
    node = self
    while node is not None:
      result.append(node)
      if node == ancestor:
        return list(reversed(result))
      node = node.parent  # pytype: disable=bad-return-type  # py310-upgrade

  def non_fictive_ancestor(self) -> 'MultiValueProtopathTreeNode':
    """Returns the first non fictive ancestor in the path to the root."""
    node = self
    while node.is_fictive():
      node = node.parent
      # assert is implied by is_fictive
      assert node is not None
    return node

  def repeated_access_ancestor(self) -> 'MultiValueProtopathTreeNode':
    """Returns the first node with repeated access in the path to the root."""
    node = self
    while node is not None:
      if node.path_from_parent_multi is not None:
        return node
      node = node.parent
    assert False, 'Internal error, all leaves should be repeated'

  def ancestor_with_branch(self) -> 'MultiValueProtopathTreeNode':
    """Returns ancestor with 2 or more children or root."""
    if self.parent is None:
      return self
    node = self.parent
    while node.parent is not None:
      if len(node.children) > 1:
        return node
      node = node.parent
    return node

  def access_cost(self, ancestor) -> float:
    """Returns access cost for single paths from the ancestor."""
    res = 0.0
    node = self
    while node != ancestor:
      if node.path_from_parent_multi is not None:
        # TODO: try not collecting `i[:]` for `i[:]/a` `i[:]/b`.
        raise ValueError(
            f'No repeated access expected in the path {self}->{ancestor}')
      if node.path_from_parent_single is not None:
        res += node.path_from_parent_single.access_cost()
      node = node.parent
    return res

  def intermediate_start_node(
      self) -> Tuple['MultiValueProtopathTreeNode', IntermediateCollectionInfo]:
    """Returns intermediate node from which population should be started.

    Returns:
      Parent node corresponded to the same size with one of the options:
      1. (node, COLLECT_VALUES): array of values should be collected
            and used as starting point.
      2. (node, COLLECT_PARENT_VALUES_AND_SIZES): parent node values should be
         collected (array or single value) and total size of the node.

    Examples of pseudo code that may be generated based
    on the intermediate_start_node.
    Generated code will be split in two phases:
    A) collection of intermediate nodes
    B) collection of actual results

    1. collect protopathes: `a/b[:]/c/d` and `a/b[:]/c/e`.
       `intermediate_start_node` will be (`a/b[:]/c`, COLLECT_VALUES) for both
       protopathes.
       A) ```
         intermediate_a_b_c = []
         for b in input.a.b:
           intermediate_a_b_c.append(b.c)
       ```
       B) ```
         res_d = []
         res_d.reserve(len(intermediate_a_b_c))
         for c in intermediate_a_b_c:
           res_d.append(c.d)
         res_e = []
         res_e.reserve(len(intermediate_a_b_c))
         for c in intermediate_a_b_c:
           res_e.append(c.e)
       ```
    2. collect protopathes: `a/b[:]/c[:]/d` and `a/b[:]/c[:]/e`.
       `intermediate_start_node` will be (`a/b[:]/c[:]`, COLLECT_VALUES)
       for both protopathes.
       A) ```
         intermediate_a_b_c = []
         for b in input.a.b:
           for c in b.c:
             intermediate_a_b_c.append(c)
       ```
       B) ```
         res_d = []
         res_d.reserve(len(intermediate_a_b_c))
         for c in intermediate_a_b_c:
           res_d.append(c.d)
         res_e = []
         res_e.reserve(len(intermediate_a_b_c))
         for c in intermediate_a_b_c:
           res_e.append(c.e)
       ```
    3. collect protopathes: `a/b[:]` and `a/c[:]/d`.
       `intermediate_start_node` will be
       (`a/b[:]`, COLLECT_PARENT_VALUES_AND_SIZES) and
       (`a/c[:]`, COLLECT_PARENT_VALUES_AND_SIZES)
       correspondingly.
       But sizes wouldn't be actually collected, because it is trivial to
       compute them.
       A) ```
         intermediate_a = input.a
       ```
       B) ```
         res_b = []
         res_b.reserve(len(intermediate_a.b))
         for b in intermediate_a.b:
           res_b.append(b)
         res_d = []
         res_d.reserve(len(intermediate_a.c))
         for c in intermediate_a.c:
           res_d.append(c.d)
       ```
    4. collect protopathes: `a/b[:]/c[:]` and `a/b[:]/d[:]`.
       `intermediate_start_node` will be
       (`a/b[:]/c[:]`, COLLECT_PARENT_VALUES_AND_SIZES) and
       (`a/b[:]/d[:]`, COLLECT_PARENT_VALUES_AND_SIZES)
       correspondingly.
       A) ```
         intermediate_b = []
         total_size_c = 0
         total_size_d = 0
         for b in input.a.b:
           intermediate_b.append(b)
           total_size_c += len(b.c)
           total_size_d += len(b.d)
       ```
       B) ```
         res_c = []
         res_c.reserve(total_size_c)
         for b in intermediate_b:
           for c in b.c:
             res_c.append(c)
         res_d = []
         res_d.reserve(total_size_d)
         for b in intermediate_b:
           for d in b.d:
             res_d.append(d)
       ```
    """
    ancestor_with_branch = self.ancestor_with_branch().non_fictive_ancestor()
    node_for_size_computation = self.repeated_access_ancestor(
    ).non_fictive_ancestor()

    if node_for_size_computation.is_ancestor_of(ancestor_with_branch):
      # Considering using previous ancestor in case collecting intermediate
      # result is more expensive than repeating access for each leaf
      # TODO: try going upper the tree. Benchmark on
      # `i[:]/x/y/a`, `i[:]/x/y/b`, `i[:]/x/z`, `i[:]/q`
      # In such situation it may be more efficient to avoid collection of
      # `i[:]/x` and start from `i[:]` even for `i[:]/x/y/{a,b}`
      # It seems also that we may try to search best intermediate nodes for
      # all nodes together oppose to doing it for each independently.
      # In the example above we may use `i[:]` for `i[:]/x/z` to avoid extra
      # extra collection of `i[:]/x`, but still collect `i[:]/x` for
      # `i[:]/x/y/{a,b}`. In that case we are accessin x all the time for
      # no benefit. We may even collect `i[:]` for no good reason.
      next_ancestor = ancestor_with_branch.ancestor_with_branch(
      ).non_fictive_ancestor()
      if next_ancestor.is_ancestor_of(node_for_size_computation):
        next_ancestor = node_for_size_computation

      # Tracing each leaf independently cause going
      # next_ancestor->ancestor_with_branch `len(self.leaves())` times instead
      # of single time.
      path_cost = ancestor_with_branch.access_cost(next_ancestor)
      leaf_count = len(ancestor_with_branch.leaves())
      access_to_all_leaves_extra_cost = path_cost * (leaf_count - 1)

      if access_to_all_leaves_extra_cost < INTERMEDIATE_RESULT_COLLECTION_COST:
        ancestor_with_branch = next_ancestor
      return (ancestor_with_branch, IntermediateCollectionInfo.COLLECT_VALUES)
    else:
      return (node_for_size_computation,
              IntermediateCollectionInfo.COLLECT_PARENT_VALUES_AND_SIZES)

  def protopath(self) -> str:
    """Returns protopath corresponed to this node."""
    res = ''
    node = self
    while node.parent is not None:
      if not node.is_fictive():
        path_from_parent = (
            node.path_from_parent_single or node.path_from_parent_multi)
        # This path should be assigned in _add_single_value_element_to_tree
        # or _add_multi_value_element_to_tree when the tree is constructed
        assert path_from_parent is not None
        res = path_from_parent.protopath_element() + (f'/{res}' if res else '')
      node = node.parent
    return res

  def __hash__(self) -> int:
    return hash(id(self))

  def __eq__(self, other: 'MultiValueProtopathTreeNode') -> bool:
    return id(self) == id(other)

  def __str__(self):
    return f'{type(self).__name__}({self.access_for_type("input")})'

  def __repr__(self):
    return str(self)


def nodes_for_intermediate_collection(
    root: MultiValueProtopathTreeNode
) -> Dict[MultiValueProtopathTreeNode, IntermediateCollectionInfo]:
  """Returns nodes to be collected as intermediate in post order.

  For each leaf up to two nodes may be listed in the dict:
  1. starting node with COLLECT_VALUES (one that may be reused for other nodes).
  2. the deepest ancestor with path_from_parent_multi
     with COLLECT_PARENT_VALUES_AND_SIZES.
     Not created if COLLECT_VALUES node corresponds to the node
     with the same size.
     In such cases parent of this node also added with COLLECT_VALUES.

  Root node is never present in the result.

  See also MultiValueProtopathTreeNode.intermediate_start_node.

  Args:
    root: root of the tree

  Returns:
    Dict from nodes to kind of the collection.
    Note that `dict` guarantee to keep insertion order.
  """
  result_unordered = dict()
  for leaf in root.leaves():
    assert leaf.is_repeated(), 'Internal error, all leaves should be repeated'
    start_node, kind = leaf.intermediate_start_node()
    if kind == IntermediateCollectionInfo.COLLECT_VALUES:
      result_unordered[start_node] = IntermediateCollectionInfo.COLLECT_VALUES
    elif kind == IntermediateCollectionInfo.COLLECT_PARENT_VALUES_AND_SIZES:
      assert start_node.parent is not None
      parent = start_node.parent.non_fictive_ancestor()
      result_unordered[parent] = IntermediateCollectionInfo.COLLECT_VALUES
      # We do not collect sizes for top level repeated fields in advance.
      # it is just a single instruction to compute.
      if parent.is_repeated() and start_node not in result_unordered:
        result_unordered[start_node] = (
            IntermediateCollectionInfo.COLLECT_PARENT_VALUES_AND_SIZES)
    else:
      raise AssertionError(f'Wrong collection info {kind} for {start_node}')
  if root in result_unordered:
    del result_unordered[root]
  result = {}
  for node in root.post_order_nodes():
    if node in result_unordered:
      result[node] = result_unordered[node]
  return result


def nodes_with_descendant_for_intermediate_collection(
    root: MultiValueProtopathTreeNode,
    intermediate_nodes: Dict[MultiValueProtopathTreeNode,
                             IntermediateCollectionInfo]
) -> Dict[MultiValueProtopathTreeNode, IntermediateCollectionInfo]:
  """Returns dict with nodes that has descendant in required_set.

  Args:
    root: root node of the tree
    intermediate_nodes: nodes to be collected

  Returns:
    Dict with nodes that has descendant for collection.
    Value is COLLECT_VALUES iff at least one descendant has COLLECT_VALUES.
    If all descendants has COLLECT_PARENT_VALUES_AND_SIZES,
    value is COLLECT_PARENT_VALUES_AND_SIZES.
  """
  result = {}
  for node in root.post_order_nodes():
    if node in intermediate_nodes:
      result[node] = intermediate_nodes[node]
    for child in node.children:
      if child in result:
        if result[child] == IntermediateCollectionInfo.COLLECT_VALUES:
          result[node] = IntermediateCollectionInfo.COLLECT_VALUES
        elif node not in result:
          result[node] = (
              IntermediateCollectionInfo.COLLECT_PARENT_VALUES_AND_SIZES)
  return result


def _add_single_value_element_to_tree(
    node: MultiValueProtopathTreeNode,
    path_element: protopath.SingleValueElement, *,
    force_create_new: bool) -> MultiValueProtopathTreeNode:
  """Adds path element as a child of the node and returns the child.

  Args:
    node: parent node for the new node
    path_element: path element to add to the edge
    force_create_new: whenever to create new node even if a child with the same
      path_element is already exist.

  Returns:
    A child of the given node with path_element. Existent or newly created.
  """
  next_node = None
  if not force_create_new:
    for child in node.children:
      if child.path_from_parent_single == path_element:
        next_node = child
        break
  if next_node is None:
    next_node = MultiValueProtopathTreeNode(
        parent=node, path_from_parent_single=path_element, children=[])
    node.children.append(next_node)
  return next_node


def _add_multi_value_element_to_tree(
    node: MultiValueProtopathTreeNode,
    path_element: protopath.MultipleValueElement, *,
    force_create_new: bool) -> MultiValueProtopathTreeNode:
  """Adds path element as a child of the node and returns the child.

  Args:
    node: parent node for the new node
    path_element: path element to add to the edge
    force_create_new: whenever to create new node even if a child with the same
      path_element is already exist.

  Returns:
    A child of the given node with path_element. Existent or newly created.
  """
  next_node = None
  if not force_create_new:
    for child in node.children:
      if child.path_from_parent_multi == path_element:
        next_node = child
        break
  if next_node is None:
    next_node = MultiValueProtopathTreeNode(
        parent=node, path_from_parent_multi=path_element, children=[])
    node.children.append(next_node)
  return next_node


def _add_multi_value_path_to_tree(
    root: MultiValueProtopathTreeNode,
    path_elements: List[Tuple[List[protopath.SingleValueElement],
                              protopath.MultipleValueElement]],
    final_path_elements: List[protopath.SingleValueElement]
) -> SingleValueProtopathTreeNode:
  """Adds path of elements to the tree and returns leaf node."""
  node = root
  # It is valid case, when exactly the same protopath is requested
  # with different names.
  # We are forcing creation of the new node in that case.
  # Last access step will be done twice. It is fine for such a rare case.
  for i, (single_elements, multi_element) in enumerate(path_elements):
    for single_element in single_elements:
      node = _add_single_value_element_to_tree(
          node, single_element, force_create_new=False)
    force_create_new = not final_path_elements and (i + 1 == len(path_elements))
    node = _add_multi_value_element_to_tree(
        node, multi_element, force_create_new=force_create_new)

  for i, single_element in enumerate(final_path_elements):
    force_create_new = i + 1 == len(final_path_elements)
    node = _add_single_value_element_to_tree(
        node, single_element, force_create_new=force_create_new)

  parent = node
  while parent is not None:
    if parent.leaf_accessor_name:
      raise ValueError(
          f'Protopath {parent.access_for_type("input")} is prefix for '
          f'{node.access_for_type("input")}.')
    parent = parent.parent

  return node  # pytype: disable=bad-return-type  # py310-upgrade


def extract_multi_value_protopath_accessors(
    accessors_list: List[Tuple[str, accessors.Accessor]]
) -> Tuple[List[Tuple[str, accessors.Accessor]], MultiValueProtopathTreeNode]:
  """Extracts multi value ProtopathAccessor's from the list into the Tree."""
  root = MultiValueProtopathTreeNode(parent=None, children=[])
  new_accessors = []
  for name, accessor in accessors_list:
    new_accessors.append((name, accessor))
    if not isinstance(accessor, protopath.ProtopathAccessor):
      continue
    if accessor.default_value_cpp is not None:
      raise NotImplementedError(
          'Multi value accessor with default is not implemented'
          f' {accessor.protopath}, {accessor.default_value_cpp}'
      )
    ppath_obj = protopath.Protopath.parse(accessor.protopath)
    if ppath_obj.is_single_value:
      continue
    del new_accessors[-1]
    leaf = _add_multi_value_path_to_tree(root, ppath_obj.multi_elems,
                                         ppath_obj.last_path_list)
    leaf.leaf_accessor_name = name or accessor.default_name
    leaf.leaf_accessor = accessor
    leaf.is_size = ppath_obj.is_size

  return new_accessors, _split_big_nodes(root)

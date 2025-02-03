// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include "arolla/util/init_arolla_internal.h"

#include <algorithm>
#include <deque>
#include <sstream>
#include <stack>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::init_arolla_internal {
namespace {

struct Node {
  const Initializer* initializer = nullptr;
  absl::string_view name;
  std::vector<Node*> deps;
  enum { kPending, kExecuting, kDone } execution_state = kPending;
};

struct Digraph {
  std::deque<Node> nodes;
  absl::flat_hash_map<absl::string_view, Node*> node_index;
};

Node* GetNode(Digraph& digraph, absl::string_view name) {
  if (name.empty()) {
    return &digraph.nodes.emplace_back();
  }
  auto& result = digraph.node_index[name];
  if (result == nullptr) {
    result = &digraph.nodes.emplace_back();
    result->name = name;
  }
  return result;
}

absl::StatusOr<Node*> InitNode(
    Digraph& digraph,
    const absl::flat_hash_set<absl::string_view>& previously_completed,
    const Initializer& initializer) {
  if (starts_with(initializer.name, kPhonyNamePrefix)) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "an initializer name may not start with `%s` prefix: '%s'",
        kPhonyNamePrefix, absl::CHexEscape(initializer.name)));
  }
  Node* result = GetNode(digraph, initializer.name);
  if (result->initializer != nullptr ||
      previously_completed.contains(initializer.name)) {
    return absl::FailedPreconditionError(
        absl::StrFormat("name collision between arolla initializers: '%s'",
                        absl::CHexEscape(initializer.name)));
  }
  result->initializer = &initializer;
  for (auto dep : initializer.deps) {
    if (!previously_completed.contains(dep)) {
      result->deps.push_back(GetNode(digraph, dep));
    }
  }
  for (auto reverse_dep : initializer.reverse_deps) {
    if (previously_completed.contains(reverse_dep)) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "the newly registered initializer '%s' expects to be executed "
          "before the previously registered and executed initializer '%s'. "
          "This is likely due to a missing linkage dependency between the "
          "library providing '%s' and the library providing '%s'",
          absl::CHexEscape(initializer.name), absl::CHexEscape(reverse_dep),
          absl::CHexEscape(initializer.name), absl::CHexEscape(reverse_dep)));
    }
    GetNode(digraph, reverse_dep)->deps.push_back(result);
  }
  return result;
}

absl::Status ExecuteNode(Node& node, std::stack<Node*>& dependency_stack) {
  if (node.execution_state == Node::kDone) {
    return absl::OkStatus();
  }
  if (node.execution_state == Node::kExecuting) {
    std::ostringstream message;
    message << "a circular dependency between initializers: '"
            << absl::CHexEscape(node.name) << "'";
    for (;; dependency_stack.pop()) {
      message << " <- '" << absl::CHexEscape(dependency_stack.top()->name)
              << "'";
      if (dependency_stack.top() == &node) {
        break;
      }
    }
    return absl::FailedPreconditionError(std::move(message).str());
  }
  DCHECK_EQ(node.execution_state, Node::kPending);
  node.execution_state = Node::kExecuting;
  std::stable_sort(node.deps.begin(), node.deps.end(),
                   [](auto* lhs, auto* rhs) { return lhs->name < rhs->name; });
  dependency_stack.push(&node);
  for (auto* dep : node.deps) {
    RETURN_IF_ERROR(ExecuteNode(*dep, dependency_stack));
  }
  dependency_stack.pop();
  node.execution_state = Node::kDone;
  if (node.initializer != nullptr) {
    if (auto* init_fn =
            std::get_if<Initializer::VoidInitFn>(&node.initializer->init_fn)) {
      (*init_fn)();
    } else if (auto* init_fn = std::get_if<Initializer::StatusInitFn>(
                   &node.initializer->init_fn)) {
      RETURN_IF_ERROR((*init_fn)()) << "while executing initializer '"
                                    << absl::CHexEscape(node.name) << "'";
    }
  } else if (!starts_with(node.name, kPhonyNamePrefix)) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "the initializer '%s' expects to be executed after the initializer "
        "'%s', which has not been defined yet. This is likely due to a "
        "missing linkage dependency between the library providing '%s' "
        "and the library providing '%s'",
        absl::CHexEscape(dependency_stack.top()->name),
        absl::CHexEscape(node.name),
        absl::CHexEscape(dependency_stack.top()->name),
        absl::CHexEscape(node.name)));
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status Coordinator::Run(
    absl::Span<const Initializer* const> initializers) {
  Digraph digraph;
  std::vector<Node*> nodes;
  nodes.reserve(initializers.size());
  for (auto& initializer : initializers) {
    ASSIGN_OR_RETURN(nodes.emplace_back(),
                     InitNode(digraph, previously_completed_, *initializer));
  }
  std::stable_sort(nodes.begin(), nodes.end(),
                   [](auto* lhs, auto* rhs) { return lhs->name < rhs->name; });
  std::stack<Node*> dependency_stack;
  for (auto* node : nodes) {
    RETURN_IF_ERROR(ExecuteNode(*node, dependency_stack));
    if (!node->name.empty() && !starts_with(node->name, kPhonyNamePrefix)) {
      previously_completed_.emplace(node->name);
    }
  }
  return absl::OkStatus();
}

}  // namespace arolla::init_arolla_internal

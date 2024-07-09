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
#include <cstddef>
#include <sstream>
#include <stack>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::init_arolla_internal {

absl::Status Coordinator::Run(
    absl::Span<const Initializer* const> initializers) {
  std::vector<Node*> new_nodes(initializers.size());
  for (size_t i = 0; i < initializers.size(); ++i) {
    ASSIGN_OR_RETURN(new_nodes[i], InitNode(*initializers[i]));
  }
  std::stable_sort(new_nodes.begin(), new_nodes.end(),
                   [](auto* lhs, auto* rhs) { return lhs->name < rhs->name; });
  std::stack<Node*> dependency_stack;
  for (auto* node : new_nodes) {
    RETURN_IF_ERROR(ExecuteNode(node, dependency_stack));
  }
  return absl::OkStatus();
}

absl::StatusOr<Coordinator::Node*> Coordinator::InitNode(
    const Initializer& initializer) {
  Node* result = GetNode(initializer.name);
  if (result->initializer != nullptr) {
    return absl::FailedPreconditionError(
        absl::StrFormat("name collision between arolla initializers: '%s'",
                        absl::CHexEscape(result->name)));
  }
  result->initializer = &initializer;
  for (absl::string_view dep : absl::StrSplit(
           initializer.deps, absl::ByAnyChar(", "), absl::SkipEmpty())) {
    result->deps.push_back(GetNode(dep));
  }
  for (absl::string_view reverse_dep :
       absl::StrSplit(initializer.reverse_deps, absl::ByAnyChar(", "),
                      absl::SkipEmpty())) {
    auto* reverse_node = GetNode(reverse_dep);
    if (reverse_node->execution_state == Node::kDone) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "the newly registered initializer '%s' expects to be executed "
          "before the previously registered and executed initializer '%s'. "
          "This is likely due to a missing linkage dependency between the "
          "library providing '%s' and the library providing '%s'",
          absl::CHexEscape(initializer.name), absl::CHexEscape(reverse_dep),
          absl::CHexEscape(initializer.name), absl::CHexEscape(reverse_dep)));
    }
    DCHECK_EQ(reverse_node->execution_state, Node::kPending);
    reverse_node->deps.push_back(result);
  }
  return result;
}

Coordinator::Node* Coordinator::GetNode(absl::string_view name) {
  if (name.empty()) {
    return &nodes_.emplace_back();
  }
  auto& result = nodes_index_[name];
  if (result == nullptr) {
    result = &nodes_.emplace_back();
    result->name = name;
  }
  return result;
}

absl::Status Coordinator::ExecuteNode(Node* node,
                                      std::stack<Node*>& dependency_stack) {
  if (node->initializer == nullptr) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "the initializer '%s' expects to be executed after the initializer "
        "'%s', which has not been defined yet. This is likely due to a "
        "missing linkage dependency between the library providing '%s' "
        "and the library providing '%s'",
        absl::CHexEscape(node->name),
        absl::CHexEscape(dependency_stack.top()->name),
        absl::CHexEscape(node->name),
        absl::CHexEscape(dependency_stack.top()->name)));
  }
  if (node->execution_state == Node::kDone) {
    return absl::OkStatus();
  }
  if (node->execution_state == Node::kExecuting) {
    std::ostringstream message;
    message << "a circular dependency between initializers: '"
            << absl::CHexEscape(node->name) << "'";
    for (;; dependency_stack.pop()) {
      message << " <- '" << absl::CHexEscape(dependency_stack.top()->name)
              << "'";
      if (dependency_stack.top() == node) {
        break;
      }
    }
    return absl::FailedPreconditionError(std::move(message).str());
  }
  DCHECK_EQ(node->execution_state, Node::kPending);
  node->execution_state = Node::kExecuting;
  std::stable_sort(node->deps.begin(), node->deps.end(),
                   [](auto* lhs, auto* rhs) { return lhs->name < rhs->name; });
  dependency_stack.push(node);
  for (auto* dep : node->deps) {
    RETURN_IF_ERROR(ExecuteNode(dep, dependency_stack));
  }
  dependency_stack.pop();
  node->execution_state = Node::kDone;
  if (auto* init_fn =
          std::get_if<Initializer::VoidInitFn>(&node->initializer->init_fn)) {
    (*init_fn)();
  } else if (auto* init_fn = std::get_if<Initializer::StatusInitFn>(
                 &node->initializer->init_fn)) {
    RETURN_IF_ERROR((*init_fn)()) << "while executing initializer '"
                                  << absl::CHexEscape(node->name) << "'";
  }
  return absl::OkStatus();
}

}  // namespace arolla::init_arolla_internal

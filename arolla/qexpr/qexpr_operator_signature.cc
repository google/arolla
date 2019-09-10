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
#include "arolla/qexpr/qexpr_operator_signature.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/const_init.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/indestructible.h"

namespace arolla {
namespace {

std::string MakeSignatureName(absl::Span<const QTypePtr> input_types,
                              QTypePtr output_type) {
  return absl::StrFormat("%s->%s", FormatTypeVector(input_types),
                         JoinTypeNames({output_type}));
}

}  // namespace

QExprOperatorSignature::QExprOperatorSignature(
    PrivateConstructoTag, absl::Span<const QTypePtr> input_qtypes,
    QTypePtr output_qtype)
    : name_(MakeSignatureName(input_qtypes, output_qtype)),
      input_qtypes_(input_qtypes.begin(), input_qtypes.end()),
      output_qtype_(output_qtype) {}

const QExprOperatorSignature* QExprOperatorSignature::Get(
    absl::Span<const QTypePtr> input_qtypes, const QTypePtr output_qtype) {
  using FnOperatorRegistryKey = std::pair<std::vector<QTypePtr>, QTypePtr>;
  static absl::Mutex lock(absl::kConstInit);
  static Indestructible<absl::flat_hash_map<
      FnOperatorRegistryKey, std::unique_ptr<QExprOperatorSignature>>>
      index;
  absl::MutexLock l(&lock);
  auto [iter, inserted] = index->insert(
      {FnOperatorRegistryKey(
           std::vector<QTypePtr>(input_qtypes.begin(), input_qtypes.end()),
           output_qtype),
       nullptr});
  if (inserted) {
    iter->second = std::make_unique<QExprOperatorSignature>(
        PrivateConstructoTag{}, input_qtypes, output_qtype);
  }
  return iter->second.get();
}

}  // namespace arolla

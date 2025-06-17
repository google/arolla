// Copyright 2025 Google LLC
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
#include "arolla/derived_qtype/labeled_qtype.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/string.h"

namespace arolla {
namespace {

std::string EscapeLabel(absl::string_view label) {
  if (IsQualifiedIdentifier(label)) {
    return std::string(label);
  } else {
    return absl::StrCat("'", absl::Utf8SafeCHexEscape(label), "'");
  }
}

std::string MakeQTypeName(absl::string_view label) {
  return absl::StrCat("LABEL[", EscapeLabel(label), "]");
}

class LabeledQType final : public BasicDerivedQType {
 public:
  LabeledQType(QTypePtr absl_nonnull base_qtype, absl::string_view label)
      : BasicDerivedQType(ConstructorArgs{
            .name = MakeQTypeName(label),
            .base_qtype = base_qtype,
            .qtype_specialization_key =
                std::string(GetLabeledQTypeSpecializationKey()),
        }),
        label_(label) {}

  absl::string_view label() const { return label_; }

 private:
  std::string label_;
};

class LabeledQTypesRegistry {
 public:
  const LabeledQType* absl_nonnull Get(QTypePtr absl_nonnull base_qtype,
                                        absl::string_view label)
      ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    auto it = registry_.find(RegistryKey{base_qtype, label});
    if (it == registry_.end()) {
      auto labeled_qtype = std::make_unique<LabeledQType>(base_qtype, label);
      it = registry_
               .emplace(RegistryKey{labeled_qtype->GetBaseQType(),
                                    labeled_qtype->label()},
                        std::move(labeled_qtype))
               .first;
    }
    return it->second.get();
  }

 private:
  using RegistryKey = std::pair<QTypePtr absl_nonnull, std::string>;
  using Registry =
      absl::flat_hash_map<RegistryKey,
                          std::unique_ptr<LabeledQType> absl_nonnull>;

  absl::Mutex mutex_;
  Registry registry_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace

bool IsLabeledQType(QTypePtr absl_nullable qtype) {
  return fast_dynamic_downcast_final<const LabeledQType*>(qtype) != nullptr;
}

QTypePtr absl_nonnull GetLabeledQType(QTypePtr absl_nonnull qtype,
                                       absl::string_view label) {
  static absl::NoDestructor<LabeledQTypesRegistry> registry;
  auto* base_qtype = DecayDerivedQType(qtype);
  if (label.empty()) {
    return base_qtype;
  }
  return registry->Get(base_qtype, label);
}

absl::string_view GetQTypeLabel(QTypePtr absl_nullable qtype) {
  if (auto* labeled_qtype =
          fast_dynamic_downcast_final<const LabeledQType*>(qtype)) {
    return labeled_qtype->label();
  }
  return absl::string_view{};
}

absl::string_view GetLabeledQTypeSpecializationKey() {
  return "::arolla::LabeledQType";
}

}  // namespace arolla

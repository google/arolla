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
#include "arolla/sequence/sequence_qtype.h"

#include <memory>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/sequence/sequence.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/meta.h"

namespace arolla {
namespace {

class SequenceQType final : public SimpleQType {
 public:
  explicit SequenceQType(QTypePtr value_qtype)
      : SimpleQType(meta::type<Sequence>(),
                    "SEQUENCE[" + std::string(value_qtype->name()) + "]",
                    value_qtype,
                    /*qtype_specialization_key=*/"::arolla::SequenceQType") {}
};

class SequenceQTypeRegistry {
 public:
  QTypePtr GetSequenceQType(QTypePtr value_qtype) {
    absl::WriterMutexLock l(&lock_);
    auto& result = registry_[value_qtype];
    if (!result) {
      result = std::make_unique<SequenceQType>(value_qtype);
    }
    return result.get();
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<QTypePtr, std::unique_ptr<SequenceQType>> registry_
      ABSL_GUARDED_BY(lock_);
};

}  // namespace

bool IsSequenceQType(const QType* qtype) {
  return fast_dynamic_downcast_final<const SequenceQType*>(qtype) != nullptr;
}

QTypePtr GetSequenceQType(QTypePtr value_qtype) {
  static absl::NoDestructor<SequenceQTypeRegistry> registry;
  return registry->GetSequenceQType(value_qtype);
}

}  // namespace arolla

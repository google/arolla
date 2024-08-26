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
#include "arolla/qtype/dict/dict_types.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

// TODO: make a common class for thread-safe QType dict.
class KeyToRowDictTypeRegistry {
 public:
  static KeyToRowDictTypeRegistry& instance() {
    static absl::NoDestructor<KeyToRowDictTypeRegistry> result;
    return *result;
  }

  absl::Status Register(QTypePtr key_qtype, QTypePtr dict_qtype) {
    absl::MutexLock l(&lock_);
    auto [iter, inserted] = dict_types_.emplace(key_qtype, dict_qtype);
    if (!inserted) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "attempt to register %s dict twice", dict_qtype->name()));
    }
    return absl::OkStatus();
  }

  absl::StatusOr<QTypePtr> Get(QTypePtr qtype) {
    absl::ReaderMutexLock l(&lock_);
    auto iter = dict_types_.find(qtype);
    if (iter == dict_types_.end()) {
      return absl::NotFoundError(
          absl::StrFormat("no dict with %s keys found", qtype->name()));
    }
    return iter->second;
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<QTypePtr, QTypePtr> dict_types_ ABSL_GUARDED_BY(lock_);
};

// QType for Dict<key_type, value_type>.
class DictQType final : public BasicDerivedQType {
 public:
  DictQType(std::string name, QTypePtr dict_type, QTypePtr values_array_type)
      : BasicDerivedQType(ConstructorArgs{
            .name = std::move(name),
            .base_qtype = MakeTupleQType({dict_type, values_array_type}),
            .qtype_specialization_key = "::arolla::DictQType",
        }) {}
};

// Thread-safe mapping [key_qtype, value_qtype] -> KeyToRowDict[key_qtype,
// value_qtype].
// TODO: TupleQTypeRegistry is very similar. Share the code?
class DictQTypeRegistry {
 public:
  static DictQTypeRegistry& instance() {
    static absl::NoDestructor<DictQTypeRegistry> result;
    return *result;
  }

  absl::StatusOr<QTypePtr> GetQType(QTypePtr key_type, QTypePtr value_type) {
    {
      absl::ReaderMutexLock guard(&lock_);
      if (const auto it = registry_.find({key_type, value_type});
          it != registry_.end()) {
        return it->second.get();
      }
    }
    ASSIGN_OR_RETURN(QTypePtr dict_type, GetKeyToRowDictQType(key_type));
    ASSIGN_OR_RETURN(QTypePtr values_array_type,
                     GetDenseArrayQTypeByValueQType(value_type));
    auto kv_dict_type = std::make_unique<DictQType>(
        absl::StrFormat("Dict<%s,%s>", key_type->name(), value_type->name()),
        dict_type, values_array_type);
    absl::MutexLock guard(&lock_);
    return registry_
        .emplace(std::make_pair(key_type, value_type), std::move(kv_dict_type))
        // In case if the type was already created between ReaderMutexLock
        // release and MutexLock acquisition, we just discard the newly created
        // one and return the already inserted.
        .first->second.get();
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<std::pair<QTypePtr, QTypePtr>, std::unique_ptr<QType>>
      registry_ ABSL_GUARDED_BY(lock_);
};

}  // namespace

namespace dict_impl {
void RegisterKeyToRowDictQType(QTypePtr key_type, QTypePtr dict_type) {
  auto status =
      KeyToRowDictTypeRegistry::instance().Register(key_type, dict_type);
  DCHECK_OK(status);
}
}  // namespace dict_impl

absl::StatusOr<QTypePtr> GetKeyToRowDictQType(QTypePtr key_type) {
  return KeyToRowDictTypeRegistry::instance().Get(key_type);
}

bool IsKeyToRowDictQType(QTypePtr type) {
  if (type->value_qtype() == nullptr) {
    return false;
  }
  ASSIGN_OR_RETURN(QTypePtr dict_type,
                   GetKeyToRowDictQType(type->value_qtype()), false);
  return dict_type == type;
}

absl::StatusOr<QTypePtr> GetDictQType(QTypePtr key_type, QTypePtr value_type) {
  return DictQTypeRegistry::instance().GetQType(key_type, value_type);
}

const QType* /*nullable*/ GetDictKeyQTypeOrNull(QTypePtr dict_type) {
  auto d = fast_dynamic_downcast_final<const DictQType*>(dict_type);
  return d != nullptr ? d->type_fields()[0].GetType()->value_qtype() : nullptr;
}

const QType* /*nullable*/ GetDictValueQTypeOrNull(QTypePtr dict_type) {
  auto d = fast_dynamic_downcast_final<const DictQType*>(dict_type);
  return d != nullptr ? d->type_fields()[1].GetType()->value_qtype() : nullptr;
}

bool IsDictQType(const QType* /*nullable*/ qtype) {
  return fast_dynamic_downcast_final<const DictQType*>(qtype) != nullptr;
}

// Explicitly instantiate dict QTypes to avoid including them into each
// translation unit and reduce linker inputs size.
template struct QTypeTraits<KeyToRowDict<bool>>;
template struct QTypeTraits<KeyToRowDict<int32_t>>;
template struct QTypeTraits<KeyToRowDict<int64_t>>;
template struct QTypeTraits<KeyToRowDict<Bytes>>;
template struct QTypeTraits<KeyToRowDict<Text>>;

}  // namespace arolla

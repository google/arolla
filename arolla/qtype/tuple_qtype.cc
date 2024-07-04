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
#include "arolla/qtype/tuple_qtype.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

// A private C++ type for TupleQType::type_info().
class Tuple {};

class TupleQType final : public QType {
 public:
  static std::unique_ptr<TupleQType> Make(
      absl::Span<const QTypePtr> field_qtypes) {
    // TODO: Consider reordering the fields so all trivially
    // copyable fields form a continuous region.
    // TODO: Consider reordering the fields according to the
    // alignment & size to make it more compact.
    FrameLayout::Builder layout_builder;
    std::vector<TypedSlot> fields;
    fields.reserve(field_qtypes.size());
    for (auto field_qtype : field_qtypes) {
      fields.push_back(AddSlot(field_qtype, &layout_builder));
    }
    // It could be that a field located at offset=0, starts with a Tuple.
    // In such case, we don't need register our Tuple field tag, because
    // there is already one.
    bool needTupleTag = true;
    for (const auto& field : fields) {
      if (field.byte_offset() == 0 &&
          field.GetType()->type_layout().HasField(0, typeid(Tuple))) {
        needTupleTag = false;
        break;
      }
    }
    if (needTupleTag) {
      auto status = layout_builder.RegisterUnsafeSlot(0, 0, typeid(Tuple));
      if (!status.ok()) {
        // Always successful, because we has just checked that there is no
        // collision with fields.
        LOG(FATAL) << status;
      }
    }
    return std::make_unique<TupleQType>(
        field_qtypes, std::move(layout_builder).Build(), std::move(fields));
  }

  TupleQType(absl::Span<const QTypePtr> field_qtypes, FrameLayout&& layout,
             std::vector<TypedSlot>&& fields)
      : QType(ConstructorArgs{
            .name = absl::StrCat("tuple<", JoinTypeNames(field_qtypes), ">"),
            .type_info = typeid(Tuple),
            .type_layout = std::move(layout),
            .type_fields = std::move(fields),
            .qtype_specialization_key = "::arolla::TupleQType",
        }),
        field_qtypes_(field_qtypes.begin(), field_qtypes.end()) {}

  absl::Span<const QTypePtr> field_qtypes() const { return field_qtypes_; }

  void UnsafeCopy(const void* source, void* destination) const override {
    // TODO: Consider using memcpy() for trivially copyable
    // fields.
    // TODO: FramePtr interface doesn't provide extra safety
    // here. Consider using QType::Copy() directly.
    ConstFramePtr source_frame(source, &type_layout());
    FramePtr destination_frame(destination, &type_layout());
    for (const auto& field : type_fields()) {
      field.CopyTo(source_frame, field, destination_frame);
    }
  }

  void UnsafeCombineToFingerprintHasher(
      const void* source, FingerprintHasher* hasher) const override {
    hasher->Combine(type_fields().size());
    for (const auto& field : type_fields()) {
      field.GetType()->UnsafeCombineToFingerprintHasher(
          static_cast<const char*>(source) + field.byte_offset(), hasher);
    }
  }

  ReprToken UnsafeReprToken(const void* source) const override {
    ConstFramePtr frame_ptr(source, &type_layout());
    std::ostringstream result;
    result << "(";
    bool first = true;
    for (const auto& field : type_fields()) {
      result << NonFirstComma(first)
             << TypedRef::FromSlot(field, frame_ptr).Repr();
    }
    result << ")";
    return ReprToken{std::move(result).str()};
  }

 private:
  std::vector<QTypePtr> field_qtypes_;
};

// Registry of TupleQTypes that provides a guarantee that each qtype is a
// singleton.
class TupleQTypeRegistry {
 public:
  static TupleQTypeRegistry* instance() {
    static Indestructible<TupleQTypeRegistry> result;
    return result.get();
  }

  QTypePtr GetQType(absl::Span<const QTypePtr> field_qtypes)
      ABSL_LOCKS_EXCLUDED(lock_) {
    {  // Fast look-up without memory allocation.
      absl::ReaderMutexLock guard(&lock_);
      if (const auto it = registry_.find(field_qtypes); it != registry_.end()) {
        return it->second.get();
      }
    }
    auto tuple_qtype = TupleQType::Make(field_qtypes);
    absl::MutexLock guard(&lock_);
    return registry_
        .try_emplace(tuple_qtype->field_qtypes(), std::move(tuple_qtype))
        .first->second.get();
  }

 private:
  absl::Mutex lock_;
  // NOTE: The map's keys are owned by the (TupleQType) values.
  absl::flat_hash_map<absl::Span<const QTypePtr>, std::unique_ptr<TupleQType>>
      registry_ ABSL_GUARDED_BY(lock_);
};

template <typename T /* = TypedRef | TypedValue*/>
TypedValue MakeTupleImpl(absl::Span<const T> fields) {
  std::vector<QTypePtr> field_types;
  field_types.reserve(fields.size());
  for (const auto& field : fields) {
    field_types.push_back(field.GetType());
  }

  // FromFields should never fail because the tuple qtype matches the value
  // types.
  auto status_or_result =
      TypedValue::FromFields(MakeTupleQType(field_types), fields);
  DCHECK_OK(status_or_result.status());
  return status_or_result.value_or(TypedValue::FromValue(Unit{}));
}

template <typename T /* = TypedRef | TypedValue*/>
absl::StatusOr<TypedValue> MakeNamedTupleImpl(
    absl::Span<const std::string> field_names, absl::Span<const T> fields) {
  std::vector<QTypePtr> field_qtypes;
  field_qtypes.reserve(fields.size());
  for (const auto& field : fields) {
    field_qtypes.push_back(field.GetType());
  }
  ASSIGN_OR_RETURN(
      auto named_tuple_qtype,
      MakeNamedTupleQType(field_names, MakeTupleQType(field_qtypes)));

  // FromFields should never fail because the tuple qtype matches the value
  // types.
  absl::StatusOr<TypedValue> result =
      TypedValue::FromFields(named_tuple_qtype, fields);
  DCHECK_OK(result.status());
  return std::move(result).value_or(TypedValue::FromValue(Unit{}));
}

// Returns name of the NamedTupleQType from constructor arguments.
std::string NamedTupleQTypeName(absl::Span<const std::string> field_names,
                                QTypePtr tuple_qtype) {
  constexpr size_t kMaxFieldNames = 5;
  std::ostringstream o;
  o << "namedtuple<";
  size_t fields_to_report = std::min(field_names.size(), kMaxFieldNames);
  for (size_t i = 0; i != fields_to_report; ++i) {
    if (i != 0) {
      o << ",";
    }
    o << field_names[i] << "="
      << tuple_qtype->type_fields()[i].GetType()->name();
  }
  if (fields_to_report < field_names.size()) {
    o << ", [" << field_names.size() - fields_to_report << " fields]";
  }
  o << ">";
  return o.str();
}

class NamedTupleQType final : public BasicDerivedQType,
                              public NamedFieldQTypeInterface {
 public:
  NamedTupleQType(absl::Span<const std::string> field_names,
                  QTypePtr tuple_qtype)
      : BasicDerivedQType(ConstructorArgs{
            .name = NamedTupleQTypeName(field_names, tuple_qtype),
            .base_qtype = tuple_qtype,
            .qtype_specialization_key = "::arolla::NamedTupleQType",
        }),
        field_names_(field_names.begin(), field_names.end()) {
    name2index_.reserve(field_names.size());
    int64_t id = 0;
    for (const std::string& name : field_names_) {
      name2index_.emplace(name, id++);
    }
  }

  // Returns list of the field names.
  absl::Span<const std::string> GetFieldNames() const final {
    return field_names_;
  }

  // Returns field index by the given name or `nullopt` if name is not present.
  std::optional<int64_t> GetFieldIndexByName(
      absl::string_view field_name) const final {
    if (auto it = name2index_.find(field_name); it != name2index_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

 private:
  absl::flat_hash_map<absl::string_view, int64_t> name2index_;
  std::vector<std::string> field_names_;
};

// Registry of NamedTupleQTypes that provides a guarantee that each qtype is a
// singleton.
class NamedTupleQTypeRegistry {
 public:
  static NamedTupleQTypeRegistry* instance() {
    static Indestructible<NamedTupleQTypeRegistry> result;
    return result.get();
  }

  QTypePtr GetQType(absl::Span<const std::string> field_names,
                    QTypePtr tuple_qtype) ABSL_LOCKS_EXCLUDED(lock_) {
    {  // Fast look-up without memory allocation.
      absl::ReaderMutexLock guard(&lock_);
      if (const auto it = registry_.find({field_names, tuple_qtype});
          it != registry_.end()) {
        return it->second.get();
      }
    }
    auto named_tuple_qtype =
        std::make_unique<NamedTupleQType>(field_names, tuple_qtype);
    absl::MutexLock guard(&lock_);
    return registry_
        .try_emplace({named_tuple_qtype->GetFieldNames(), tuple_qtype},
                     std::move(named_tuple_qtype))
        .first->second.get();
  }

 private:
  using RegistryKey = std::pair<absl::Span<const std::string>, QTypePtr>;

  absl::Mutex lock_;
  // NOTE: The map's keys are owned by the (NamedTupleQType) values.
  absl::flat_hash_map<RegistryKey, std::unique_ptr<NamedTupleQType>> registry_
      ABSL_GUARDED_BY(lock_);
};

}  // namespace

bool IsTupleQType(const QType* /*nullable*/ qtype) {
  return fast_dynamic_downcast_final<const TupleQType*>(qtype) != nullptr;
}

QTypePtr MakeTupleQType(absl::Span<const QTypePtr> field_qtypes) {
  return TupleQTypeRegistry::instance()->GetQType(field_qtypes);
}

TypedValue MakeTuple(absl::Span<const TypedRef> fields) {
  return MakeTupleImpl(fields);
}

TypedValue MakeTuple(absl::Span<const TypedValue> fields) {
  return MakeTupleImpl(fields);
}

absl::StatusOr<TypedValue> MakeNamedTuple(
    absl::Span<const std::string> field_names,
    absl::Span<const TypedRef> fields) {
  return MakeNamedTupleImpl(field_names, fields);
}

absl::StatusOr<TypedValue> MakeNamedTuple(
    absl::Span<const std::string> field_names,
    absl::Span<const TypedValue> fields) {
  return MakeNamedTupleImpl(field_names, fields);
}

bool IsNamedTupleQType(const QType* /*nullable*/ qtype) {
  return fast_dynamic_downcast_final<const NamedTupleQType*>(qtype) != nullptr;
}

absl::StatusOr<QTypePtr> MakeNamedTupleQType(
    absl::Span<const std::string> field_names, QTypePtr tuple_qtype) {
  if (!IsTupleQType(tuple_qtype)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "incorrect NamedTupleQType: expected tuple, found %s",
        tuple_qtype != nullptr ? tuple_qtype->name() : std::string("nullptr")));
  }
  if (field_names.size() != tuple_qtype->type_fields().size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "incorrect NamedTupleQType #field_names != #fields: %d vs %d",
        field_names.size(), tuple_qtype->type_fields().size()));
  }
  absl::flat_hash_set<absl::string_view> name_set;
  for (const std::string& name : field_names) {
    if (!name_set.insert(name).second) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "incorrect NamedTupleQType: field name %s is duplicated", name));
    }
  }
  return NamedTupleQTypeRegistry::instance()->GetQType(field_names,
                                                       tuple_qtype);
}

}  // namespace arolla

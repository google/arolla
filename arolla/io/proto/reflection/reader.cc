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
#include "arolla/io/proto/reflection/reader.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/types/span.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/reflection.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/memory/buffer.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace {

using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::Reflection;

using ::absl::StatusOr;
using ::arolla::Bytes;
using ::arolla::FrameLayout;
using ::arolla::FramePtr;
using ::arolla::GetDenseArrayQType;
using ::arolla::GetOptionalQType;
using ::arolla::GetQType;
using ::arolla::OptionalValue;
using ::arolla::QTypePtr;
using ::arolla::Text;
using ::arolla::TypedSlot;

using ::arolla::DenseArrayShape;
using ::arolla::proto::arolla_size_t;
using ::arolla::proto::ProtoFieldAccessInfo;
using ::arolla::proto::ProtoTypeReader;
using ::arolla::proto::RegularFieldAccess;
using ::arolla::proto::RepeatedFieldAccess;
using ::arolla::proto::RepeatedFieldIndexAccess;
using ::arolla::proto::RepeatedFieldSizeAccess;
using ::arolla::proto::StringFieldType;

// Type erased function for setting value read from the message.
// We are using void* to reduce number of types and template expansion
// and as a result binary size.
using ReadValueFn = std::function<void(const Message&, void*)>;

// ReadValueFn to read value by index from repeated field to OptionalValue<T>.
template <class T, class ProtoGetFn>
struct ByIndexReader {
  void operator()(const Message& m, void* res_void) const {
    auto* res = reinterpret_cast<OptionalValue<T>*>(res_void);
    // NO_CDC: reflection based library
    const auto* ref = m.GetReflection();
    if (access_info.idx < ref->FieldSize(m, field)) {
      *res = static_cast<T>(
          getter.GetFromRepeated(ref, m, field, access_info.idx));
    } else {
      *res = std::nullopt;
    }
  }

  const FieldDescriptor* field;
  RepeatedFieldIndexAccess access_info;
  ProtoGetFn getter;
};

// ReadValueFn to read value from a regular field to OptionalValue<T>.
template <class T, class ProtoGetFn>
struct FieldReader {
  void operator()(const Message& m, void* res_void) const {
    auto* res = reinterpret_cast<OptionalValue<T>*>(res_void);
    // NO_CDC: reflection based library
    const auto* ref = m.GetReflection();
    if (ref->HasField(m, field)) {
      *res = static_cast<T>(getter.GetSingular(ref, m, field));
    } else {
      *res = std::nullopt;
    }
  }

  const FieldDescriptor* field;
  ProtoGetFn getter;
};

// Type erased function for adding values into the vector.
// We are using void* to reduce number of types and template expansion
// and as a result binary size.
using PushbackFn = std::function<void(const Message&, void*)>;

// PushbackFn pushing several values from repeated field to the
// std::vector<OptionalValue<T>>.
template <class T, class ProtoGetFn>
struct ManyPushBackFn {
  void operator()(const Message& m, void* res_void) const {
    auto* res = reinterpret_cast<std::vector<OptionalValue<T>>*>(res_void);
    // NO_CDC: reflection based library
    const auto* ref = m.GetReflection();
    for (const auto& val : getter.GetRepeated(ref, m, field)) {
      res->push_back(static_cast<T>(val));
    }
  }
  const FieldDescriptor* field;
  ProtoGetFn getter;
};

// PushbackFn pushing size of the message into std::vector<arolla_size_t>.
struct SizePushBackFn {
  void operator()(const Message& m, void* res_void) const {
    auto* res = reinterpret_cast<std::vector<arolla_size_t>*>(res_void);
    // NO_CDC: reflection based library
    const auto* ref = m.GetReflection();
    res->push_back(ref->FieldSize(m, field));
  }
  const FieldDescriptor* field;
};

// SizeToShapeFn setting size of the message into DenseArrayShape.
struct SizeToShapeFn {
  void operator()(const Message& m, void* res_void) const {
    auto* res = reinterpret_cast<DenseArrayShape*>(res_void);
    // NO_CDC: reflection based library
    const auto* ref = m.GetReflection();
    res->size = ref->FieldSize(m, field);
  }
  const FieldDescriptor* field;
};

// PushbackFn pushing single element into std::vector<OptionalValue<ResultT>>.
template <class ResultT>
struct SinglePushBackFn {
  void operator()(const Message& m, void* res_void) const {
    auto* res =
        reinterpret_cast<std::vector<OptionalValue<ResultT>>*>(res_void);
    res->emplace_back();
    get_fn(m, &res->back());
  }
  ReadValueFn get_fn;
};

// The type name is not used in any public interactions, so we choose
// to make it short to slightly reduce binary size, since these structs are
// used as template parameter in quite a few functions and classes.
#define PROTO_GETTER_OBJ(TYPE, CPP_TYPE)                                  \
  struct PFG##TYPE {                                                      \
    auto GetSingular(const Reflection* ref, const Message& m,             \
                     const FieldDescriptor* field) const {                \
      return ref->Get##TYPE(m, field);                                    \
    }                                                                     \
    auto GetFromRepeated(const Reflection* ref, const Message& m,         \
                         const FieldDescriptor* field, int index) const { \
      return ref->GetRepeated##TYPE(m, field, index);                     \
    }                                                                     \
    auto GetRepeated(const Reflection* ref, const Message& m,             \
                     const FieldDescriptor* field) const {                \
      return ref->GetRepeatedFieldRef<CPP_TYPE>(m, field);                \
    }                                                                     \
  };                                                                      \
  constexpr auto kProtoGetter##TYPE = PFG##TYPE{};

PROTO_GETTER_OBJ(Int32, int32_t);
PROTO_GETTER_OBJ(Int64, int64_t);
PROTO_GETTER_OBJ(UInt32, uint32_t);
PROTO_GETTER_OBJ(UInt64, uint64_t);
PROTO_GETTER_OBJ(Float, float);
PROTO_GETTER_OBJ(Double, double);
PROTO_GETTER_OBJ(Bool, bool);
PROTO_GETTER_OBJ(String, std::string);
PROTO_GETTER_OBJ(EnumValue, int32_t);

#undef PROTO_GETTER_OBJ

absl::Status CheckAccessInfo(const FieldDescriptor* field,
                             const ProtoFieldAccessInfo& info,
                             bool allow_repeated, bool is_last) {
  if (field == nullptr) {
    return absl::FailedPreconditionError(
        "field is nullptr (incorrect name passed into FindFieldByName?)");
  }
  if (field->is_repeated()) {
    if (std::holds_alternative<RepeatedFieldIndexAccess>(info)) {
      return absl::OkStatus();
    }
    if (allow_repeated && std::holds_alternative<RepeatedFieldAccess>(info)) {
      return absl::OkStatus();
    }
    if (allow_repeated && is_last &&
        std::holds_alternative<RepeatedFieldSizeAccess>(info)) {
      return absl::OkStatus();
    }
    return absl::FailedPreconditionError(absl::StrCat(
        "incorrect access to the repeated field: ", field->full_name()));
  } else {
    if (!std::holds_alternative<RegularFieldAccess>(info)) {
      return absl::FailedPreconditionError(absl::StrCat(
          "incorrect access to the regular field: ", field->full_name()));
    }
  }
  return absl::OkStatus();
}

// Helper class for traversing proto by provided intermediate fields and
// access information.
class Traverser {
 public:
  Traverser(std::vector<const FieldDescriptor*> fields,
            std::vector<ProtoFieldAccessInfo> access_infos)
      : fields_(std::move(fields)), access_infos_(std::move(access_infos)) {
    DCHECK_EQ(fields_.size(), access_infos_.size());
  }

  // Resolve intermediate submessages and return the last one.
  // nullptr is returned if any of the submessages are missed.
  const Message* GetLastSubMessage(const Message& m) const {
    const Message* current_message = &m;
    for (size_t i = 0; i != fields_.size(); ++i) {
      current_message = GetSubMessage(*current_message, i);
      if (current_message == nullptr) {
        return nullptr;
      }
    }
    return current_message;
  }

  // Call a callback for every last element after processing
  // intermediate_fields.
  // res is a pointer to the vector, which will be passed to the callback.
  // callback and res must be in sync. Type checking is not happen on that level
  // to significantly reduce binary size.
  void TraverseSubmessages(const Message& m, PushbackFn callback,
                           void* res) const {
    using IndexedMessage = std::pair<const Message*,
                                     size_t>;  // layer depth of the message
    // Non recursive implementation of the fields traversing, aka DFS.
    std::vector<IndexedMessage> stack;
    stack.emplace_back(&m, 0);
    while (!stack.empty()) {
      auto [current_message, i] = stack.back();
      stack.pop_back();
      if (i != fields_.size()) {
        size_t end_id = stack.size();
        const auto* field = fields_[i];
        const auto& access_info = access_infos_[i];
        DCHECK(!std::holds_alternative<RepeatedFieldSizeAccess>(access_info));
        if (std::holds_alternative<RepeatedFieldAccess>(access_info)) {
          // NO_CDC: reflection based library
          const auto* ref = m.GetReflection();
          for (const Message& sub_message :
               ref->GetRepeatedFieldRef<Message>(m, field)) {
            stack.emplace_back(&sub_message, i + 1);
          }
        } else {
          stack.emplace_back(GetSubMessage(m, i), i + 1);
        }
        // Reverse just added stack elements since we use stack and need to
        // process elements in the regular order.
        std::reverse(stack.begin() + end_id, stack.end());
      } else {
        callback(*current_message, res);
      }
    }
  }

 private:
  // Returns submessage by given field and access information.
  // Returns nullptr if sub message is missed.
  const Message* GetSubMessage(const Message& m, int i) const {
    const auto* field = fields_[i];
    const auto& access_info = access_infos_[i];
    DCHECK(!std::holds_alternative<RepeatedFieldAccess>(access_info));
    // NO_CDC: reflection based library
    const auto* ref = m.GetReflection();
    if (field->is_repeated()) {
      auto& access = *std::get_if<RepeatedFieldIndexAccess>(&access_info);
      if (access.idx < ref->FieldSize(m, field)) {
        return &ref->GetRepeatedMessage(m, field, access.idx);
      } else {
        return nullptr;
      }
    } else {
      if (ref->HasField(m, field)) {
        return &ref->GetMessage(m, field);
      } else {
        return nullptr;
      }
    }
  }

  std::vector<const FieldDescriptor*> fields_;
  std::vector<ProtoFieldAccessInfo> access_infos_;
};

// Class for reading from proto and setting
// OptionalValue<T> to the specified frame.
// LastGetFn must have signature OptionalValue<T>(const Message& m) and expect
// message after resolving all intermediate submessages.
template <class T>
struct OptionalReader {
  void operator()(const Message& m, FramePtr frame) const {
    const Message* last_message = traverser.GetLastSubMessage(m);
    if (last_message == nullptr) {
      frame.Set(slot, {});
    } else {
      get_fn(*last_message, frame.GetMutable(slot));
    }
  }

  Traverser traverser;
  FrameLayout::Slot<OptionalValue<T>> slot;
  ReadValueFn get_fn;
};

// Factory for OptionalReader for setting data into OptionalValue<T>.
template <class T>
struct OptionalReaderFactory {
  absl::StatusOr<ProtoTypeReader::BoundReadFn> operator()(
      TypedSlot typed_slot) const {
    ASSIGN_OR_RETURN(auto slot, typed_slot.ToSlot<OptionalValue<T>>());
    return OptionalReader<T>{traverser, slot, get_fn};
  }

  Traverser traverser;
  ReadValueFn get_fn;
};

// ArraySizeReader for setting size of the last field into
// ::arolla::DenseArray<arolla_size_t>. Note that Traverser works
// on std::vector behind the scenes, then converts to ::arolla::DenseArray.
struct ArraySizeReader {
  void operator()(const Message& m, FramePtr frame) const {
    std::vector<arolla_size_t> res;
    traverser.TraverseSubmessages(m, last_push_back_fn, &res);
    frame.Set(slot,
              ::arolla::DenseArray<arolla_size_t>{
                  ::arolla::Buffer<arolla_size_t>::Create(std::move(res))});
  }

  Traverser traverser;
  FrameLayout::Slot<::arolla::DenseArray<arolla_size_t>> slot;
  PushbackFn last_push_back_fn;
};

// Factory for ArraySizeReader for setting size of the last field into
// ::arolla::DenseArray<arolla_size_t>.
struct ArraySizeReaderFactory {
  absl::StatusOr<ProtoTypeReader::BoundReadFn> operator()(
      TypedSlot typed_slot) const {
    ASSIGN_OR_RETURN(auto slot,
                     typed_slot.ToSlot<::arolla::DenseArray<arolla_size_t>>());
    return ArraySizeReader{traverser, slot, SizePushBackFn{last_field}};
  }

  Traverser traverser;
  const FieldDescriptor* last_field;
};

// ShapeSizeReader for setting size of the last field into
// ::arolla::DenseArrayShape.
struct ShapeSizeReader {
  void operator()(const Message& m, FramePtr frame) const {
    DenseArrayShape res;
    traverser.TraverseSubmessages(m, last_push_back_fn, &res);
    frame.Set(slot, res);
  }

  Traverser traverser;
  FrameLayout::Slot<::arolla::DenseArrayShape> slot;
  PushbackFn last_push_back_fn;
};

// Factory for ShapeSizeReaderFactory for setting size of the last field into
// ::arolla::DenseArrayShape.
struct ShapeSizeReaderFactory {
  absl::StatusOr<ProtoTypeReader::BoundReadFn> operator()(
      TypedSlot typed_slot) const {
    ASSIGN_OR_RETURN(auto slot, typed_slot.ToSlot<::arolla::DenseArrayShape>());
    return ShapeSizeReader{traverser, slot, SizeToShapeFn{last_field}};
  }

  Traverser traverser;
  const FieldDescriptor* last_field;
};

// DenseArrayReader for setting data into ::arolla::DenseArray<T> slot.
// Note that Traverser works
// on std::vector behind the scenes, then converts to ::arolla::DenseArray.
template <class T>
struct DenseArrayReader {
  void operator()(const Message& m, FramePtr frame) const {
    std::vector<OptionalValue<T>> res;
    traverser.TraverseSubmessages(m, last_push_back_fn, &res);
    // CreateDenseArray since direct transfer of ownership from std::vector is
    // impossible with bool or arolla::Bytes.
    frame.Set(slot, ::arolla::CreateDenseArray<T>(res));
  }

  Traverser traverser;
  FrameLayout::Slot<::arolla::DenseArray<T>> slot;
  PushbackFn last_push_back_fn;
};

// Factory for DenseArrayReader for setting data into ::arolla::DenseArray<T>.
template <class T>
struct DenseArrayReaderFactory {
  absl::StatusOr<ProtoTypeReader::BoundReadFn> operator()(
      TypedSlot typed_slot) const {
    ASSIGN_OR_RETURN(auto slot, typed_slot.ToSlot<::arolla::DenseArray<T>>());
    return DenseArrayReader<T>{traverser, slot, last_push_back_fn};
  }

  Traverser traverser;
  PushbackFn last_push_back_fn;
};

// Call Callback with corresponding std::decay<T> and kProtoGetter* object
// Callback must return absl::StatusOr<R>.
template <class CallBackFn>
auto SwitchByProtoType(FieldDescriptor::Type type, CallBackFn callback,
                       StringFieldType string_type)
    -> decltype(std::declval<CallBackFn>()(std::decay<int32_t>(),
                                           kProtoGetterInt32)) {
  switch (type) {
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SFIXED32:
      return callback(std::decay<int32_t>{}, kProtoGetterInt32);
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_SFIXED64:
      return callback(std::decay<int64_t>{}, kProtoGetterInt64);
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_FIXED32:
      return callback(std::decay<int64_t>{}, kProtoGetterUInt32);
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED64:
      return callback(std::decay<uint64_t>{}, kProtoGetterUInt64);
    case FieldDescriptor::TYPE_DOUBLE:
      return callback(std::decay<double>{}, kProtoGetterDouble);
    case FieldDescriptor::TYPE_FLOAT:
      return callback(std::decay<float>{}, kProtoGetterFloat);
    case FieldDescriptor::TYPE_BOOL:
      return callback(std::decay<bool>{}, kProtoGetterBool);
    case FieldDescriptor::TYPE_STRING: {
      switch (string_type) {
        case StringFieldType::kText:
          return callback(std::decay<Text>{}, kProtoGetterString);
        case StringFieldType::kBytes:
          return callback(std::decay<Bytes>{}, kProtoGetterString);
      }
    }
    case FieldDescriptor::TYPE_BYTES:
      return callback(std::decay<Bytes>{}, kProtoGetterString);
    case FieldDescriptor::TYPE_ENUM:
      return callback(std::decay<int32_t>{}, kProtoGetterEnumValue);
    default:
      return absl::FailedPreconditionError(
          absl::StrCat("type ", type, " is not supported"));
  }
}

absl::Status VerifyFieldsAndAccessInfos(
    absl::Span<const FieldDescriptor* const> fields,
    const std::vector<ProtoFieldAccessInfo>& access_infos,
    bool allow_repeated = false) {
  if (fields.empty()) {
    return absl::FailedPreconditionError("fields must be non empty");
  }
  if (fields.size() != access_infos.size()) {
    return absl::FailedPreconditionError(
        "fields and access_info must be same size if access_info is not empty");
  }
  for (size_t i = 0; i != fields.size(); ++i) {
    RETURN_IF_ERROR(CheckAccessInfo(fields[i], access_infos[i], allow_repeated,
                                    /*is_last=*/i + 1 == fields.size()));
  }
  return absl::OkStatus();
}

// Class for creation OptionalReader based on C++ type and ProtoFieldGetter.
class OptionalReaderCallback {
 public:
  OptionalReaderCallback(absl::Span<const FieldDescriptor* const> fields,
                         absl::Span<const ProtoFieldAccessInfo> access_infos)
      : fields_(fields),
        access_infos_(access_infos),
        traverser_(
            std::vector(fields_.begin(), fields_.end() - 1),
            std::vector(access_infos_.begin(), access_infos_.end() - 1)) {}

  template <class ResultMetaFn, class ProtoFieldGetter>
  absl::StatusOr<std::unique_ptr<ProtoTypeReader>> operator()(
      ResultMetaFn, ProtoFieldGetter last_field_getter) const {
    using ResultT = typename ResultMetaFn::type;
    ProtoFieldAccessInfo last_access_info = access_infos_.back();
    const FieldDescriptor* last_field = fields_.back();
    ReadValueFn read_fn;
    if (last_field->is_repeated()) {
      DCHECK(
          std::holds_alternative<RepeatedFieldIndexAccess>(last_access_info));
      read_fn = ByIndexReader<ResultT, ProtoFieldGetter>{
          last_field, *std::get_if<RepeatedFieldIndexAccess>(&last_access_info),
          last_field_getter};
    } else {
      read_fn =
          FieldReader<ResultT, ProtoFieldGetter>{last_field, last_field_getter};
    }
    return std::make_unique<ProtoTypeReader>(
        GetOptionalQType<ResultT>(),
        OptionalReaderFactory<ResultT>{traverser_, read_fn});
  }

  // Construct size accessor, which doesn't depend on the type of the field.
  absl::StatusOr<std::unique_ptr<ProtoTypeReader>> CreateSizeAccessor() const {
    ProtoFieldAccessInfo last_access_info = access_infos_.back();
    if (!std::holds_alternative<RepeatedFieldSizeAccess>(last_access_info)) {
      return absl::InternalError("size accessor creation expected");
    }
    const FieldDescriptor* last_field = fields_.back();
    return std::make_unique<ProtoTypeReader>(
        GetQType<DenseArrayShape>(),
        ShapeSizeReaderFactory{traverser_, last_field});
  }

 private:
  absl::Span<const FieldDescriptor* const> fields_;
  absl::Span<const ProtoFieldAccessInfo> access_infos_;
  Traverser traverser_;
};

// Class for creation DenseArrayReaderCallback based on C++ type and
// ProtoFieldGetter.
class DenseArrayReaderCallback {
 public:
  DenseArrayReaderCallback(absl::Span<const FieldDescriptor* const> fields,
                           absl::Span<const ProtoFieldAccessInfo> access_infos)
      : fields_(fields),
        access_infos_(access_infos),
        traverser_(
            std::vector(fields_.begin(), fields_.end() - 1),
            std::vector(access_infos_.begin(), access_infos_.end() - 1)) {}

  // Construct size accessor, which doesn't depend on the type of the field.
  absl::StatusOr<std::unique_ptr<ProtoTypeReader>> CreateSizeAccessor() const {
    ProtoFieldAccessInfo last_access_info = access_infos_.back();
    if (!std::holds_alternative<RepeatedFieldSizeAccess>(last_access_info)) {
      return absl::InternalError("size accessor creation expected");
    }
    const FieldDescriptor* last_field = fields_.back();
    return std::make_unique<ProtoTypeReader>(
        GetDenseArrayQType<arolla_size_t>(),
        ArraySizeReaderFactory{traverser_, last_field});
  }

  // Construct fields accessor, which depends on the type of the field.
  template <class ResultMetaFn, class ProtoFieldGetter>
  absl::StatusOr<std::unique_ptr<ProtoTypeReader>> operator()(
      ResultMetaFn, ProtoFieldGetter last_field_getter) const {
    using ResultT = typename ResultMetaFn::type;
    using DenseArrayResultT = ::arolla::DenseArray<ResultT>;
    ProtoFieldAccessInfo last_access_info = access_infos_.back();
    const FieldDescriptor* last_field = fields_.back();
    PushbackFn pb_fn;
    if (std::holds_alternative<RepeatedFieldAccess>(last_access_info)) {
      pb_fn = ManyPushBackFn<ResultT, ProtoFieldGetter>{last_field,
                                                        last_field_getter};
    } else if (std::holds_alternative<RepeatedFieldSizeAccess>(
                   last_access_info)) {
      return absl::InternalError(
          "size accessor must be created with CreateSizeAccessor");
    } else if (last_field->is_repeated()) {
      DCHECK(
          std::holds_alternative<RepeatedFieldIndexAccess>(last_access_info));
      pb_fn =
          SinglePushBackFn<ResultT>{ByIndexReader<ResultT, ProtoFieldGetter>{
              last_field,
              *std::get_if<RepeatedFieldIndexAccess>(&last_access_info),
              last_field_getter}};
    } else {
      pb_fn = SinglePushBackFn<ResultT>{FieldReader<ResultT, ProtoFieldGetter>{
          last_field, last_field_getter}};
    }
    return std::make_unique<ProtoTypeReader>(
        GetQType<DenseArrayResultT>(),
        DenseArrayReaderFactory<ResultT>{traverser_, pb_fn});
  }

 private:
  absl::Span<const FieldDescriptor* const> fields_;
  absl::Span<const ProtoFieldAccessInfo> access_infos_;
  Traverser traverser_;
};

}  // namespace

namespace arolla::proto {

absl::StatusOr<std::unique_ptr<ProtoTypeReader>>
ProtoTypeReader::CreateOptionalReader(
    absl::Span<const FieldDescriptor* const> fields,
    std::vector<ProtoFieldAccessInfo> access_infos,
    proto::StringFieldType string_type) {
  RETURN_IF_ERROR(VerifyFieldsAndAccessInfos(fields, access_infos));
  const FieldDescriptor* last_field = fields.back();
  return SwitchByProtoType(
      last_field->type(),
      OptionalReaderCallback(fields, std::move(access_infos)), string_type);
}

absl::StatusOr<std::unique_ptr<ProtoTypeReader>>
ProtoTypeReader::CreateDenseArrayShapeReader(
    absl::Span<const FieldDescriptor* const> fields,
    std::vector<ProtoFieldAccessInfo> access_infos,
    proto::StringFieldType string_type) {
  RETURN_IF_ERROR(VerifyFieldsAndAccessInfos(fields, access_infos,
                                             /*allow_repeated=*/true));
  return OptionalReaderCallback(fields, std::move(access_infos))
      .CreateSizeAccessor();
}

absl::StatusOr<std::unique_ptr<ProtoTypeReader>>
ProtoTypeReader::CreateDenseArrayReader(
    absl::Span<const FieldDescriptor* const> fields,
    std::vector<ProtoFieldAccessInfo> access_infos,
    proto::StringFieldType string_type) {
  RETURN_IF_ERROR(VerifyFieldsAndAccessInfos(fields, access_infos,
                                             /*allow_repeated=*/true));
  const FieldDescriptor* last_field = fields.back();
  auto last_access = access_infos.back();
  DenseArrayReaderCallback callback(fields, std::move(access_infos));
  if (std::holds_alternative<proto::RepeatedFieldSizeAccess>(last_access)) {
    return callback.CreateSizeAccessor();
  } else {
    return SwitchByProtoType(last_field->type(), callback, string_type);
  }
}

}  // namespace arolla::proto

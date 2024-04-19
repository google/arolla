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
#include "arolla/codegen/expr/types.h"

#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "double-conversion/double-to-string.h"
#include "double-conversion/utils.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/bytes.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/string.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::codegen {

namespace {

std::string CppLiteralReprImpl(Unit) { return "::arolla::kUnit"; }

std::string CppLiteralReprImpl(bool value) { return value ? "true" : "false"; }

std::string CppLiteralReprImpl(int32_t value) {
  return absl::StrFormat("int32_t{%d}", value);
}

std::string CppLiteralReprImpl(int64_t value) {
  return absl::StrFormat("int64_t{%d}", value);
}

std::string CppLiteralReprImpl(uint64_t value) {
  return absl::StrFormat("uint64_t{%dull}", value);
}

using double_conversion::DoubleToStringConverter;

std::string CppLiteralReprImpl(float value) {
  static const DoubleToStringConverter converter(
      DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT,
      "std::numeric_limits<float>::infinity()",
      "std::numeric_limits<float>::quiet_NaN()", 'e', -8, 21, 6, 0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  builder.AddString("float{");
  converter.ToShortestSingle(value, &builder);
  builder.AddString("}");
  return builder.Finalize();
}

std::string CppLiteralReprImpl(double value) {
  static const DoubleToStringConverter converter(
      DoubleToStringConverter::EMIT_TRAILING_DECIMAL_POINT,
      "std::numeric_limits<double>::infinity()",
      "std::numeric_limits<double>::quiet_NaN()", 'e', -12, 21, 6, 0);
  char buf[128];
  double_conversion::StringBuilder builder(buf, sizeof(buf));
  builder.AddString("double{");
  converter.ToShortest(value, &builder);
  builder.AddString("}");
  return builder.Finalize();
}

const char kRawStringDelimiter[] = "RL_CODEGEN_DELIM";

std::string CppRawStringLiteral(absl::string_view view) {
  return absl::StrFormat("R\"%s(%s)%s\"", kRawStringDelimiter, view,
                         kRawStringDelimiter);
}

std::string CppLiteralReprImpl(const Bytes& bytes) {
  return absl::StrFormat("::arolla::Bytes(%s)", CppRawStringLiteral(bytes));
}

std::string CppLiteralReprImpl(const Text& text) {
  return absl::StrFormat("::arolla::Text(%s)",
                         CppRawStringLiteral(text.view()));
}

// Returns default constructed type.
// Doesn't allocate value on heap.
// Default constructed object's destructor is typically no op.
absl::StatusOr<std::string> DefaultConstructedCppLiteralRepr(QTypePtr qtype) {
  ASSIGN_OR_RETURN(std::string type_name, CppTypeName(qtype));
  return absl::StrFormat("%s{}", type_name);
}

absl::StatusOr<std::string> OptionalCppLiteralRepr(TypedRef value) {
  auto qtype = value.GetType();
  if (IsScalarQType(DecayOptionalQType(qtype))) {
    bool is_correct_optional =
        (qtype == GetQType<OptionalUnit>() && value.GetFieldCount() == 1) ||
        (qtype != GetQType<OptionalUnit>() && value.GetFieldCount() == 2);
    if (!is_correct_optional) {
      return absl::InternalError(absl::StrFormat(
          "Wrong number of fields in optional type %s", qtype->name()));
    }
    ASSIGN_OR_RETURN(bool present, value.GetField(0).As<bool>());
    if (!present) {
      return DefaultConstructedCppLiteralRepr(qtype);
    } else {
      ASSIGN_OR_RETURN(std::string type_name, CppTypeName(qtype));
      ASSIGN_OR_RETURN(std::string value_repr,
                       qtype == GetQType<OptionalUnit>()
                           ? CppLiteralReprImpl(kUnit)
                           : CppLiteralRepr(value.GetField(1)));
      return absl::StrFormat("::arolla::MakeOptionalValue(%s)", value_repr);
    }
  }
  return absl::UnimplementedError(
      absl::StrFormat("CppLiteralRepr is unknown for type %s", qtype->name()));
}

template <class T>
absl::StatusOr<std::string> CppLiteralReprImpl(const DenseArray<T>& values) {
  std::vector<std::string> values_str;
  values_str.reserve(values.size());
  for (int64_t i = 0; i != values.size(); ++i) {
    OptionalValue<T> value;
    if (values.present(i)) {
      value = T(values.values[i]);
    }
    ASSIGN_OR_RETURN(auto value_str,
                     OptionalCppLiteralRepr(TypedRef::FromValue(value)));
    values_str.push_back(value_str);
  }
  ASSIGN_OR_RETURN(std::string type_name, CppTypeName(GetQType<T>()));
  return absl::StrFormat("::arolla::CreateDenseArray<%s>({%s})", type_name,
                         absl::StrJoin(values_str, ","));
}

// Helper to resolve absl::StatusOr<std::reference_wrapper<T>>.
template <class T>
absl::StatusOr<std::string> CppLiteralReprImpl(
    const absl::StatusOr<std::reference_wrapper<T>>& value_or) {
  ASSIGN_OR_RETURN(auto value, value_or);
  return CppLiteralReprImpl(value.get());
}

#define RETURN_CPP_LITERAL_IF_SAME_TYPE(_, CTYPE)         \
  if (GetQType<CTYPE>() == value.GetType()) {             \
    return CppLiteralReprImpl(value.As<CTYPE>().value()); \
  }

absl::StatusOr<std::string> NonOptionalCppLiteralRepr(TypedRef value) {
  AROLLA_FOREACH_BASE_TYPE(RETURN_CPP_LITERAL_IF_SAME_TYPE);
  // TODO: Add Unit to AROLLA_FOREACH_BASE_TYPE macro.
  RETURN_CPP_LITERAL_IF_SAME_TYPE(UNIT, Unit);
  return absl::FailedPreconditionError(absl::StrFormat(
      "Unsupported literal QType: %s", value.GetType()->name()));
}

#define RETURN_CPP_LITERAL_IF_SAME_DENSE_ARRAY_TYPE(_, CTYPE) \
  if (GetQType<DenseArray<CTYPE>>() == value.GetType()) {     \
    return CppLiteralReprImpl(value.As<DenseArray<CTYPE>>()); \
  }

absl::StatusOr<std::string> DenseArrayCppLiteralRepr(TypedRef value) {
  AROLLA_FOREACH_BASE_TYPE(RETURN_CPP_LITERAL_IF_SAME_DENSE_ARRAY_TYPE);
  return absl::UnimplementedError(absl::StrFormat(
      "CppLiteralRepr is unknown for type %s", value.GetType()->name()));
}

#undef RETURN_CPP_LITERAL_IF_SAME_DENSE_ARRAY_TYPE

absl::StatusOr<std::string> DenseArrayEdgeCppLiteralRepr(DenseArrayEdge edge) {
  // Wrap as lambda to workaround clang false positive -Wdangling in
  // generated code.
  auto wrap_as_lambda = [](absl::string_view x) {
    return absl::StrFormat("[]() { return %s; }()", x);
  };
  if (edge.edge_type() == DenseArrayEdge::SPLIT_POINTS) {
    ASSIGN_OR_RETURN(std::string split_points,
                     CppLiteralReprImpl(edge.edge_values()));
    return wrap_as_lambda(absl::StrFormat(
        "::arolla::DenseArrayEdge::FromSplitPoints(%s).value()", split_points));
  } else if (edge.edge_type() == DenseArrayEdge::MAPPING) {
    ASSIGN_OR_RETURN(std::string mapping,
                     CppLiteralReprImpl(edge.edge_values()));
    return wrap_as_lambda(
        absl::StrFormat("::arolla::DenseArrayEdge::FromMapping(%s, %d).value()",
                        mapping, edge.parent_size()));
  }

  return absl::UnimplementedError(absl::StrFormat(
      "CppLiteralRepr is unknown for %d DenseArrayEdge edge_type",
      edge.edge_type()));
}

struct TypeMap {
  absl::Mutex lock;
  absl::flat_hash_map<QTypePtr, std::string> cpp_type_name;
  absl::flat_hash_map<QTypePtr,
                      std::function<absl::StatusOr<std::string>(TypedRef)>>
      cpp_literal_repr_fn;
};

TypeMap& GetTypeMap() {
  static Indestructible<TypeMap> kTypeMap;
  return *kTypeMap;
}

absl::StatusOr<std::string> CppTupleLiteralRepr(TypedRef value) {
  if (!IsTupleQType(value.GetType())) {
    return absl::InternalError("expected tuple QType");
  }
  std::ostringstream res;
  res << "::arolla::MakeTupleFromFields(";
  bool first = true;
  for (int64_t i = 0; i != value.GetFieldCount(); ++i) {
    TypedRef f_slot = value.GetField(i);
    ASSIGN_OR_RETURN(std::string value, CppLiteralRepr(f_slot));
    res << NonFirstComma(first) << value;
  }
  res << ")";
  return res.str();
}

absl::StatusOr<std::string> CppTupleQTypeConstruction(QTypePtr qtype) {
  if (!IsTupleQType(qtype)) {
    return absl::InternalError("expected tuple QType");
  }
  std::ostringstream res;
  res << "::arolla::MakeTupleQType({";
  bool first = true;
  for (const auto& f_slot : qtype->type_fields()) {
    ASSIGN_OR_RETURN(std::string qtype, CppQTypeConstruction(f_slot.GetType()));
    res << NonFirstComma(first) << qtype;
  }
  res << "})";
  return res.str();
}

}  // namespace

absl::Status RegisterCppType(
    QTypePtr qtype, absl::string_view cpp_type_name,
    std::function<absl::StatusOr<std::string>(TypedRef)> cpp_literal_repr) {
  TypeMap& type_map = GetTypeMap();
  absl::MutexLock l(&type_map.lock);
  if (type_map.cpp_type_name.contains(qtype) ||
      type_map.cpp_literal_repr_fn.contains(qtype)) {
    return absl::FailedPreconditionError(
        absl::StrFormat("RegisterCppType called twice for %s", qtype->name()));
  }
  type_map.cpp_type_name.emplace(qtype, std::string(cpp_type_name));
  type_map.cpp_literal_repr_fn.emplace(qtype, std::move(cpp_literal_repr));
  return absl::OkStatus();
}

absl::StatusOr<std::string> CppTypeName(QTypePtr qtype) {
  // check external types
  const TypeMap& type_map = GetTypeMap();
  if (auto it = type_map.cpp_type_name.find(qtype);
      it != type_map.cpp_type_name.end()) {
    return it->second;
  }
  if (IsScalarQType(qtype)) {
    if (qtype == GetQType<bool>()) {
      return "bool";
    }
    if (qtype == GetQType<int32_t>()) {
      return "int32_t";
    }
    if (qtype == GetQType<int64_t>()) {
      return "int64_t";
    }
    if (qtype == GetQType<float>()) {
      return "float";
    }
    if (qtype == GetQType<double>()) {
      return "double";
    }
    if (qtype == GetQType<uint64_t>()) {
      return "uint64_t";
    }
    if (qtype == GetQType<Unit>()) {
      return "::arolla::Unit";
    }
    if (qtype == GetQType<Bytes>()) {
      return "::arolla::Bytes";
    }
    if (qtype == GetQType<Text>()) {
      return "::arolla::Text";
    }
  }
  if (IsOptionalQType(qtype)) {
    if (qtype == GetOptionalQType<Unit>()) {
      return "::arolla::OptionalUnit";
    }
    if (IsScalarQType(qtype->value_qtype())) {
      ASSIGN_OR_RETURN(auto value_type_name,
                       CppTypeName(DecayOptionalQType(qtype)));
      return absl::StrFormat("::arolla::OptionalValue<%s>", value_type_name);
    }
  }
  if (IsDenseArrayQType(qtype)) {
    if (IsScalarQType(qtype->value_qtype())) {
      ASSIGN_OR_RETURN(auto value_type_name, CppTypeName(qtype->value_qtype()));
      return absl::StrFormat("::arolla::DenseArray<%s>", value_type_name);
    }
  }
  if (qtype == GetQType<DenseArrayShape>()) {
    return "::arolla::DenseArrayShape";
  }
  if (qtype == GetQType<DenseArrayEdge>()) {
    return "::arolla::DenseArrayEdge";
  }
  if (qtype == GetQType<DenseArrayGroupScalarEdge>()) {
    return "::arolla::DenseArrayGroupScalarEdge";
  }
  if (IsTupleQType(qtype)) {
    return "::arolla::TypedValue";
  }
  return absl::UnimplementedError(
      absl::StrFormat("CppTypeName is unknown for type %s", qtype->name()));
}

absl::StatusOr<std::string> CppQTypeConstruction(QTypePtr qtype) {
  if (IsTupleQType(qtype)) {
    return CppTupleQTypeConstruction(qtype);
  }
  ASSIGN_OR_RETURN(std::string type_name, CppTypeName(qtype));
  return absl::StrFormat("::arolla::GetQType<%s>()", type_name);
}

absl::StatusOr<std::string> CppLiteralRepr(TypedRef value) {
  auto qtype = value.GetType();
  // check external types
  const TypeMap& type_map = GetTypeMap();
  if (auto it = type_map.cpp_literal_repr_fn.find(qtype);
      it != type_map.cpp_literal_repr_fn.end()) {
    return it->second(value);
  }
  if (IsScalarQType(qtype)) {
    return NonOptionalCppLiteralRepr(value);
  }
  if (IsOptionalQType(qtype)) {
    return OptionalCppLiteralRepr(value);
  }
  if (IsDenseArrayQType(qtype)) {
    return DenseArrayCppLiteralRepr(value);
  }
  if (qtype == GetQType<DenseArrayShape>()) {
    return absl::StrFormat("::arolla::DenseArrayShape{%d}",
                           value.UnsafeAs<DenseArrayShape>().size);
  }
  if (qtype == GetQType<DenseArrayEdge>()) {
    ASSIGN_OR_RETURN(auto edge, value.As<DenseArrayEdge>());
    return DenseArrayEdgeCppLiteralRepr(edge);
  }
  if (qtype == GetQType<DenseArrayGroupScalarEdge>()) {
    return absl::StrFormat(
        "::arolla::DenseArrayGroupScalarEdge{%d}",
        value.UnsafeAs<DenseArrayGroupScalarEdge>().child_size());
  }
  if (IsTupleQType(qtype)) {
    return CppTupleLiteralRepr(value);
  }
  return absl::UnimplementedError(
      absl::StrFormat("CppLiteralRepr is unknown for type %s", qtype->name()));
}

}  // namespace arolla::codegen

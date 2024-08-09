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
#include "arolla/expr/operators/bootstrap_operators.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/array/array.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/annotation_expr_operators.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/lambda_expr_operator.h"
#include "arolla/expr/operators/casting_registry.h"
#include "arolla/expr/operators/derived_qtype_operators.h"
#include "arolla/expr/operators/map_operator.h"
#include "arolla/expr/operators/meta_operators.h"
#include "arolla/expr/operators/registration.h"
#include "arolla/expr/operators/tuple_bootstrap_operators.h"
#include "arolla/expr/operators/type_meta_eval_strategies.h"
#include "arolla/expr/operators/weak_qtype_operators.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/expr/seq_map_expr_operator.h"
#include "arolla/expr/seq_reduce_expr_operator.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/shape_qtype.h"
#include "arolla/qtype/slice_qtype.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"
#include "arolla/qtype/standard_type_properties/properties.h"
#include "arolla/qtype/strings/regex.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/qtype/unspecified_qtype.h"
#include "arolla/sequence/sequence_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::BackendExprOperatorTag;
using ::arolla::expr::ExportAnnotation;
using ::arolla::expr::ExportValueAnnotation;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorWithFixedSignature;
using ::arolla::expr::GetValueQTypes;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeLambdaOperator;
using ::arolla::expr::NameAnnotation;
using ::arolla::expr::QTypeAnnotation;
using ::arolla::expr::RegisterOperator;
using ::arolla::expr::SeqMapOperator;
using ::arolla::expr::SeqReduceOperator;
using ::arolla::expr_operators::type_meta::Binary;
using ::arolla::expr_operators::type_meta::Broadcast;
using ::arolla::expr_operators::type_meta::Chain;
using ::arolla::expr_operators::type_meta::CommonType;
using ::arolla::expr_operators::type_meta::Is;
using ::arolla::expr_operators::type_meta::Nth;
using ::arolla::expr_operators::type_meta::NthApply;
using ::arolla::expr_operators::type_meta::NthMatch;
using ::arolla::expr_operators::type_meta::Or;
using ::arolla::expr_operators::type_meta::PresenceOrType;
using ::arolla::expr_operators::type_meta::QTypes;
using ::arolla::expr_operators::type_meta::Returns;
using ::arolla::expr_operators::type_meta::ScalarTypeIs;
using ::arolla::expr_operators::type_meta::ToOptional;
using ::arolla::expr_operators::type_meta::ToTestResult;

absl::StatusOr<bool> GetBoolFlag(const TypedValue& value) {
  if (value.GetType() == GetOptionalQType<bool>()) {
    OptionalValue<bool> flag = value.UnsafeAs<OptionalValue<bool>>();
    return flag.present && flag.value;
  } else {
    return value.As<bool>();
  }
}

// core.cast(arg, qtype, implicit_only=false) operator casts `arg` to `qtype`
// type, or fails in ToLower if no casting available.
class CastOp final : public ExprOperatorWithFixedSignature {
 public:
  CastOp()
      : ExprOperatorWithFixedSignature(
            "core.cast",
            ExprOperatorSignature{
                {"arg"},
                {"qtype"},
                {.name = "implicit_only",
                 .default_value = TypedValue::FromValue(false)}},
            "Casts `arg` to `qtype`, or fails if no casting available.",
            FingerprintHasher("arolla::expr_operators::CastOp").Finish()) {}

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;  // We are not ready for lowering yet.
    }
    return CastingRegistry::GetInstance()->GetCast(node->node_deps()[0],
                                                   node->qtype(),
                                                   /*implicit_only=*/false);
  }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    {  // Input validation.
      const auto& qtype = inputs[1];
      const auto& implicit_only = inputs[2];
      if (qtype.qtype() && qtype.qtype() != GetQTypeQType()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected a qtype, got qtype: %s", qtype.qtype()->name()));
      }
      if (qtype.qtype() && !qtype.qvalue()) {
        return absl::InvalidArgumentError("`qtype` must be a literal");
      }
      if (implicit_only.qtype() && implicit_only.qtype() != GetQType<bool>() &&
          implicit_only.qtype() != GetOptionalQType<bool>()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected a boolean scalar or optional, got implicit_only: %s",
            implicit_only.qtype()->name()));
      }
      if (implicit_only.qtype() && !implicit_only.qvalue()) {
        return absl::InvalidArgumentError(
            "`implicit_only` must be a boolean literal");
      }
      if (!HasAllAttrQTypes(inputs)) {
        return ExprAttributes{};
      }
    }
    {  // Deducing result.
      const QTypePtr arg_qtype = inputs[0].qtype();
      const QTypePtr target_qtype = inputs[1].qvalue()->UnsafeAs<QTypePtr>();
      ASSIGN_OR_RETURN(bool implicit_only, GetBoolFlag(*inputs[2].qvalue()));
      if (arg_qtype == target_qtype) {  // if no casting is needed, return true.
        return ExprAttributes(target_qtype);
      }
      if (implicit_only) {  // implicit only casting
        if (!CanCastImplicitly(arg_qtype, target_qtype,
                               /*enable_broadcasting=*/false)) {
          return absl::InvalidArgumentError(
              absl::StrFormat("implicit casting from %s to %s is not allowed",
                              arg_qtype->name(), target_qtype->name()));
        }
        return ExprAttributes(target_qtype);
      }
      bool ok = true;
      const QType* arg_scalar_qtype = GetScalarQTypeOrNull(arg_qtype);
      const QType* target_scalar_qtype = GetScalarQTypeOrNull(target_qtype);
      if (ok) {
        ok = (arg_scalar_qtype != nullptr && target_scalar_qtype != nullptr);
      }
      if (ok) {  // Check compatibility of the scalar qtypes.
        bool arg_pass = IsNumericScalarQType(arg_scalar_qtype) ||
                        arg_scalar_qtype == GetQType<bool>() ||
                        arg_scalar_qtype == GetQType<uint64_t>();
        bool target_pass = IsNumericScalarQType(target_scalar_qtype) ||
                           target_scalar_qtype == GetQType<bool>() ||
                           target_scalar_qtype == GetQType<uint64_t>();
        ok = (arg_scalar_qtype == target_scalar_qtype ||
              (arg_pass && target_pass));
      }
      const QType* arg_with_unit_qtype =
          WithScalarQType(arg_qtype, GetQType<Unit>()).value_or(nullptr);
      const QType* target_with_unit_qtype =
          WithScalarQType(target_qtype, GetQType<Unit>()).value_or(nullptr);
      if (ok) {
        ok = (arg_with_unit_qtype != nullptr &&
              target_with_unit_qtype != nullptr);
      }
      if (ok) {
        ok = (arg_with_unit_qtype == target_with_unit_qtype ||
              ToOptionalLikeQType(arg_with_unit_qtype).value_or(nullptr) ==
                  target_with_unit_qtype);
      }
      if (ok) {
        return ExprAttributes(target_qtype);
      }
      return absl::InvalidArgumentError(
          absl::StrFormat("casting from %s to %s is not allowed",
                          arg_qtype->name(), target_qtype->name()));
    }
  }
};

// core.cast_values(arg, scalar_qtype) operator casts `arg` to a compatible type
// with `scalar_qtype` scalar type.
class CastValuesOp final : public expr::ExprOperatorWithFixedSignature {
 public:
  CastValuesOp()
      : ExprOperatorWithFixedSignature(
            "core.cast_values",
            ExprOperatorSignature{{"arg"}, {"scalar_qtype"}},
            "Casts elements to a new type. The resulting type has the same "
            "shape type as arg and the same scalar type as scalar_qtype",
            FingerprintHasher("arolla::expr_operators::CastValuesToOp")
                .Finish()) {}

  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;  // We don't know QType so we're not ready for lowering.
    }
    return CastingRegistry::GetInstance()->GetCast(node->node_deps()[0],
                                                   node->qtype(),
                                                   /*implicit_only=*/false);
  }

  absl::StatusOr<expr::ExprAttributes> InferAttributes(
      absl::Span<const expr::ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    if (inputs[1].qtype() && inputs[1].qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a qtype, got scalar_qtype: %s", inputs[1].qtype()->name()));
    }
    if (inputs[1].qtype() && !inputs[1].qvalue()) {
      return absl::InvalidArgumentError("`scalar_qtype` must be a literal");
    }
    if (!inputs[1].qvalue()) {
      return ExprAttributes{};
    }
    auto scalar_qtype = inputs[1].qvalue()->UnsafeAs<QTypePtr>();
    if (!IsScalarQType(scalar_qtype)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a scalar qtype, got scalar_qtype=%s",
                          scalar_qtype->name()));
    }
    if (!inputs[0].qtype()) {
      return ExprAttributes{};
    }

    // Check compatibility of the scalar qtypes.
    const QType* target_qtype =
        GetScalarQType(inputs[0].qtype()).value_or(nullptr);
    if (target_qtype == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("casting from %s to %s is not allowed",
                          scalar_qtype->name(), inputs[0].qtype()->name()));
    }
    bool arg_pass = IsNumericScalarQType(scalar_qtype) ||
                    scalar_qtype == GetQType<bool>() ||
                    scalar_qtype == GetQType<uint64_t>();
    bool target_pass = IsNumericScalarQType(target_qtype) ||
                       target_qtype == GetQType<bool>() ||
                       target_qtype == GetQType<uint64_t>();
    if (scalar_qtype != target_qtype && (!arg_pass || !target_pass)) {
      return absl::InvalidArgumentError(
          absl::StrFormat("casting from %s to %s is not allowed",
                          scalar_qtype->name(), target_qtype->name()));
    }

    ASSIGN_OR_RETURN(auto output_qtype,
                     WithScalarQType(inputs[0].qtype(), scalar_qtype));
    return ExprAttributes(output_qtype);
  }
};

// qtype.qtype_of
class QTypeOfOp final : public ExprOperatorWithFixedSignature {
 public:
  QTypeOfOp()
      : ExprOperatorWithFixedSignature(
            "qtype.qtype_of", ExprOperatorSignature{{"arg"}},
            "Returns QType of the argument.",
            FingerprintHasher("::arolla::expr_operators::InferQTypeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    if (auto* qtype = inputs[0].qtype()) {
      return ExprAttributes(TypedRef::FromValue(qtype));
    }
    return ExprAttributes{};
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (const auto& qvalue = node->qvalue(); qvalue.has_value()) {
      return Literal(*qvalue);
    }
    return node;
  }
};

// qtype.broadcast_qtype_like
class BroadcastQTypeLikeOp final : public BackendExprOperatorTag,
                                   public ExprOperatorWithFixedSignature {
 public:
  BroadcastQTypeLikeOp()
      : ExprOperatorWithFixedSignature(
            "qtype.broadcast_qtype_like",
            ExprOperatorSignature{{"target"}, {"x"}},
            "Broadcasts the given qtype `x` to match the `target` qtype shape "
            "kind.",
            FingerprintHasher("::arolla::expr_operators::BroadcastQTypeLikeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& target = inputs[0];
    const auto& x = inputs[1];
    if (target.qtype() != nullptr && target.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a qtype, got target: %s", x.qtype()->name()));
    }
    if (x.qtype() != nullptr && x.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a qtype, got x: %s", x.qtype()->name()));
    }
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    if (!target.qvalue() || !x.qvalue()) {
      return ExprAttributes(GetQTypeQType());
    }
    ASSIGN_OR_RETURN(
        auto result_qvalue,
        InvokeOperator("qtype.broadcast_qtype_like",
                       {*target.qvalue(), *x.qvalue()}, GetQTypeQType()));
    return ExprAttributes(std::move(result_qvalue));
  }
};

// qtype.common_qtype
class CommonQTypeOp final : public BackendExprOperatorTag,
                            public ExprOperatorWithFixedSignature {
 public:
  CommonQTypeOp()
      : ExprOperatorWithFixedSignature(
            "qtype.common_qtype", ExprOperatorSignature{{"x"}, {"y"}},
            "Returns a common qtype for the given `x` and `y`.",
            FingerprintHasher("::arolla::expr_operators::CommonQTypeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& x = inputs[0];
    const auto& y = inputs[1];
    if (x.qtype() != nullptr && x.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a qtype, got x: %s", x.qtype()->name()));
    }
    if (y.qtype() != nullptr && y.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a qtype, got y: %s", y.qtype()->name()));
    }
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    if (!x.qvalue() || !y.qvalue()) {
      return ExprAttributes(GetQTypeQType());
    }
    ASSIGN_OR_RETURN(
        auto result_qvalue,
        InvokeOperator("qtype.common_qtype", {*x.qvalue(), *y.qvalue()},
                       GetQTypeQType()));
    return ExprAttributes(std::move(result_qvalue));
  }
};

// Operator that creates a slice qtype:
//  `qtype.make_slice_qtype(start, stop, step)`.
// Requires the inputs to be literal qtypes.
class MakeSliceQTypeOperator final : public ExprOperatorWithFixedSignature {
 public:
  MakeSliceQTypeOperator()
      : ExprOperatorWithFixedSignature(
            "qtype.make_slice_qtype",
            ExprOperatorSignature{
                {"start", TypedValue::FromValue(GetUnspecifiedQType())},
                {"stop", TypedValue::FromValue(GetUnspecifiedQType())},
                {"step", TypedValue::FromValue(GetUnspecifiedQType())}},
            "Constructs a slice qtype from the given values.",
            FingerprintHasher("arolla::expr::MakeSliceQTypeOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    std::vector<QTypePtr> qtypes;
    qtypes.reserve(inputs.size());
    for (const auto& attr : inputs) {
      if (!attr.qtype()) continue;
      if (attr.qtype() != GetQTypeQType()) {
        return absl::InvalidArgumentError(
            absl::StrFormat("expected QTYPE, got: %s", attr.qtype()->name()));
      }
      if (!attr.qvalue()) {
        return absl::InvalidArgumentError("expected a literal");
      }
      qtypes.push_back(attr.qvalue()->UnsafeAs<QTypePtr>());
    }
    if (qtypes.size() != inputs.size()) {
      return ExprAttributes{};
    }
    auto* output_qtype = MakeSliceQType(qtypes[0], qtypes[1], qtypes[2]);
    return ExprAttributes(TypedRef::FromValue(output_qtype));
  }
};

class MakeDictQTypeOp final : public BackendExprOperatorTag,
                              public ExprOperatorWithFixedSignature {
 public:
  MakeDictQTypeOp()
      : ExprOperatorWithFixedSignature(
            "qtype.make_dict_qtype",
            ExprOperatorSignature{{"key_qtype"}, {"value_qtype"}},
            "Returns a dict qtype with the given key and value qtypes.",
            FingerprintHasher("::arolla::expr_operators::MakeDictQTypeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));

    const ExprAttributes& key_qtype = inputs[0];
    const ExprAttributes& value_qtype = inputs[1];
    if (key_qtype.qtype() != nullptr && key_qtype.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected QTYPE, got key_qtype: %s", key_qtype.qtype()->name()));
    }
    if (value_qtype.qtype() != nullptr &&
        value_qtype.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected QTYPE, got value_qtype: %s", value_qtype.qtype()->name()));
    }
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    if (!key_qtype.qvalue().has_value() || !value_qtype.qvalue().has_value()) {
      return ExprAttributes(GetQTypeQType());
    }
    ASSIGN_OR_RETURN(auto result_qvalue, InvokeOperator("qtype.make_dict_qtype",
                                                        {*key_qtype.qvalue(),
                                                         *value_qtype.qvalue()},
                                                        GetQTypeQType()));
    return ExprAttributes(std::move(result_qvalue));
  }
};

class GetScalarQTypeOp final : public BackendExprOperatorTag,
                               public ExprOperatorWithFixedSignature {
 public:
  GetScalarQTypeOp()
      : ExprOperatorWithFixedSignature(
            "qtype.get_scalar_qtype", ExprOperatorSignature{{"x"}},
            "Returns scalar qtype corresponding to the qtype `x`.",
            FingerprintHasher("::arolla::expr_operators::GetScalarQTypeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    if (!inputs[0].qtype()) {
      return ExprAttributes{};
    }
    if (inputs[0].qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a qtype, got x: %s", inputs[0].qtype()->name()));
    }
    if (!inputs[0].qvalue()) {
      return ExprAttributes(GetQTypeQType());
    }
    const auto* x = inputs[0].qvalue()->UnsafeAs<QTypePtr>();
    return ExprAttributes(
        TypedRef::FromValue(GetScalarQType(x).value_or(GetNothingQType())));
  }
};

class GetShapeQTypeOp final : public BackendExprOperatorTag,
                              public ExprOperatorWithFixedSignature {
 public:
  GetShapeQTypeOp()
      : ExprOperatorWithFixedSignature(
            "qtype.get_shape_qtype", ExprOperatorSignature{{"x"}},
            "Returns the corresponding shape qtype.",
            FingerprintHasher("::arolla::expr_operators::GetShapeQTypeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    if (!inputs[0].qtype()) {
      return ExprAttributes{};
    }
    if (inputs[0].qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a qtype, got x: %s", inputs[0].qtype()->name()));
    }
    if (!inputs[0].qvalue()) {
      return ExprAttributes(GetQTypeQType());
    }
    const auto* x = inputs[0].qvalue()->UnsafeAs<QTypePtr>();
    return ExprAttributes(
        TypedRef::FromValue(absl::StatusOr<QTypePtr>(GetShapeQType(x))
                                .value_or(GetNothingQType())));
  }
};

// seq.zip
class SeqZipOp final : public BackendExprOperatorTag,
                       public expr::ExprOperatorWithFixedSignature {
 public:
  SeqZipOp()
      : ExprOperatorWithFixedSignature(
            "seq.zip", ExprOperatorSignature::MakeVariadicArgs(),
            "Scans sequences in parallel, producing tuples with a field from "
            "each one."
            "\n\n"
            "There has to be at least one sequence, and all the sequences "
            "should have\n"
            "the same size.\n"
            "\n"
            "Example:\n"
            ">>> seq.zip(rl.types.Sequence(1, 2, 3), rl.types.Sequence('a', "
            "'b', 'c'))"
            "\n"
            "Sequence(Tuple(1, 'a'), Tuple(2, 'b'), Tuple(3, 'c'))",
            FingerprintHasher("::arolla::expr_operators::SeqZipOp").Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const override {
    if (inputs.empty()) {
      return absl::InvalidArgumentError("at least one argument is expected");
    }
    for (const auto& input : inputs) {
      if (input.qtype() && !IsSequenceQType(input.qtype())) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected a sequence, got %s", input.qtype()->name()));
      }
    }
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    return ExprAttributes(GetSequenceQType(
        MakeTupleQType(GetValueQTypes(GetAttrQTypes(inputs)))));
  }
};

class StringsStaticDecodeOp final : public ExprOperatorWithFixedSignature {
 public:
  StringsStaticDecodeOp()
      : ExprOperatorWithFixedSignature(
            "strings.static_decode", {{"x"}},
            "Converts a bytes literal to text (using utf-8 coding)",
            FingerprintHasher("arolla::expr_operators::StringStaticDecodeOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& x_attr = inputs[0];
    if (x_attr.qtype() != nullptr && x_attr.qtype() != GetQType<Bytes>() &&
        x_attr.qtype() != GetQType<OptionalValue<Bytes>>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a bytes literal, got x: %s", x_attr.qtype()->name()));
    }
    if (x_attr.qtype() != nullptr && !x_attr.qvalue().has_value()) {
      return absl::InvalidArgumentError("`x` must be a literal");
    }
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    auto* output_qtype = BroadcastQType({x_attr.qtype()}, GetQType<Text>());
    if (output_qtype == nullptr) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "failed to infer output qtype for x: %s", x_attr.qtype()->name()));
    }
    // Delegate evaluation to the qexpr implementation of the operator.
    // We could call the implementation directly, but this would require us to
    // add the ICU library to our dependencies.
    ASSIGN_OR_RETURN(
        auto result_qvalue,
        InvokeOperator("strings.decode", {*x_attr.qvalue()}, output_qtype));
    return ExprAttributes(result_qvalue);
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qvalue().has_value()) {
      return node;  // We don't know the value so we're not ready for lowering.
    }
    return Literal(*node->qvalue());
  }
};

}  // namespace

// low-level kind-and-shape operators

AROLLA_DEFINE_EXPR_OPERATOR(CoreMap, RegisterOperator<MapOperator>("core.map"));

AROLLA_DEFINE_EXPR_OPERATOR(CoreToWeakFloat,
                            RegisterOperator("core._to_weak_float",
                                             MakeCoreToWeakFloatOperator()));

AROLLA_DEFINE_EXPR_OPERATOR(CoreCast, RegisterOperator<CastOp>("core.cast"));

AROLLA_DEFINE_EXPR_OPERATOR(CoreCastValues,
                            RegisterOperator<CastValuesOp>("core.cast_values"));

AROLLA_DEFINE_EXPR_OPERATOR(
    CoreEqual,
    RegisterBackendOperator(
        "core.equal", ExprOperatorSignature{{"x"}, {"y"}},
        Chain(Binary, CommonType,
              Or(ToTestResult, Chain(Is<QTypePtr>, Returns<OptionalUnit>))),
        "Returns the presence value of (x == y) element-wise."));

AROLLA_DEFINE_EXPR_OPERATOR(
    CoreMakeTuple,
    RegisterOperator<expr::MakeTupleOperator>("core.make_tuple"));
AROLLA_DEFINE_EXPR_OPERATOR(CoreGetNth, RegisterOperator("core.get_nth",
                                                         MakeCoreGetNthOp()));
AROLLA_DEFINE_EXPR_OPERATOR(CoreApplyVarargs,
                            RegisterOperator("core.apply_varargs",
                                             MakeApplyVarargsOperator()));
AROLLA_DEFINE_EXPR_OPERATOR(
    CoreNotEqual,
    RegisterBackendOperator(
        "core.not_equal", ExprOperatorSignature{{"x"}, {"y"}},
        Chain(Binary, CommonType,
              Or(ToTestResult, Chain(Is<QTypePtr>, Returns<OptionalUnit>))),
        "Returns the presence value of (x != y) element-wise."));

AROLLA_DEFINE_EXPR_OPERATOR(
    CorePresenceAnd,
    RegisterBackendOperator(
        "core.presence_and", ExprOperatorSignature{{"x"}, {"y"}},
        Chain(Binary, NthMatch(1, ScalarTypeIs<Unit>),
              Chain(Broadcast,
                    Or(NthMatch(1, Is<Unit>),      // If `y` is optional_unit,
                       NthApply(0, ToOptional))),  // make `x` optional.
              Nth(0)),
        "Returns the value of `x` iff the unit-valued `y` is present "
        "element-wise."));

AROLLA_DEFINE_EXPR_OPERATOR(
    CorePresenceOr,
    RegisterBackendOperator(
        "core.presence_or", ExprOperatorSignature{{"x"}, {"y"}},
        Chain(Binary, Broadcast, PresenceOrType),
        "Returns the value of `x` if present, else `y` element-wise."));

AROLLA_INITIALIZER(
        .name = "arolla_operators/standard:bootstrap",
        .reverse_deps = {arolla::initializer_dep::kOperators},
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(RegisterCoreCast());
          RETURN_IF_ERROR(RegisterCoreCastValues());
          RETURN_IF_ERROR(RegisterCoreMap());
          RETURN_IF_ERROR(RegisterCoreToWeakFloat());

          RETURN_IF_ERROR(RegisterCoreApplyVarargs());
          RETURN_IF_ERROR(RegisterCoreEqual());
          RETURN_IF_ERROR(RegisterCoreMakeTuple());
          RETURN_IF_ERROR(RegisterCoreGetNth());
          RETURN_IF_ERROR(RegisterCoreNotEqual());
          RETURN_IF_ERROR(RegisterCorePresenceAnd());
          RETURN_IF_ERROR(RegisterCorePresenceOr());

          // Operators that we cannot declare in the standard operator package
          // yet.
          RETURN_IF_ERROR(
              RegisterOperator<ExportAnnotation>("annotation.export").status());
          RETURN_IF_ERROR(
              RegisterOperator<ExportValueAnnotation>("annotation.export_value")
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator<NameAnnotation>("annotation.name").status());
          RETURN_IF_ERROR(
              RegisterOperator<QTypeAnnotation>("annotation.qtype").status());

          RETURN_IF_ERROR(
              RegisterOperator("core.apply", MakeCoreApplyOp()).status());
          RETURN_IF_ERROR(
              RegisterOperator("core.zip", MakeCoreZipOp()).status());
          RETURN_IF_ERROR(
              RegisterOperator("core.map_tuple", MakeCoreMapTupleOp())
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator("core.reduce_tuple", MakeCoreReduceTupleOp())
                  .status());
          RETURN_IF_ERROR(RegisterOperator("core.concat_tuples",
                                           MakeCoreConcatTuplesOperator())
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator("namedtuple._make", MakeNamedtupleMakeOp())
                  .status());
          RETURN_IF_ERROR(RegisterOperator("namedtuple.get_field",
                                           MakeNamedtupleGetFieldOp())
                              .status());

          RETURN_IF_ERROR(
              RegisterOperator("core.coalesce_units", MakeCoreCoalesceUnitsOp())
                  .status());
          RETURN_IF_ERROR(RegisterOperator("core.default_if_unspecified",
                                           MakeCoreDefaultIfUnspecifiedOp())
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator("seq.map", SeqMapOperator::Make()).status());
          RETURN_IF_ERROR(
              RegisterOperator("seq.reduce", SeqReduceOperator::Make())
                  .status());
          RETURN_IF_ERROR(RegisterOperator<SeqZipOp>("seq.zip").status());
          RETURN_IF_ERROR(
              RegisterOperator<QTypeOfOp>("qtype.qtype_of").status());
          RETURN_IF_ERROR(RegisterOperator<BroadcastQTypeLikeOp>(
                              "qtype.broadcast_qtype_like")
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator<CommonQTypeOp>("qtype.common_qtype").status());
          RETURN_IF_ERROR(
              RegisterOperator<GetScalarQTypeOp>("qtype.get_scalar_qtype")
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator<GetShapeQTypeOp>("qtype.get_shape_qtype")
                  .status());
          RETURN_IF_ERROR(RegisterOperator("derived_qtype.upcast",
                                           MakeDerivedQTypeUpcastOp())
                              .status());
          RETURN_IF_ERROR(RegisterOperator("derived_qtype.downcast",
                                           MakeDerivedQTypeDowncastOp())
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator<MakeSliceQTypeOperator>("qtype.make_slice_qtype")
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator<MakeDictQTypeOp>("qtype.make_dict_qtype")
                  .status());

          RETURN_IF_ERROR(
              RegisterOperator<StringsStaticDecodeOp>("strings.static_decode")
                  .status());

          // Operators for constants that we cannot serialize with a minimal set
          // of codecs.
          RETURN_IF_ERROR(
              RegisterOperator("qtype._const_scalar_shape",
                               MakeLambdaOperator(ExprOperatorSignature{},
                                                  Literal(ScalarShape{})))
                  .status());
          RETURN_IF_ERROR(RegisterOperator("qtype._const_optional_scalar_shape",
                                           MakeLambdaOperator(
                                               ExprOperatorSignature{},
                                               Literal(OptionalScalarShape{})))
                              .status());
          RETURN_IF_ERROR(
              RegisterOperator("qtype._const_empty_dense_array_shape",
                               MakeLambdaOperator(ExprOperatorSignature{},
                                                  Literal(DenseArrayShape{0})))
                  .status());
          RETURN_IF_ERROR(
              RegisterOperator("qtype._const_empty_array_shape",
                               MakeLambdaOperator(ExprOperatorSignature{},
                                                  Literal(ArrayShape{0})))
                  .status());
          RETURN_IF_ERROR(RegisterOperator(
                              "qtype._const_regex_qtype",
                              MakeLambdaOperator(ExprOperatorSignature{},
                                                 Literal(GetQType<RegexPtr>())))
                              .status());
          return absl::OkStatus();
        })

}  // namespace arolla::expr_operators

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
#include "arolla/expr/operators/tuple_bootstrap_operators.h"

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/derived_qtype_cast_operator.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/tuple_expr_operator.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::expr_operators {
namespace {

using ::arolla::expr::BasicExprOperator;
using ::arolla::expr::CallOp;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorWithFixedSignature;
using ::arolla::expr::GetNthOperator;
using ::arolla::expr::Literal;
using ::arolla::expr::MakeOpNode;

// core.apply_varargs operator implementation
class CoreApplyVarargsOperator final : public ExprOperatorWithFixedSignature {
 public:
  CoreApplyVarargsOperator()
      : ExprOperatorWithFixedSignature(
            "core.apply_varargs",
            ExprOperatorSignature{{"op"},
                                  {.name = "args_with_tuple_at_end",
                                   .kind = ExprOperatorSignature::Parameter::
                                       Kind::kVariadicPositional}},
            "Applies the operator to args, unpacking the last one.\n"
            "\n"
            "The operator is most useful to unpack varargs tuple inside\n"
            "lambda body, although it can be used in other contexts.\n"
            "\n"
            "Args:\n"
            "  op: operator to apply, must be a literal.\n"
            "  *args_with_tuple_at_end: arguments to pass to the operator:\n"
            "    all except the last one will be passed as is. The last one \n"
            "    must be a tuple that will be unpacked.\n"
            "\n"
            "Returns:\n"
            "  op(*args_with_tuple_at_end[:-1], *args_with_tuple_at_end[-1])",
            FingerprintHasher(
                "arolla::expr_operators::CoreApplyVarargsOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    if (inputs.size() < 2) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "too few arguments: expected at least 2, got %d", inputs.size()));
    }
    const auto& op = inputs[0];
    const auto& args_tuple = inputs[inputs.size() - 1];
    if (op.qtype() && op.qtype() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected an operator, got op: %s", op.qtype()->name()));
    }
    if (args_tuple.qtype() && !IsTupleQType(args_tuple.qtype())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a tuple, got args_tuple: %s", args_tuple.qtype()->name()));
    }
    if (op.qtype() && !op.qvalue()) {
      return absl::InvalidArgumentError("`op` must be literal");
    }
    if (!expr::HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    std::vector<ExprAttributes> op_input_attrs;
    op_input_attrs.reserve(inputs.size() - 2 +
                           args_tuple.qtype()->type_fields().size());
    op_input_attrs.insert(op_input_attrs.end(), std::next(inputs.begin()),
                          std::prev(inputs.end()));
    for (const auto& field : args_tuple.qtype()->type_fields()) {
      op_input_attrs.emplace_back(field.GetType());
    }
    return op.qvalue()->UnsafeAs<ExprOperatorPtr>()->InferAttributes(
        op_input_attrs);
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    if (node->node_deps().size() < 2) {
      return absl::InvalidArgumentError(
          absl::StrFormat("too few arguments: expected at least 2, got %d",
                          node->node_deps().size()));
    }
    if (!node->qtype()) {
      return node;  // We are not ready for lowering yet.
    }
    const auto& op_expr = node->node_deps()[0];
    const auto& op_qvalue = op_expr->qvalue();
    const auto& tuple_expr = node->node_deps()[node->node_deps().size() - 1];
    const auto* tuple_qtype = tuple_expr->qtype();
    if (!op_qvalue.has_value() || tuple_qtype == nullptr) {
      return node;
    }
    std::vector<ExprNodePtr> args;
    args.reserve(node->node_deps().size() - 2 +
                 tuple_qtype->type_fields().size());
    args.insert(args.end(), std::next(node->node_deps().begin()),
                std::prev(node->node_deps().end()));

    for (size_t i = 0; i < tuple_qtype->type_fields().size(); ++i) {
      ASSIGN_OR_RETURN(
          args.emplace_back(),
          expr::MakeOpNode(std::make_shared<GetNthOperator>(i), {tuple_expr}));
    }
    ASSIGN_OR_RETURN(const ExprOperatorPtr& op,
                     op_qvalue->As<ExprOperatorPtr>());
    return MakeOpNode(op, std::move(args));
  }
};

absl::StatusOr<QTypePtr> UnwrapFieldQType(QTypePtr value_qtype, size_t n) {
  const auto& value_qtype_fields = value_qtype->type_fields();
  if (value_qtype_fields.empty()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected 'value' to be a compound type, got %s", value_qtype->name()));
  }
  if (n >= value_qtype_fields.size()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("'n' is out of range: n=%d, %s has only %d fields", n,
                        value_qtype->name(), value_qtype_fields.size()));
  }
  return value_qtype_fields[n].GetType();
}

absl::Status CheckHasSubfieldsOrIsTuple(absl::string_view operator_name,
                                        QTypePtr qtype) {
  if (qtype->type_fields().empty() && !IsTupleQType(qtype)) {
    return absl::InvalidArgumentError(absl::StrCat(
        operator_name, " received non-tuple object with no fields"));
  }
  return absl::OkStatus();
}

absl::StatusOr<std::optional<size_t>> UnwrapTupleSize(ExprNodePtr tuple_node) {
  const QType* tuple_type = tuple_node->qtype();
  if (tuple_type == nullptr) {
    return std::nullopt;
  }
  RETURN_IF_ERROR(CheckHasSubfieldsOrIsTuple("core.zip", tuple_type));
  return tuple_type->type_fields().size();
}

// core.get_nth operator
class CoreGetNthOp final : public ExprOperatorWithFixedSignature {
 public:
  CoreGetNthOp()
      : ExprOperatorWithFixedSignature(
            "core.get_nth", ExprOperatorSignature{{"value"}, {"n"}},
            "Returns the n-th field of a compound value.",
            FingerprintHasher("arolla::expr_operators::CoreGetNthOp")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& value_attr = inputs[0];
    const auto& n_attr = inputs[1];
    if (n_attr.qtype() != nullptr && !n_attr.qvalue().has_value()) {
      return absl::InvalidArgumentError("`n` must be literal");
    }
    if (n_attr.qtype() == nullptr) {
      return ExprAttributes();
    }
    ASSIGN_OR_RETURN(int64_t n, UnwrapN(*n_attr.qvalue()));
    return GetNthOperator::StaticInferAttributes(n, value_attr);
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    const auto& n_attr = node->node_deps()[1];
    if (!n_attr->qvalue().has_value()) {
      return node;
    }
    ASSIGN_OR_RETURN(int64_t n, UnwrapN(*n_attr->qvalue()));
    return expr::MakeOpNode(std::make_shared<GetNthOperator>(n),
                            {node->node_deps()[0]});
  }

 private:
  static absl::StatusOr<int64_t> UnwrapN(const TypedValue& n_qvalue) {
    const auto* n_qtype = n_qvalue.GetType();
    bool found = false;
    std::optional<int64_t> n;
    using IntegerTypes = meta::type_list<int32_t, int64_t>;
    meta::foreach_type<IntegerTypes>([&](auto meta_type) {
      using T = typename decltype(meta_type)::type;
      if (n_qtype == GetQType<T>()) {
        found = true;
        n = n_qvalue.UnsafeAs<T>();
      } else if (n_qtype == GetQType<OptionalValue<T>>()) {
        found = true;
        n = n_qvalue.UnsafeAs<OptionalValue<T>>().AsOptional();
      }
    });
    if (!found) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected an integer, got n: %s", n_qtype->name()));
    }
    if (!n.has_value()) {
      return absl::InvalidArgumentError("expected an integer, got n=missing");
    }
    if (*n < 0) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a non-negative integer, got n=%d", *n));
    }
    return *n;
  }
};

class CoreZipOp final : public BasicExprOperator {
 public:
  CoreZipOp()
      : BasicExprOperator(
            "core.zip", ExprOperatorSignature::MakeVariadicArgs(),
            "Scans several tuples in parallel, producing tuples with a field "
            "from each one.",
            FingerprintHasher("arolla::expr_operators:CoreZipOp").Finish()) {}

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    if (node->node_deps().empty()) {
      return Literal(MakeTupleFromFields());
    }
    auto outer_size = node->node_deps().size();
    ASSIGN_OR_RETURN(auto inner_size, UnwrapTupleSize(node->node_deps()[0]));
    if (!inner_size.has_value()) {
      return node;  // First argument is not lowered to a tuple yet.
    }

    // Check that all arguments are tuples of the same size.
    for (int i = 1; i < outer_size; ++i) {
      const auto& item_node = node->node_deps()[i];
      ASSIGN_OR_RETURN(auto item_size, UnwrapTupleSize(item_node));
      if (!item_size.has_value()) {
        return node;  // Argument is not lowered to a tuple yet.
      }

      if (*item_size != *inner_size) {
        return absl::InvalidArgumentError(
            absl::StrFormat("all tuple arguments must be of the same size, but "
                            "got %i vs %i for %i-th argument",
                            *inner_size, *item_size, i));
      }
    }

    // Repack the arguments.
    std::vector<absl::StatusOr<ExprNodePtr>> result_list;
    result_list.reserve(*inner_size);
    for (int j = 0; j < inner_size; ++j) {
      std::vector<absl::StatusOr<ExprNodePtr>> inner_tuples;
      inner_tuples.reserve(outer_size);
      for (int i = 0; i < outer_size; ++i) {
        inner_tuples.push_back(
            CallOp("core.get_nth", {node->node_deps()[i], Literal(j)}));
      }
      result_list.push_back(CallOp("core.make_tuple", inner_tuples));
    }
    return CallOp("core.make_tuple", result_list);
  }

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    if (input_qtypes.empty()) {
      return MakeTupleQType({});
    }
    auto outer_size = input_qtypes.size();
    auto inner_size = input_qtypes[0]->type_fields().size();

    // Check that all arguments are tuples.
    for (QTypePtr qtype : input_qtypes) {
      RETURN_IF_ERROR(CheckHasSubfieldsOrIsTuple("core.zip", qtype));
    }

    // Check that all arguments are tuples of the same size.
    for (int i = 1; i < outer_size; ++i) {
      if (input_qtypes[i]->type_fields().size() != inner_size) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "all tuple arguments must be of the same size, but "
            "got %i vs %i for %i-th argument",
            inner_size, input_qtypes[i]->type_fields().size(), i));
      }
    }

    // Repack the arguments.
    std::vector<QTypePtr> result_list;
    result_list.reserve(inner_size);
    for (int j = 0; j < inner_size; ++j) {
      std::vector<QTypePtr> qtypes;
      qtypes.reserve(outer_size);
      for (auto* input_qtype : input_qtypes) {
        ASSIGN_OR_RETURN(auto item_type, UnwrapFieldQType(input_qtype, j));
        qtypes.push_back(item_type);
      }
      result_list.push_back(MakeTupleQType(qtypes));
    }
    return MakeTupleQType(result_list);
  }
};

// Reduce operator.
class CoreReduceTupleOperator final : public ExprOperatorWithFixedSignature {
 public:
  CoreReduceTupleOperator()
      : ExprOperatorWithFixedSignature(
            "core.reduce_tuple", ExprOperatorSignature{{"op"}, {"tuple"}},
            "Applies the given (literal) operator cumulatively to the tuple.\n"
            "\n"
            "The operator must be a binary operator that will be applied on \n"
            "the elements of the tuple from left to right reducing them to a \n"
            "single value.\n"
            "\n"
            "Example:\n"
            "  # Equivalent to: `(1.0 + 2.0) + 3.0`.\n"
            "  M.core.reduce_tuple(M.math.add, (1.0, 2.0, 3.0))\n"
            "\n"
            "Args:\n"
            "  op: binary operator to apply.\n"
            "  tuple: tuple of elements to reduce using the provided op.",
            FingerprintHasher("arolla::expr_operators::CoreReduceTupleOperator")
                .Finish()) {}

  static absl::Status CheckArgs(const QType* op_qtype, const QType* tuple_qtype,
                                const std::optional<TypedValue>& op_qvalue) {
    if (op_qtype != nullptr) {
      if (op_qtype != GetQType<ExprOperatorPtr>()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected %s, got op:%s", GetQType<ExprOperatorPtr>()->name(),
            op_qtype->name()));
      }
      if (!op_qvalue.has_value()) {
        return absl::InvalidArgumentError("`op` must be literal");
      }
      DCHECK(op_qvalue->GetType() == op_qtype);
      const auto& op = op_qvalue->UnsafeAs<ExprOperatorPtr>();
      ASSIGN_OR_RETURN(auto op_sig, op->GetSignature());
      if (!ValidateDepsCount(op_sig, 2, absl::StatusCode::kInvalidArgument)
               .ok()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected a binary operator, got %s", op_qvalue->Repr()));
      }
    }
    if (tuple_qtype != nullptr) {
      const auto& tuple_field_slots = tuple_qtype->type_fields();
      if (!IsTupleQType(tuple_qtype) && tuple_field_slots.empty()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "expected a tuple, got tuple: %s", tuple_qtype->name()));
      }
      if (tuple_field_slots.empty()) {
        return absl::InvalidArgumentError("unable to reduce an empty tuple");
      }
    }
    return absl::OkStatus();
  }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& op_attr = inputs[0];
    const auto* tuple_qtype = inputs[1].qtype();
    RETURN_IF_ERROR(CheckArgs(op_attr.qtype(), tuple_qtype, op_attr.qvalue()));
    if (!op_attr.qtype() || !tuple_qtype) {
      return ExprAttributes{};
    }
    DCHECK(op_attr.qvalue() &&
           op_attr.qvalue()->GetType() == GetQType<ExprOperatorPtr>());
    DCHECK(!tuple_qtype->type_fields().empty());
    const auto& op = op_attr.qvalue()->UnsafeAs<ExprOperatorPtr>();
    const auto& tuple_field_slots = tuple_qtype->type_fields();
    auto result = ExprAttributes(tuple_field_slots[0].GetType());
    for (size_t i = 1; i < tuple_field_slots.size(); ++i) {
      ASSIGN_OR_RETURN(
          result,
          op->InferAttributes(
              {result, ExprAttributes(tuple_field_slots[i].GetType())}));
    }
    return result;
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;  // We are not ready for lowering yet.
    }
    const auto& op_attr = node->node_deps()[0]->attr();
    const auto& tuple_expr = node->node_deps()[1];
    const auto* tuple_qtype = tuple_expr->qtype();
    if (!op_attr.qvalue() || !tuple_qtype) {
      return node;
    }
    DCHECK(op_attr.qvalue()->GetType() == GetQType<ExprOperatorPtr>());
    DCHECK(!tuple_qtype->type_fields().empty());
    const auto& op = op_attr.qvalue()->UnsafeAs<ExprOperatorPtr>();
    const auto& tuple_field_slots = tuple_qtype->type_fields();
    auto tuple_get_nth_expr = [&](int64_t i) {
      return CallOp("core.get_nth", {tuple_expr, Literal<int64_t>(i)});
    };
    ASSIGN_OR_RETURN(ExprNodePtr result, tuple_get_nth_expr(0));
    for (size_t i = 1; i < tuple_field_slots.size(); ++i) {
      ASSIGN_OR_RETURN(result, CallOp(op, {result, tuple_get_nth_expr(i)}));
    }
    return result;
  }
};

// core.concat_tuples operator implementation.
class CoreConcatTuplesOperator final : public BasicExprOperator {
 public:
  CoreConcatTuplesOperator()
      : BasicExprOperator(
            "core.concat_tuples", ExprOperatorSignature::MakeVariadicArgs(),
            "Concatenates two given tuples into one.",
            FingerprintHasher(
                "arolla::expr_operators::CoreConcatTuplesOperator")
                .Finish()) {}

  absl::StatusOr<QTypePtr> GetOutputQType(
      absl::Span<const QTypePtr> input_qtypes) const final {
    std::vector<QTypePtr> result_field_types;
    for (QTypePtr qtype : input_qtypes) {
      if (qtype != nullptr && !IsTupleQType(qtype)) {
        return absl::InvalidArgumentError(
            absl::StrFormat("expected a tuple, got %s", qtype->name()));
      }
      if (qtype == nullptr) {
        return nullptr;
      }
      for (const auto& field : qtype->type_fields()) {
        result_field_types.emplace_back(field.GetType());
      }
    }
    return MakeTupleQType(result_field_types);
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    std::vector<ExprNodePtr> args;
    for (const auto& dep : node->node_deps()) {
      if (dep->qtype() == nullptr) {
        return node;
      }
      for (size_t i = 0; i < dep->qtype()->type_fields().size(); ++i) {
        ASSIGN_OR_RETURN(args.emplace_back(),
                         CallOp(GetNthOperator::Make(i), {dep}));
      }
    }
    return BindOp("core.make_tuple", std::move(args), {});
  }
};

class CoreMapTupleOperator final : public ExprOperatorWithFixedSignature {
 public:
  CoreMapTupleOperator()
      : ExprOperatorWithFixedSignature(
            "core.map_tuple", ExprOperatorSignature{{"op"}, {"tuple"}},
            "Applies the given op to each of the tuple elements.",
            FingerprintHasher("arolla::expr_operators::CoreMapTupleOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& op_attr = inputs[0];
    const auto* tuple_qtype = inputs[1].qtype();
    if (op_attr.qtype() && op_attr.qtype() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected %s, got op: %s", GetQType<ExprOperatorPtr>()->name(),
          op_attr.qtype()->name()));
    }
    if (op_attr.qtype() && !op_attr.qvalue().has_value()) {
      return absl::InvalidArgumentError("`op` must be literal");
    }
    if (tuple_qtype && !IsTupleQType(tuple_qtype)) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a tuple, got tuple: %s", tuple_qtype->name()));
    }
    if (!op_attr.qtype() || !tuple_qtype) {
      return ExprAttributes{};
    }

    const auto& op = op_attr.qvalue()->UnsafeAs<ExprOperatorPtr>();
    std::vector<QTypePtr> result_types;
    result_types.reserve(tuple_qtype->type_fields().size());
    for (const auto& f : tuple_qtype->type_fields()) {
      ASSIGN_OR_RETURN(
          auto result_attr, op->InferAttributes({ExprAttributes(f.GetType())}),
          _ << "while infering output type for operator " << op->display_name()
            << "(" << f.GetType()->name() << ")");
      if (result_attr.qtype() == nullptr) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "unable to infer `op` (%s) output type for input type %s",
            op->display_name(), f.GetType()->name()));
      }
      result_types.push_back(result_attr.qtype());
    }
    return ExprAttributes(MakeTupleQType(result_types));
  }

  absl::StatusOr<ExprNodePtr> ToLowerLevel(
      const ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;  // We are not ready for lowering yet.
    }
    const auto& op_attr = node->node_deps()[0]->attr();
    const auto& tuple_expr = node->node_deps()[1];
    const auto* tuple_qtype = tuple_expr->qtype();
    DCHECK(op_attr.qvalue().has_value());
    DCHECK(op_attr.qvalue()->GetType() == GetQType<ExprOperatorPtr>());
    DCHECK(tuple_qtype != nullptr);
    const auto& op = op_attr.qvalue()->UnsafeAs<ExprOperatorPtr>();
    auto tuple_get_nth_expr = [&](int64_t i) {
      return CallOp("core.get_nth", {tuple_expr, Literal<int64_t>(i)});
    };
    std::vector<ExprNodePtr> deps;
    deps.reserve(tuple_qtype->type_fields().size());
    for (size_t i = 0; i < tuple_qtype->type_fields().size(); ++i) {
      ASSIGN_OR_RETURN(deps.emplace_back(),
                       CallOp(op, {tuple_get_nth_expr(i)}));
    }
    return BindOp("core.make_tuple", deps, {});
  }
};

absl::StatusOr<absl::string_view> UnwrapFieldName(TypedRef value) {
  if (value.GetType() == GetQType<Text>()) {
    return value.UnsafeAs<Text>().view();
  }
  return absl::InvalidArgumentError(
      absl::StrCat("field_name must be ", GetQType<Text>()->name(),
                   ", found: ", value.GetType()->name()));
}

absl::StatusOr<std::vector<std::string>> UnwrapFieldNames(TypedRef value) {
  if (value.GetType() == GetQType<Text>()) {
    return std::vector<std::string>(
        absl::StrSplit(value.UnsafeAs<Text>().view(), absl::ByAnyChar(", "),
                       absl::SkipEmpty()));
  }
  if (IsTupleQType(value.GetType())) {
    std::vector<std::string> names;
    names.reserve(value.GetFieldCount());
    for (int i = 0; i < value.GetFieldCount(); ++i) {
      if (value.GetField(i).GetType() != GetQType<Text>()) {
        return absl::InvalidArgumentError(absl::StrFormat(
            "all field_names must be TEXTs, got %s for field %d",
            value.GetField(i).GetType()->name(), i + 1));
      }
      names.push_back(std::string(value.GetField(i).UnsafeAs<Text>().view()));
    }
    return names;
  }

  if (value.GetType() == GetDenseArrayQType<Bytes>()) {
    auto arr = value.UnsafeAs<DenseArray<Bytes>>();
    std::vector<std::string> names;
    names.reserve(arr.size());
    for (const auto& name : arr) {
      if (!name.present) {
        return absl::InvalidArgumentError("all field_names must be present");
      }
      names.push_back(std::string(name.value));
    }
    return names;
  }
  return absl::InvalidArgumentError(absl::StrCat(
      "field_names must be ", GetQType<Text>()->name(),
      ", a tuple thereof, or an array of ", GetQType<Bytes>()->name(),
      ", found: ", value.GetType()->name()));
}

class MakeNamedTupleOperator final : public ExprOperatorWithFixedSignature {
 public:
  MakeNamedTupleOperator()
      : ExprOperatorWithFixedSignature(
            "namedtuple._make",
            ExprOperatorSignature{{"field_names"}, {"field_values"}},
            "(internal) Returns a namedtuple with the given fields.",
            FingerprintHasher("arolla::expr::MakeNamedTupleOperator")
                .Finish()) {}

  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;  // We are not ready for lowering yet.
    }
    auto named_tuple_type = node->qtype();
    if (!named_tuple_type) {
      return node;
    }
    if (!IsNamedTupleQType(named_tuple_type)) {
      return absl::InternalError(
          absl::StrCat("incorrect namedtuple._make output type: ",
                       named_tuple_type->name()));
    }
    const auto& regular_tuple = node->node_deps()[1];
    ExprOperatorPtr cast_to_op =
        std::make_shared<expr::DerivedQTypeDowncastOperator>(named_tuple_type);
    return CallOp(std::move(cast_to_op), {regular_tuple});
  }

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& field_names = inputs[0];
    const auto& tuple = inputs[1];
    if (!field_names.qtype()) {
      return ExprAttributes{};
    }
    if (!field_names.qvalue()) {
      return absl::InvalidArgumentError("field_names must be literal");
    }
    ASSIGN_OR_RETURN(std::vector<std::string> names,
                     UnwrapFieldNames(field_names.qvalue()->AsRef()));
    if (!tuple.qtype()) {
      return ExprAttributes{};
    }
    ASSIGN_OR_RETURN(auto* output_qtype,
                     MakeNamedTupleQType(names, tuple.qtype()));
    return ExprAttributes(output_qtype);
  }
};

class GetNamedTupleFieldOperator final : public ExprOperatorWithFixedSignature {
 public:
  GetNamedTupleFieldOperator()
      : ExprOperatorWithFixedSignature(
            "namedtuple.get_field",
            ExprOperatorSignature{{"namedtuple"}, {"field_name"}},
            "Returns the field value by name.",
            FingerprintHasher("arolla::expr::GetNamedTupleFieldOperator")
                .Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    const auto& named_tuple_qtype = inputs[0].qtype();
    const auto& field_name_tv = inputs[1].qvalue();
    if (!field_name_tv.has_value()) {
      return absl::InvalidArgumentError("field_name must be literal");
    }
    ASSIGN_OR_RETURN(absl::string_view field_name,
                     UnwrapFieldName(field_name_tv->AsRef()));
    QTypePtr res_qtype = GetFieldQTypeByName(named_tuple_qtype, field_name);
    if (res_qtype == nullptr) {
      return absl::InvalidArgumentError(
          absl::StrFormat("field_name='%s' is not found in %s", field_name,
                          named_tuple_qtype->name()));
    }
    return ExprAttributes(res_qtype);
  }

  absl::StatusOr<expr::ExprNodePtr> ToLowerLevel(
      const expr::ExprNodePtr& node) const final {
    RETURN_IF_ERROR(ValidateNodeDepsCount(*node));
    if (!node->qtype()) {
      return node;  // We are not ready for lowering yet.
    }
    const auto& tuple = node->node_deps()[0];
    const auto* named_tuple_type = tuple->qtype();
    if (!named_tuple_type) {
      return node;
    }
    const auto& field_name_expr = node->node_deps()[1];
    QTypePtr field_name_type = field_name_expr->qtype();
    if (field_name_type == nullptr) {
      return node;
    }

    const std::optional<TypedValue>& field_name_tv = field_name_expr->qvalue();
    if (!field_name_tv.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrCat("field name must be literal, expr: ",
                       GetDebugSnippet(field_name_expr)));
    }
    ASSIGN_OR_RETURN(absl::string_view field_name,
                     UnwrapFieldName(field_name_tv->AsRef()));
    std::optional<int64_t> id =
        GetFieldIndexByName(named_tuple_type, field_name);
    if (!id.has_value()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("field_name='%s' is not found in %s", field_name,
                          named_tuple_type->name()));
    }

    ASSIGN_OR_RETURN(ExprOperatorPtr get_op, GetNthOperator::Make(*id));
    return CallOp(std::move(get_op), {tuple});
  }
};

}  // namespace

ExprOperatorPtr MakeApplyVarargsOperator() {
  return std::make_shared<CoreApplyVarargsOperator>();
}

ExprOperatorPtr MakeCoreGetNthOp() { return std::make_shared<CoreGetNthOp>(); }

ExprOperatorPtr MakeCoreZipOp() { return std::make_shared<CoreZipOp>(); }

ExprOperatorPtr MakeCoreReduceTupleOp() {
  return std::make_shared<CoreReduceTupleOperator>();
}

ExprOperatorPtr MakeCoreConcatTuplesOperator() {
  return std::make_shared<CoreConcatTuplesOperator>();
}

ExprOperatorPtr MakeCoreMapTupleOp() {
  return std::make_shared<CoreMapTupleOperator>();
}

ExprOperatorPtr MakeNamedtupleGetFieldOp() {
  return std::make_shared<GetNamedTupleFieldOperator>();
}

ExprOperatorPtr MakeNamedtupleMakeOp() {
  return std::make_shared<MakeNamedTupleOperator>();
}

}  // namespace arolla::expr_operators

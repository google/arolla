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
#ifndef AROLLA_CODEGEN_EXPR_CODEGEN_OPERATOR_H_
#define AROLLA_CODEGEN_EXPR_CODEGEN_OPERATOR_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"

// All required information to generate operator code.

namespace arolla::codegen {

namespace codegen_impl {
// Returns true for types that should be inlined as literal.
// These types should be cheap to create, ideally constructors should be
// constexpr.
bool IsInlinableLiteralType(const QType* /*nullable*/ qtype);
}  // namespace codegen_impl

enum struct LValueKind : int {
  // literals are global and should be computed once for all computations
  kLiteral,
  // inputs should be read from the user provided input
  kInput,
  // local variables could be defined in the function.
  // they will be referred only from statements located in the same function.
  kLocal,
};

// Description of a single variable.
struct LValue {
  // C++ cpp_type name. "auto" signal that type should be deduced.
  std::string type_name;
  // True in case error can be produced during the evaluation of this variable.
  bool is_entire_expr_status_or;
  // True in case calling an operator would produce StatusOr.
  // While entire expression may produce errors, some variables can be already
  // assigned to the local variables (with ASSIGN_OR_RETURN).
  // In that case calling this operator may return StatusOr free.
  bool is_local_expr_status_or;
  QTypePtr qtype;
  LValueKind kind;

  // Returns C++ type construction to create QType.
  // E.g., "::arolla::GetQType<float>()".
  absl::StatusOr<std::string> QTypeConstruction() const;

  friend std::ostream& operator<<(std::ostream& out, const LValue& v) {
    return out << static_cast<int>(v.kind) << " " << v.qtype->name()
               << " is_entire_expr_status_or=" << v.is_entire_expr_status_or
               << " is_local_expr_status_or=" << v.is_local_expr_status_or;
  }
  friend bool operator==(const LValue& a, const LValue& b) {
    return a.type_name == b.type_name &&
           a.is_entire_expr_status_or == b.is_entire_expr_status_or &&
           a.is_local_expr_status_or == b.is_local_expr_status_or &&
           a.qtype == b.qtype && a.kind == b.kind;
  }
};

// Id of the LValue and index to the OperatorCodegenData::assignments.
using LValueId = int64_t;

enum struct RValueKind : int {
  // code would be empty, user need to provide the way to read input
  kInput,
  // full c++ code is already generated
  kVerbatim,
  // code contains the function name, arguments need to be passed in
  kFunctionCall,
  // code contains the function name, EvaluationContext* and
  // arguments need to be passed in
  kFunctionWithContextCall,
  // code is empty, N + 1 arguments are main_output and
  // N arguments with side effects that must be not optimized
  kFirst,
  // code contains export_id as string, 1 argument is side output to export
  kOutput,
};

// Description of the RValue for the specific variable.
// Refer to RValueKind to find out different types of rvalues.
struct RValue {
  RValueKind kind;
  // whenever operator stored in `code` itself returns StatusOr
  bool operator_returns_status_or;
  std::string code;  // verbatim C++ code to call to evaluate operator
  std::vector<LValueId> argument_ids;
  // offsets of arguments that should be passed as function
  std::vector<int> argument_as_function_offsets = {};
  // comment to be added to the code.
  std::string comment;

  static RValue CreateInput() {
    return RValue{.kind = RValueKind::kInput,
                  .operator_returns_status_or = false,
                  .code = "",
                  .argument_ids = {}};
  }
  static RValue CreateLiteral(std::string code) {
    return RValue{.kind = RValueKind::kVerbatim,
                  .operator_returns_status_or = false,
                  .code = std::move(code),
                  .argument_ids = {}};
  }

  friend std::ostream& operator<<(std::ostream& out, const RValue& v) {
    return out << static_cast<int>(v.kind)
               << "returns_status_or=" << v.operator_returns_status_or << " "
               << v.code << " {" << absl::StrJoin(v.argument_ids, ",") << "}";
  }
  friend bool operator==(const RValue& a, const RValue& b) {
    return a.kind == b.kind &&
           a.operator_returns_status_or == b.operator_returns_status_or &&
           a.code == b.code && a.argument_ids == b.argument_ids;
  }
};

// Information about assignment operation.
// `type` `lvalue` = `rvalue`
class Assignment {
 public:
  Assignment() = default;

  Assignment(LValue lvalue, RValue rvalue, bool inlinable = false)
      : lvalue_(std::move(lvalue)),
        rvalue_(std::move(rvalue)),
        inlinable_(inlinable) {}

  // C++ expression of lvalue of the assignment statement.
  const LValue& lvalue() const { return lvalue_; }
  LValue& lvalue() { return lvalue_; }

  // C++ expression of lvalue of the assignment statement.
  const RValue& rvalue() const { return rvalue_; }
  RValue& rvalue() { return rvalue_; }

  // Returns whenever assignment can be inlined.
  // Leaves are always inlinable. Literals are always not inlinable.
  bool is_inlinable() const { return inlinable_; }
  void set_inlinable(bool inlinable) { inlinable_ = inlinable; }

  friend std::ostream& operator<<(std::ostream& out, const Assignment& s) {
    return out << s.lvalue_ << " = " << s.rvalue_ << ";";
  }

 private:
  LValue lvalue_;
  RValue rvalue_;
  bool inlinable_;
};

// List of assignments in increasing order, which are possible to place
// in a single separate function.
// No assignments should refer to any LOCAL lvalue outside of the function.
//
// Each function `F` is called by exactly one other function `G`
// (except the root). `G.assignment_ids` contain `F.output_id`.
//
// Exception to the all rules above: literals, output_id and inlinable
// assignments are not listed in assignment_ids.
// Note that leaves are always inlinable.
struct Function {
  std::vector<LValueId> assignment_ids;
  LValueId output_id;
  // True if the resulting type must be StatusOr.
  // For lambdas we assume that captured arguments are not StatusOr.
  bool is_result_status_or;
};

// Data required for a single operator code generation.
struct OperatorCodegenData {
  // Returns ids of literal assignments
  std::vector<LValueId> literal_ids() const {
    std::vector<LValueId> res;
    for (int64_t i = 0; i != assignments.size(); ++i) {
      if (assignments[i].lvalue().kind == LValueKind::kLiteral) {
        res.push_back(i);
      }
    }
    return res;
  }

  // Returns mapping from assignment_id to input name.
  std::map<LValueId, std::string> input_id_to_name() const {
    std::map<LValueId, std::string> res;
    for (const auto& [name, id] : inputs) {
      res.emplace(id, name);
    }
    return res;
  }

  // Returns mapping from assignment_id, which is one of the function output
  // to the corresponding function id.
  std::map<LValueId, int64_t> function_entry_points() const {
    std::map<LValueId, int64_t> res;
    size_t fn_id = 0;
    for (const auto& fn : functions) {
      res.emplace(fn.output_id, fn_id++);
    }
    return res;
  }

  std::set<std::string> deps;     // required build dependencies
  std::set<std::string> headers;  // required headers
  std::map<std::string, LValueId>
      inputs;  // mapping from input name to assignment id
  std::vector<std::pair<std::string, LValueId>>
      side_outputs;  // mapping from output name to assignment id
  std::vector<Assignment> assignments;  // evaluation statements

  // All assignments are split into functions and lambdas.
  // The main goal is to reduce lifetime of the temporary variables and as
  // result have: guaranteed smaller stack, faster compilation and potentially
  // better performance.
  //
  // Each assignment falls into one of the following category:
  // * Literal: not referred in any function or lambda
  // * Inlinable: not listed in `assignment_ids`, can be referred by `output_id`
  // * Root: specified in `OperatorCodegenData.output_id`
  // * Others: referred by exactly one function or lambda,
  //     can be specified in `output_id` of one or zero functions or lambdas.

  // Split of assignments into functions
  // All `assignment_ids` can be placed into the separate function.
  // There is guarantee that nothing except `output_id` will be used by other
  // functions.
  // Inlined and assignments within lambda are not listed.
  std::vector<Function> functions;
  // split of assignments into lambdas.
  // All `assignment_ids` can be placed into the lambda that capture
  // everything defined before it.
  // There is guarantee that nothing except `output_id` will be used outside
  // of this lambda.
  // Inlined and assignments within inner lambda are not listed. Separate entry
  // for inner lambdas will be created.
  std::vector<Function> lambdas;

  LValueId output_id;  // output variable
};

// Analyze provided expression and prepare data required for code generation.
// At the moment following requirements are applied:
// * All leaves must have QTypeMetadata.
// * All operators need to be defined via simple_operator.
// * For all literals `CppLiteralRepr` should be implemented.
// * For all input and output types `CppTypeName` should be implemented.
// * Custom types need to be registered with `RegisterCppType`.
absl::StatusOr<OperatorCodegenData> GenerateOperatorCode(
    expr::ExprNodePtr expr,
    // If true inputs considered to be stored in a global context
    // (e.g., Frame).
    // If false inputs are considered to be expensive
    // to compute and shouldn't be reevaluated many times.
    bool inputs_are_cheap_to_read);

}  // namespace arolla::codegen

#endif  // AROLLA_CODEGEN_EXPR_CODEGEN_OPERATOR_H_

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
#include "py/arolla/abc/py_eval.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/base/optimization.h"
#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "py/arolla/abc/py_aux_binding_policy.h"
#include "py/arolla/abc/py_expr.h"
#include "py/arolla/abc/py_operator.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/eval/model_executor.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/expr_visitor.h"
#include "arolla/expr/optimization/default/default_optimizer.h"
#include "arolla/io/typed_refs_input_loader.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/string.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

static constexpr size_t kCCacheSize = 1024;
static constexpr size_t kExprInfoCacheSize = 1024;

using ::arolla::expr::CompileModelExecutor;
using ::arolla::expr::DefaultOptimizer;
using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::MakeOpNode;
using ::arolla::expr::ModelEvaluationOptions;
using ::arolla::expr::ModelExecutor;
using ::arolla::expr::ModelExecutorOptions;
using ::arolla::expr::Placeholder;
using ::arolla::expr::PostOrder;

using Executor = ModelExecutor<absl::Span<const TypedRef>, TypedValue>;
using ExecutorPtr = std::shared_ptr<const Executor>;

// (internal) Compiles an expression for the given input types.
absl::StatusOr<ExecutorPtr> Compile(const ExprNodePtr& expr,
                                    absl::Span<const std::string> input_names,
                                    absl::Span<const QTypePtr> input_qtypes) {
  DCheckPyGIL();
  ReleasePyGIL guard;
  DCHECK_EQ(input_names.size(), input_qtypes.size());
  std::vector<std::pair<std::string, QTypePtr>> args(input_names.size());
  for (size_t i = 0; i < input_names.size(); ++i) {
    args[i] = {input_names[i], input_qtypes[i]};
  }
  ASSIGN_OR_RETURN(auto optimizer, DefaultOptimizer());
  ASSIGN_OR_RETURN(auto executor,
                   CompileModelExecutor<TypedValue>(
                       expr, CreateTypedRefsInputLoader(args),
                       ModelExecutorOptions{
                           .eval_options = {.optimizer = std::move(optimizer)},
                       }));
  return std::make_shared<const Executor>(std::move(executor));
}

// (internal) Executes a compiled expression with the given inputs.
absl::StatusOr<TypedValue> Execute(const Executor& executor,
                                   absl::Span<const TypedRef> input_qvalues) {
  DCheckPyGIL();
  ReleasePyGIL guard;
  if (executor.CanExecuteOnStack(4096)) {
    return executor.ExecuteOnStack<4096>(ModelEvaluationOptions{},
                                         input_qvalues);
  } else {
    return executor.ExecuteOnHeap(ModelEvaluationOptions{}, input_qvalues);
  }
}

// (internal) Compiler Cache.
//
// The cache is shared between:
//   * arolla.abc.eval_expr
//   * arolla.abc.invoke_op
//   * arolla.abc.aux_eval_op
//
// The implementation depends on the Python GIL for thread safety.
class CCache {
 public:
  static absl::Nullable<ExecutorPtr> LookupOrNull(
      const Fingerprint& fingerprint,
      absl::Span<const TypedRef> input_qvalues) {
    DCheckPyGIL();
    if (auto* result =
            impl().LookupOrNull(LookupKey{fingerprint, input_qvalues})) {
      return *result;
    }
    return nullptr;
  }

  [[nodiscard]] static absl::Nonnull<ExecutorPtr> Put(
      const Fingerprint& fingerprint, std::vector<QTypePtr>&& input_qtypes,
      absl::Nonnull<ExecutorPtr>&& executor) {
    DCheckPyGIL();
    return *impl().Put(Key{fingerprint, std::move(input_qtypes)},
                       std::move(executor));
  }

  static void Clear() {
    DCheckPyGIL();
    impl().Clear();
  }

  CCache() = delete;

 private:
  struct Key {
    Fingerprint fingerprint;
    std::vector<QTypePtr> input_qtypes;

    template <typename H>
    friend H AbslHashValue(H h, const Key& key) {
      h = H::combine(std::move(h), key.fingerprint, key.input_qtypes.size());
      for (const auto& input_qtype : key.input_qtypes) {
        h = H::combine(std::move(h), input_qtype);
      }
      return h;
    }
  };

  struct LookupKey {
    Fingerprint fingerprint;
    absl::Span<const TypedRef> input_qvalues;

    template <typename H>
    friend H AbslHashValue(H h, const LookupKey& lookup_key) {
      h = H::combine(std::move(h), lookup_key.fingerprint,
                     lookup_key.input_qvalues.size());
      for (const auto& input_qvalue : lookup_key.input_qvalues) {
        h = H::combine(  // combine the type, ignore the value
            std::move(h), input_qvalue.GetType());
      }
      return h;
    }
  };

  struct KeyHash {
    size_t operator()(const Key& key) const { return absl::HashOf(key); }
    size_t operator()(const LookupKey& key) const { return absl::HashOf(key); }
  };

  struct KeyEq {
    bool operator()(const Key& lhs, const Key& rhs) const {
      return lhs.fingerprint == rhs.fingerprint &&
             lhs.input_qtypes == rhs.input_qtypes;
    }
    bool operator()(const Key& lhs, const LookupKey& rhs) const {
      return lhs.fingerprint == rhs.fingerprint &&
             std::equal(lhs.input_qtypes.begin(), lhs.input_qtypes.end(),
                        rhs.input_qvalues.begin(), rhs.input_qvalues.end(),
                        [](const QTypePtr& ltype, const TypedRef& rvalue) {
                          return ltype == rvalue.GetType();
                        });
    }
  };

  using Impl = LruCache<Key, ExecutorPtr, KeyHash, KeyEq>;
  static Impl& impl() {
    static Indestructible<Impl> result(kCCacheSize);
    return *result;
  }
};

// (internal) The inner part of InvokeOp that interacts with Arolla C++ API.
absl::StatusOr<TypedValue> InvokeOpImpl(
    ExprOperatorPtr op, absl::Span<const TypedRef> input_qvalues) {
  DCheckPyGIL();
  const auto& fgpt = op->fingerprint();
  auto executor = CCache::LookupOrNull(fgpt, input_qvalues);
  if (ABSL_PREDICT_FALSE(executor == nullptr)) {
    std::vector<QTypePtr> input_qtypes(input_qvalues.size());
    std::vector<std::string> input_names(input_qvalues.size());
    std::vector<ExprNodePtr> input_leaves(input_qvalues.size());
    for (size_t i = 0; i < input_qvalues.size(); ++i) {
      input_qtypes[i] = input_qvalues[i].GetType();
      input_names[i] = absl::StrCat(i);
      input_leaves[i] = Leaf(input_names[i]);
    }
    ASSIGN_OR_RETURN(auto expr,
                     MakeOpNode(std::move(op), std::move(input_leaves)));
    ASSIGN_OR_RETURN(executor, Compile(expr, input_names, input_qtypes));
    executor = CCache::Put(fgpt, std::move(input_qtypes), std::move(executor));
  }
  return Execute(*executor, input_qvalues);
}

// def invoke_op(
//     op: str|QValue, input_qvalues: tuple[QValue, ...] = (), /
// ) -> QValue
PyObject* PyInvokeOp(PyObject* /*self*/, PyObject** py_args, Py_ssize_t nargs) {
  DCheckPyGIL();
  if (nargs < 1) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.invoke_op() missing 1 required positional "
                    "argument: 'op'");
    return nullptr;
  } else if (nargs > 2) {
    return PyErr_Format(PyExc_TypeError,
                        "arolla.abc.invoke_op() takes 2 positional arguments "
                        "but %zu were given",
                        nargs);
  }

  // Parse `op`.
  auto op = ParseArgPyOperator("arolla.abc.invoke_op", py_args[0]);
  if (op == nullptr) {
    return nullptr;
  }

  // Parse `input_qvalues`.
  std::vector<TypedRef> input_qvalues;
  if (nargs >= 2) {
    PyObject* py_tuple_input_qvalues = py_args[1];
    if (!PyTuple_Check(py_tuple_input_qvalues)) {
      return PyErr_Format(
          PyExc_TypeError,
          "arolla.abc.invoke_op() expected a tuple[QValue, ...],"
          " got input_qvalues: %s",
          Py_TYPE(py_tuple_input_qvalues)->tp_name);
    }
    input_qvalues.resize(
        PyTuple_GET_SIZE(py_tuple_input_qvalues),
        TypedRef::UnsafeFromRawPointer(GetNothingQType(), nullptr));
    for (size_t i = 0; i < input_qvalues.size(); ++i) {
      PyObject* py_qvalue = PyTuple_GET_ITEM(py_tuple_input_qvalues, i);
      auto* typed_value = UnwrapPyQValue(py_qvalue);
      if (typed_value == nullptr) {
        return PyErr_Format(PyExc_TypeError,
                            "arolla.abc.invoke_op() expected qvalues, got "
                            "input_qvalues[%zu]: %s",
                            i, Py_TYPE(py_qvalue)->tp_name);
      }
      input_qvalues[i] = typed_value->AsRef();
    }
  }

  // Call the implementation.
  ASSIGN_OR_RETURN(auto result, InvokeOpImpl(std::move(op), input_qvalues),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyQValue(std::move(result));
}

// (internal) An utility entry storing the leaf and placeholder keys of
// an expression.
//
// The leaf keys are needed to identify the required inputs without scanning
// the whole expression. The placeholder keys are necessary for the error
// message.
struct ExprInfo {
  std::vector<std::string> leaf_keys;
  std::vector<std::string> placeholder_keys;
  absl::flat_hash_map<absl::string_view, size_t> leaf_key_index;
};

using ExprInfoPtr = std::shared_ptr<const ExprInfo>;

// (internal) A factory for ExprInfo entries with a simple caching strategy.
// The implementation depends on the Python GIL for thread safety.
class ExprInfoCache {
 public:
  // Returns expr_info for the given expression.
  static ExprInfoPtr Get(const ExprNodePtr& expr) {
    DCheckPyGIL();
    if (auto* result = impl().LookupOrNull(expr->fingerprint());
        ABSL_PREDICT_TRUE(result != nullptr)) {
      return *result;
    }
    ExprInfoPtr result;
    {
      ReleasePyGIL guard;
      std::vector<std::string> leaf_keys;
      std::vector<std::string> placeholder_keys;
      PostOrder post_order(expr);
      for (const auto& node : post_order.nodes()) {
        if (node->is_leaf()) {
          leaf_keys.emplace_back(node->leaf_key());
        } else if (node->is_placeholder()) {
          placeholder_keys.emplace_back(node->placeholder_key());
        }
      }
      std::sort(leaf_keys.begin(), leaf_keys.end());
      std::sort(placeholder_keys.begin(), placeholder_keys.end());
      absl::flat_hash_map<absl::string_view, size_t> leaf_key_index;
      leaf_key_index.reserve(leaf_keys.size());
      for (size_t i = 0; i < leaf_keys.size(); ++i) {
        leaf_key_index.emplace(leaf_keys[i], i);
      }
      DCHECK_EQ(leaf_keys.size(), leaf_key_index.size());
      result = std::make_shared<ExprInfo>(ExprInfo{std::move(leaf_keys),
                                                   std::move(placeholder_keys),
                                                   std::move(leaf_key_index)});
    }
    return *impl().Put(expr->fingerprint(), std::move(result));
  }

  static void Clear() {
    DCheckPyGIL();
    impl().Clear();
  }

  ExprInfoCache() = delete;

 private:
  using Impl = LruCache<Fingerprint, ExprInfoPtr>;
  static Impl& impl() {
    static Indestructible<Impl> result(kExprInfoCacheSize);
    return *result;
  }
};

// (internal) The inner part of EvalExpr that interacts with Arolla C++ API.
absl::StatusOr<TypedValue> EvalExprImpl(
    const ExprNodePtr& expr, absl::Span<const std::string> input_names,
    absl::Span<const TypedRef> input_qvalues) {
  DCHECK(std::is_sorted(input_names.begin(), input_names.end()));
  DCHECK_EQ(std::adjacent_find(input_names.begin(), input_names.end()),
            input_names.end());
  DCheckPyGIL();
  const auto& fgpt = expr->fingerprint();
  auto executor = CCache::LookupOrNull(fgpt, input_qvalues);
  if (ABSL_PREDICT_FALSE(executor == nullptr)) {
    std::vector<QTypePtr> input_qtypes(input_qvalues.size());
    for (size_t i = 0; i < input_qvalues.size(); ++i) {
      input_qtypes[i] = input_qvalues[i].GetType();
    }
    ASSIGN_OR_RETURN(executor, Compile(expr, input_names, input_qtypes));
    executor = CCache::Put(fgpt, std::move(input_qtypes), std::move(executor));
  }
  return Execute(*executor, input_qvalues);
}

// def arolla.abc.eval_expr(
//     expr: Expr, input_qvalues: dict[str, QValue] = {}, /
// ) -> QValue
PyObject* PyEvalExpr(PyObject* /*self*/, PyObject** py_args, Py_ssize_t nargs) {
  DCheckPyGIL();
  if (nargs < 1) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.eval_expr() missing 1 required positional "
                    "argument: 'expr'");
    return nullptr;
  } else if (nargs > 2) {
    return PyErr_Format(PyExc_TypeError,
                        "arolla.abc.eval_expr() takes 2 positional arguments "
                        "but %zu were given",
                        nargs);
  }

  // Parse `expr`.
  const auto py_expr = py_args[0];
  const auto expr = UnwrapPyExpr(py_expr);
  if (expr == nullptr) {
    PyErr_Clear();
    return PyErr_Format(
        PyExc_TypeError,
        "arolla.abc.eval_expr() expected an expression, got expr: %s",
        Py_TYPE(py_expr)->tp_name);
  }
  const auto expr_info = ExprInfoCache::Get(expr);

  // Parse `input_qvalues`.
  std::vector<TypedRef> input_qvalues(
      expr_info->leaf_key_index.size(),
      TypedRef::UnsafeFromRawPointer(GetNothingQType(), nullptr));
  size_t input_count = 0;
  if (nargs >= 2) {
    PyObject* py_dict_input_qvalues = py_args[1];
    if (!PyDict_Check(py_dict_input_qvalues)) {
      return PyErr_Format(
          PyExc_TypeError,
          "arolla.abc.eval_expr() expected a dict[str, QValue], "
          "got input_qvalues: %s",
          Py_TYPE(py_dict_input_qvalues)->tp_name);
    }
    PyObject *py_str, *py_qvalue;
    Py_ssize_t pos = 0;
    while (PyDict_Next(py_dict_input_qvalues, &pos, &py_str, &py_qvalue)) {
      Py_ssize_t input_name_size = 0;
      if (!PyUnicode_Check(py_str)) {
        return PyErr_Format(PyExc_TypeError,
                            "arolla.abc.eval_expr() expected all "
                            "input_qvalues.keys() to be "
                            "strings, got %s",
                            Py_TYPE(py_str)->tp_name);
      }
      const char* input_name_data =
          PyUnicode_AsUTF8AndSize(py_str, &input_name_size);
      if (input_name_data == nullptr) {
        return nullptr;
      }
      const absl::string_view input_name(input_name_data, input_name_size);
      auto it = expr_info->leaf_key_index.find(input_name);
      if (it == expr_info->leaf_key_index.end()) {
        continue;
      }
      const auto* typed_value = UnwrapPyQValue(py_qvalue);
      if (typed_value == nullptr) {
        PyErr_Clear();
        return PyErr_Format(PyExc_TypeError,
                            "arolla.abc.eval_expr() expected all "
                            "input_qvalues.values() to be "
                            "QValues, got %s",
                            Py_TYPE(py_qvalue)->tp_name);
      }
      input_qvalues[it->second] = typed_value->AsRef();
      input_count += 1;
    }
  }

  // Check that all inputs present.
  if (input_count != expr_info->leaf_keys.size()) {
    std::ostringstream message;
    message << "arolla.abc.eval_expr() missing values for: ";
    bool is_first = true;
    size_t i = 0;
    for (const auto& leaf_key : expr_info->leaf_keys) {
      if (input_qvalues[i++].GetRawPointer() == nullptr) {
        message << NonFirstComma(is_first) << ToDebugString(Leaf(leaf_key));
      }
    }
    PyErr_SetString(PyExc_ValueError, std::move(message).str().c_str());
    return nullptr;
  }

  // Check that there are no placeholders.
  if (!expr_info->placeholder_keys.empty()) {
    std::ostringstream message;
    message << "arolla.abc.eval_expr() expression contains placeholders: ";
    bool is_first = true;
    for (const auto& placeholder_key : expr_info->placeholder_keys) {
      message << NonFirstComma(is_first)
              << ToDebugString(Placeholder(placeholder_key));
    }
    PyErr_SetString(PyExc_ValueError, std::move(message).str().c_str());
    return nullptr;
  }

  // Call the implementation.
  ASSIGN_OR_RETURN(auto result,
                   EvalExprImpl(expr, expr_info->leaf_keys, input_qvalues),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyQValue(std::move(result));
}

// def aux_eval_op(
//     op: str|QValue, /, *args: Any, **kwargs: Any
// ) -> QValue
PyObject* PyAuxEvalOp(PyObject* /*self*/, PyObject** py_args, Py_ssize_t nargs,
                      PyObject* py_tuple_kwnames) {
  DCheckPyGIL();
  if (nargs == 0) {
    PyErr_SetString(PyExc_TypeError,
                    "arolla.abc.aux_eval_op() missing 1 required positional "
                    "argument: 'op'");
    return nullptr;
  }

  // Parse `op`.
  auto op = ParseArgPyOperator("arolla.abc.aux_eval_op", py_args[0]);
  if (op == nullptr) {
    return nullptr;
  }

  // Bind the arguments.
  ASSIGN_OR_RETURN(auto signature, op->GetSignature(),
                   (SetPyErrFromStatus(_), nullptr));
  std::vector<QValueOrExpr> bound_args;
  if (!AuxBindArguments(signature, py_args + 1,
                        (nargs - 1) | PY_VECTORCALL_ARGUMENTS_OFFSET,
                        py_tuple_kwnames, &bound_args)) {
    return nullptr;
  }

  // Generate `input_qvalues`.
  const auto param_name = [&signature](size_t i) -> std::string {
    if (!HasVariadicParameter(signature)) {
      DCHECK_LT(i, signature.parameters.size());
      return signature.parameters[i].name;
    }
    if (i + 1 < signature.parameters.size()) {
      return signature.parameters[i].name;
    }
    return absl::StrCat(signature.parameters.back().name, "[",
                        i - signature.parameters.size() + 1, "]");
  };
  std::vector<TypedRef> input_qvalues(
      bound_args.size(),
      TypedRef::UnsafeFromRawPointer(GetNothingQType(), nullptr));
  for (size_t i = 0; i < bound_args.size(); ++i) {
    const auto* qvalue = std::get_if<TypedValue>(&bound_args[i]);
    if (qvalue == nullptr) {
      return PyErr_Format(
          PyExc_TypeError,
          "arolla.abc.aux_eval_op() expected all arguments "
          "to be qvalues, got an expression for the parameter '%s'",
          param_name(i).c_str());
    }
    input_qvalues[i] = qvalue->AsRef();
  }

  // Call the implementation.
  ASSIGN_OR_RETURN(auto result, InvokeOpImpl(std::move(op), input_qvalues),
                   (SetPyErrFromStatus(_), nullptr));
  return WrapAsPyQValue(std::move(result));
}

// def clear_eval_compile_cache() -> None
PyObject* PyClearEvalCompileCache(PyObject* /*self*/, PyObject* /*py_args*/) {
  CCache::Clear();
  ExprInfoCache::Clear();
  Py_RETURN_NONE;
}

}  // namespace

const PyMethodDef kDefPyInvokeOp = {
    "invoke_op",
    reinterpret_cast<PyCFunction>(&PyInvokeOp),
    METH_FASTCALL,
    ("invoke_op(op, input_qvalues=(), /)\n"
     "--\n\n"
     "Invokes the operator with the given inputs and returns the result.\n"
     "\n"
     "This function is not intended for use by end users. Like \n"
     "make_operator_node(), it passes the provided inputs to the operator\n"
     "implementation without checking them against the operator's signature.\n"
     "In particular, it does not handle the default values of the parameters.\n"
     "\n"
     "This function doesn't work with operators, that expect literal inputs:\n"
     "\n"
     "  invoke_op('core.get_nth', tuple, idx)  # a compilation failure\n"
     "\n"
     "To invoke such operators, you need to construct an expression and\n"
     "evaluate it:\n\n"
     "  eval_expr(M.core.get_nth(L.tuple, literal(idx)), tuple=tuple)\n"
     "\n"
     "Args:\n"
     "  op: An operator object, or a name of an operator in the registry.\n"
     "  inputs: Operator inputs that will be passed as-is. Must match \n"
     "    the operator signature.\n"
     "\n"
     "Returns:\n"
     "  A result of invocation.\n"),
};

const PyMethodDef kDefPyEvalExpr = {
    "eval_expr",
    reinterpret_cast<PyCFunction>(&PyEvalExpr),
    METH_FASTCALL,
    ("eval_expr(expr, input_qvalues={}, /)\n"
     "--\n\n"
     "Compiles and executes an expression for the given inputs."),
};

const PyMethodDef kDefPyAuxEvalOp = {
    "aux_eval_op",
    reinterpret_cast<PyCFunction>(&PyAuxEvalOp),
    METH_FASTCALL | METH_KEYWORDS,
    ("aux_eval_op(op, /, *args, **kwargs)\n"
     "--\n\n"
     "Returns the result of an operator evaluation with given arguments.\n\n"
     "This function is not intended for regular use; however, it can be\n"
     "useful in performance-sensitive applications as it allows you to avoid\n"
     "constructing an expression.\n\n"
     "  rl.abc.aux_eval_op('math.add', 2, 3)      # returns rl.int32(5)\n"
     "  rl.abc.aux_eval_op('math.add', x=3, y=5)  # returns rl.int32(8)\n\n"
     "The main difference of this function from `rl.abc.invoke_op()` is that\n"
     "it depends on `signature.aux_policy` and operates with arguments\n"
     "rather than inputs. Specifically, it adheres to the operator-specific\n"
     "boxing rules.\n\n"
     "Args\n"
     "  op: An operator, or the name of an operator in the registry.\n"
     "  *args: Positional arguments for the operator.\n"
     "  *kwargs: Keyword arguments for the operator.\n\n"
     "Returns:\n"
     "  The evalution result."),
};

const PyMethodDef kDefPyClearEvalCompileCache = {
    "clear_eval_compile_cache",
    reinterpret_cast<PyCFunction>(&PyClearEvalCompileCache),
    METH_NOARGS,
    ("clear_eval_compile_cache()\n"
     "--\n\n"
     "Clears py-eval compile cache."),
};

}  // namespace arolla::python

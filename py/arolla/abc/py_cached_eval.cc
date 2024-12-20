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
#include "py/arolla/abc/py_cached_eval.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl//base/no_destructor.h"
#include "absl//base/nullability.h"
#include "absl//base/optimization.h"
#include "absl//functional/any_invocable.h"
#include "absl//hash/hash.h"
#include "absl//log/check.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_cat.h"
#include "absl//types/span.h"
#include "py/arolla/abc/eval_options.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/expr/expr.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/io/typed_refs_input_loader.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serving/expr_compiler.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/lru_cache.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

static constexpr size_t kCCacheSize = 1024;

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::Leaf;
using ::arolla::expr::MakeOpNode;

using Model = std::function<absl::StatusOr<TypedValue>(
    const ModelFunctionOptions&, absl::Span<const TypedRef>)>;
using ModelPtr = std::shared_ptr<Model>;

// Compiles an expression for the given input types.
absl::StatusOr<ModelPtr> Compile(const ExprNodePtr& expr,
                                 absl::Span<const std::string> input_names,
                                 absl::Span<const QTypePtr> input_qtypes,
                                 const ExprCompilationOptions& options) {
  DCheckPyGIL();
  ReleasePyGIL guard;
  DCHECK_EQ(input_names.size(), input_qtypes.size());
  std::vector<std::pair<std::string, QTypePtr>> args(input_names.size());
  for (size_t i = 0; i < input_names.size(); ++i) {
    args[i] = {input_names[i], input_qtypes[i]};
  }
  ASSIGN_OR_RETURN(auto model,
                   (ExprCompiler<absl::Span<const TypedRef>, TypedValue>())
                       .SetInputLoader(CreateTypedRefsInputLoader(args))
                       .SetAlwaysCloneThreadSafetyPolicy()
                       .VerboseRuntimeErrors(options.verbose_runtime_errors)
                       .Compile<ExprCompilerFlags::kEvalWithOptions>(expr));
  return std::make_shared<Model>(std::move(model));
}

// Executes a compiled expression with the given inputs.
absl::StatusOr<TypedValue> Execute(const Model& model,
                                   absl::Span<const TypedRef> input_qvalues) {
  DCheckPyGIL();
  absl::AnyInvocable<absl::Status()> check_interrupt_fn = []() {
    AcquirePyGIL guard;
    return PyErr_CheckSignals() < 0
               ? StatusCausedByPyErr(absl::StatusCode::kCancelled,
                                     "interrupted")
               : absl::OkStatus();
  };
  ModelFunctionOptions options{.check_interrupt_fn = PyErr_CanCallCheckSignal()
                                                         ? &check_interrupt_fn
                                                         : nullptr};
  ReleasePyGIL guard;
  return model(options, input_qvalues);
}

// (internal) Compiler Cache.
//
// The cache is shared between between functions such as arolla.abc.eval_expr
// and arolla.abc.invoke_op.
//
// The implementation depends on the Python GIL for thread safety.
class CCache {
 public:
  static absl::Nullable<ModelPtr> LookupOrNull(
      const Fingerprint& fingerprint, absl::Span<const TypedRef> input_qvalues,
      const ExprCompilationOptions& options) {
    DCheckPyGIL();
    if (auto* result = impl().LookupOrNull(
            LookupKey{fingerprint, input_qvalues, options})) {
      return *result;
    }
    return nullptr;
  }

  [[nodiscard]] static absl::Nonnull<ModelPtr> Put(
      const Fingerprint& fingerprint, std::vector<QTypePtr>&& input_qtypes,
      ExprCompilationOptions options, absl::Nonnull<ModelPtr>&& model) {
    DCheckPyGIL();
    return *impl().Put(
        Key{fingerprint, std::move(input_qtypes), std::move(options)},
        std::move(model));
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
    ExprCompilationOptions options;

    template <typename H>
    friend H AbslHashValue(H h, const Key& key) {
      h = H::combine(std::move(h), key.fingerprint, key.options,
                     key.input_qtypes.size());
      for (const auto& input_qtype : key.input_qtypes) {
        h = H::combine(std::move(h), input_qtype);
      }
      return h;
    }
  };

  struct LookupKey {
    Fingerprint fingerprint;
    absl::Span<const TypedRef> input_qvalues;
    ExprCompilationOptions options;

    template <typename H>
    friend H AbslHashValue(H h, const LookupKey& lookup_key) {
      h = H::combine(std::move(h), lookup_key.fingerprint, lookup_key.options,
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
             lhs.input_qtypes == rhs.input_qtypes && lhs.options == rhs.options;
    }
    bool operator()(const Key& lhs, const LookupKey& rhs) const {
      return lhs.fingerprint == rhs.fingerprint &&
             std::equal(lhs.input_qtypes.begin(), lhs.input_qtypes.end(),
                        rhs.input_qvalues.begin(), rhs.input_qvalues.end(),
                        [](const QTypePtr& ltype, const TypedRef& rvalue) {
                          return ltype == rvalue.GetType();
                        }) &&
             lhs.options == rhs.options;
    }
  };

  using Impl = LruCache<Key, ModelPtr, KeyHash, KeyEq>;
  static Impl& impl() {
    static absl::NoDestructor<Impl> result(kCCacheSize);
    return *result;
  }
};

}  // namespace

absl::StatusOr<TypedValue> InvokeOpWithCompilationCache(
    ExprOperatorPtr op, absl::Span<const TypedRef> input_qvalues,
    const ExprCompilationOptions& options) {
  DCheckPyGIL();
  const auto& fgpt = op->fingerprint();
  auto model = CCache::LookupOrNull(fgpt, input_qvalues, options);
  if (ABSL_PREDICT_FALSE(model == nullptr)) {
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
    ASSIGN_OR_RETURN(model, Compile(expr, input_names, input_qtypes, options));
    model =
        CCache::Put(fgpt, std::move(input_qtypes), options, std::move(model));
  }
  return Execute(*model, input_qvalues);
}

absl::StatusOr<TypedValue> EvalExprWithCompilationCache(
    const ExprNodePtr& expr, absl::Span<const std::string> input_names,
    absl::Span<const TypedRef> input_qvalues,
    const ExprCompilationOptions& options) {
  DCHECK(std::is_sorted(input_names.begin(), input_names.end()));
  DCHECK_EQ(std::adjacent_find(input_names.begin(), input_names.end()),
            input_names.end());
  DCheckPyGIL();
  const auto& fgpt = expr->fingerprint();
  auto model = CCache::LookupOrNull(fgpt, input_qvalues, options);
  if (ABSL_PREDICT_FALSE(model == nullptr)) {
    std::vector<QTypePtr> input_qtypes(input_qvalues.size());
    for (size_t i = 0; i < input_qvalues.size(); ++i) {
      input_qtypes[i] = input_qvalues[i].GetType();
    }
    ASSIGN_OR_RETURN(model, Compile(expr, input_names, input_qtypes, options));
    model =
        CCache::Put(fgpt, std::move(input_qtypes), options, std::move(model));
  }
  return Execute(*model, input_qvalues);
}

void ClearCompilationCache() { CCache::Clear(); }

}  // namespace arolla::python

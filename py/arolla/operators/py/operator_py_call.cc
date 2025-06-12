// Copyright 2025 Google LLC
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
// Operator: py.call_py(fn, output_qtype, *args, **kwargs)
//

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/basic_expr_operator.h"
#include "arolla/expr/expr_attributes.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/expr/qtype_utils.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/bound_operators.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/named_field_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/init_arolla.h"
#include "py/arolla/abc/py_object_qtype.h"
#include "py/arolla/abc/py_qvalue.h"
#include "py/arolla/abc/py_qvalue_specialization.h"
#include "py/arolla/py_utils/py_utils.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::BackendExprOperatorTag;
using ::arolla::expr::ExprAttributes;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::expr::ExprOperatorWithFixedSignature;
using ::arolla::expr::HasAllAttrQTypes;
using ::arolla::expr::RegisterOperator;

constexpr absl::string_view kPyCallOpName = "py.call";

constexpr absl::string_view kPyCallOpDoc =
    R"(Calls the python callable `fn` with the specified `args` and `kwargs`.

Example:
  ```python
  result = arolla.eval(
      M.py.call(fn, return_qtype, arolla.tuple(...), arolla.namedtuple(...)))
  ```
  Equivalent to:
  ```python
  result = fn(*arolla.tuple(...), **arolla.namedtuple(...).as_dict())
  if not isinstance(result, arolla.QValue):
    raise TypeError
  if result.qtype != return_qtype:
    raise ValueError
  ```

Args:
  fn (callable): A python callable.
  return_qtype: The expected return type (must be a compile-time value).
  args: A tuple containing the positional arguments to pass to `fn`.
  kwargs: A namedtuple containing the keyword arguments to pass to `fn`.

Returns:
  The result of the `fn` call.)";

class PyCallBackendOp final : public QExprOperator {
 public:
  using QExprOperator::QExprOperator;

  absl::StatusOr<std::unique_ptr<BoundOperator>> DoBind(
      absl::Span<const TypedSlot> input_slots,
      TypedSlot output_slot) const final {
    return MakeBoundOperator([input_slots = std::vector(input_slots.begin(),
                                                        input_slots.end()),
                              output_slot](EvaluationContext* ctx,
                                           FramePtr frame) -> void {
      // Access the values stored in the frame.
      const auto fn_qvalue = TypedRef::FromSlot(input_slots[0], frame);
      const auto return_type_qvalue = TypedRef::FromSlot(input_slots[1], frame);
      const auto args_qvalue = TypedRef::FromSlot(input_slots[2], frame);
      const auto kwargs_qvalue = TypedRef::FromSlot(input_slots[3], frame);

      // Unpack the values.
      AcquirePyGIL guard;
      ASSIGN_OR_RETURN(PyObjectPtr py_fn, GetPyObjectValue(fn_qvalue),
                       ctx->set_status(std::move(_)));
      const QTypePtr return_type = return_type_qvalue.UnsafeAs<QTypePtr>();
      const int64_t args_count = args_qvalue.GetFieldCount();
      const auto kwnames = GetFieldNames(kwargs_qvalue.GetType());
      const int64_t kwargs_count = std::ssize(kwnames);

      // Prepare args and kwnames for a vectorcall.
      auto py_tuple_args =
          PyObjectPtr::Own(PyTuple_New(1 + args_count + kwargs_count));
      if (py_tuple_args.get() == nullptr) {
        return ctx->set_status(StatusWithRawPyErr(absl::StatusCode::kInternal,
                                                  "PyTuple_New: failed"));
      }
      auto py_tuple_kwnames = PyObjectPtr::Own(PyTuple_New(kwargs_count));
      if (py_tuple_kwnames.get() == nullptr) {
        return ctx->set_status(StatusWithRawPyErr(absl::StatusCode::kInternal,
                                                  "PyTuple_New: failed"));
      }
      for (int64_t i = 0; i < args_count; ++i) {
        PyTuple_SET_ITEM(py_tuple_args.get(), 1 + i,
                         WrapAsPyQValue(TypedValue(args_qvalue.GetField(i))));
        if (PyTuple_GET_ITEM(py_tuple_args.get(), 1 + i) == nullptr) {
          return ctx->set_status(StatusWithRawPyErr(absl::StatusCode::kInternal,
                                                    "WrapAsPyQValue: failed"));
        }
      }
      for (int64_t i = 0; i < kwargs_count; ++i) {
        PyTuple_SET_ITEM(py_tuple_args.get(), 1 + args_count + i,
                         WrapAsPyQValue(TypedValue(kwargs_qvalue.GetField(i))));
        if (PyTuple_GET_ITEM(py_tuple_args.get(), 1 + args_count + i) ==
            nullptr) {
          return ctx->set_status(StatusWithRawPyErr(absl::StatusCode::kInternal,
                                                    "WrapAsPyQValue: failed"));
        }
        PyTuple_SET_ITEM(
            py_tuple_kwnames.get(), i,
            PyUnicode_FromStringAndSize(kwnames[i].data(), kwnames[i].size()));
        if (PyTuple_GET_ITEM(py_tuple_kwnames.get(), i) == nullptr) {
          return ctx->set_status(
              StatusWithRawPyErr(absl::StatusCode::kInternal,
                                 "PyUnicode_FromStringAndSize: failed"));
        }
      }

      // Call `fn` and process the result.
      auto py_result = PyObjectPtr::Own(PyObject_Vectorcall(
          py_fn.get(), PySequence_Fast_ITEMS(py_tuple_args.get()) + 1,
          args_count | PY_VECTORCALL_ARGUMENTS_OFFSET, py_tuple_kwnames.get()));
      if (py_result.get() == nullptr) {
        return ctx->set_status(StatusWithRawPyErr(
            absl::StatusCode::kInvalidArgument, "error during calling `fn`"));
      }
      if (!IsPyQValueInstance(py_result.get())) {
        PyErr_Format(PyExc_TypeError,
                     "expected the result to be a qvalue, got %s",
                     Py_TYPE(py_result.get())->tp_name);
        return ctx->set_status(StatusWithRawPyErr(
            absl::StatusCode::kInvalidArgument, "unexpected result type"));
      }
      const auto& result = UnsafeUnwrapPyQValue(py_result.get());
      if (result.GetType() != return_type) {
        PyErr_SetString(
            PyExc_ValueError,
            absl::StrFormat("expected the result to have qtype %s, got %s",
                            return_type->name(), result.GetType()->name())
                .c_str());
        return ctx->set_status(StatusWithRawPyErr(
            absl::StatusCode::kInvalidArgument, "unexpected result qtype"));
      }
      RETURN_IF_ERROR(result.CopyToSlot(output_slot, frame))
          .With(ctx->set_status());
    });
  }
};

class PyCallBackendOpFamily final : public OperatorFamily {
  absl::StatusOr<OperatorPtr> DoGetOperator(
      absl::Span<const QTypePtr> input_types,
      QTypePtr output_type) const final {
    if (input_types.size() != 4 || input_types[0] != GetPyObjectQType() ||
        input_types[1] != GetQTypeQType() || !IsTupleQType(input_types[2]) ||
        !IsNamedTupleQType(input_types[3])) {
      return absl::InvalidArgumentError(
          "expected inputs: (PY_OBJECT, QTYPE, TUPLE[...], NAMEDTUPLE[...])");
    }
    return std::make_shared<PyCallBackendOp>(input_types, output_type);
  }
};

class PyCallOp final : public BackendExprOperatorTag,
                       public ExprOperatorWithFixedSignature {
 public:
  PyCallOp()
      : ExprOperatorWithFixedSignature(
            kPyCallOpName,
            ExprOperatorSignature{
                {.name = "fn"},
                {.name = "return_type"},
                {.name = "args", .default_value = MakeEmptyTuple()},
                {.name = "kwargs", .default_value = MakeEmptyNamedTuple()},
            },
            kPyCallOpDoc,
            FingerprintHasher("::arolla::python::PyCallOp").Finish()) {}

  absl::StatusOr<ExprAttributes> InferAttributes(
      absl::Span<const ExprAttributes> inputs) const final {
    RETURN_IF_ERROR(ValidateOpInputsCount(inputs));
    const auto& fn_attr = inputs[0];
    const auto& return_type_attr = inputs[1];
    const auto& args_attr = inputs[2];
    const auto& kwargs_attr = inputs[3];
    if (fn_attr.qtype() != nullptr && fn_attr.qtype() != GetPyObjectQType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a PY_OBJECT, got fn: %s", fn_attr.qtype()->name()));
    }
    if (return_type_attr.qtype() != nullptr &&
        return_type_attr.qtype() != GetQTypeQType()) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected return_type: QTYPE, got %s",
                          return_type_attr.qtype()->name()));
    }
    if (args_attr.qtype() != nullptr && !IsTupleQType(args_attr.qtype())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected a tuple, got args: %s", args_attr.qtype()->name()));
    }
    if (kwargs_attr.qtype() != nullptr &&
        !IsNamedTupleQType(kwargs_attr.qtype())) {
      return absl::InvalidArgumentError(
          absl::StrFormat("expected a namedtuple, got kwargs: %s",
                          kwargs_attr.qtype()->name()));
    }
    if (return_type_attr.qtype() != nullptr &&
        !return_type_attr.qvalue().has_value()) {
      return absl::InvalidArgumentError("`return_type` must be a literal");
    }
    if (!HasAllAttrQTypes(inputs)) {
      return ExprAttributes{};
    }
    return ExprAttributes(return_type_attr.qvalue()->UnsafeAs<QTypePtr>());
  }
};

AROLLA_INITIALIZER(
        .name = "arolla_operators/py:call",
        .reverse_deps =
            {
                arolla::initializer_dep::kOperators,
            },
        .init_fn = []() -> absl::Status {
          RETURN_IF_ERROR(
              arolla::OperatorRegistry::GetInstance()->RegisterOperatorFamily(
                  kPyCallOpName, std::make_unique<PyCallBackendOpFamily>()));
          return RegisterOperator<PyCallOp>(kPyCallOpName).status();
        })

}  // namespace
}  // namespace arolla::python

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
#ifndef AROLLA_QEXPR_AGGREGATION_OPS_INTERFACE_H_
#define AROLLA_QEXPR_AGGREGATION_OPS_INTERFACE_H_

#include <cstdint>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/util/meta.h"
#include "arolla/util/view_types.h"

namespace arolla {

// A Group Operation is an operation accepting as input groups of values from
// zero or more data columns in a Child ID Space and individual values from
// zero or more data column in a Parent ID Space. A Group Operation's output is
// a single data column in either the Parent ID Space or the Child ID Space.
//
// Group Operations can be Aggregational or Non-Aggregational.
//
// For each defined parent (1 row in the Parent ID Space and a
// set of zero or more rows in the Child ID Space) an Aggregational Group
// Operation produces a single aggregated value in the Parent ID Space, while a
// Non-Aggregational Group Operation produces a value for each row in the
// Child ID Space.
//
// Non-Aggregational Group Operations can be further classified as either
// Partial or Full. Partial Group Operations produce output values
// incrementally, one output per set of input values in the Child ID Space.
// Full Group Operations accept all of the input values in the Child ID Space
// before producing their output in the same order.
//
// A Group Mapping provides a mapping between child row-ids to parent row-ids.
// One general representation of a parent mapping is a sparse array in the
// Child ID Space containing row ids in the Parent ID Space. This can be thought
// of as a foreign key from the Child ID Space to the Parent ID Space.
//
// An important special case is where the mapping from Child IDs to Parent IDs
// is a non-decreasing function. For example:
//
// Child ID   Parent ID
// ---------   --------
//         0          0
//         1          0
//         2          0
//         3          1
//         4          1
//         5          2
//         :          :
//
// In this case, the mapping can be efficiently represented as a vector S of
// (PARENT_COUNT + 1) split points, and the range of Child IDs in parent P are
// in the range S[P] through S[P+1]-1, inclusive. If S[P] == S[P+1], then
// parent P has no corresponding Child ID rows.
//

// Enumeration of supported group operation types, as described above.
enum struct AccumulatorType {
  // Aggregator accumulator generates a single output per group of inputs.
  kAggregator,

  // Partial accumulator generates a single output per child row input,
  // which value is read after each call to Accumulator::Add().
  kPartial,

  // Full accumulator generates a single output per child row input,
  // but values are not read until final call to Accumulator::Add().
  kFull,
};

// New group operations are created by implementing this Accumulator
// class, and using the implementation as a template argument to one of
// the GroupOp classes.
//
// RESULT_T is either the element result type of the accumulator or
// OptionalValue of the result type.
// PARENT_TL is a (possibly empty) type_list of value types from parent columns.
// CHILD_TL is a (possibly empty) type_list of value types from child columns.
//
// Arguments in Accumulator::Reset and Accumulator::Add can be optional. Then
// it will be called even for rows where optional arguments are missed.
// For optional arguments the corresponding types in PARENT_TL and CHILD_TL
// should be OptionalValue<T>.
//
// Note: In this file "TL" is an abbreviation for "Type List" which is
// represented using a meta::type_list of types whereas "Ts" is an abbreviation
// for "Types" which is represented with a parameter pack of individual types.
template <AccumulatorType type, typename RESULT_T, typename PARENT_TL,
          typename CHILD_TL>
struct Accumulator;

template <AccumulatorType TYPE, typename RESULT_T, typename... PARENT_Ts,
          typename... CHILD_Ts>
struct Accumulator<TYPE, RESULT_T, meta::type_list<PARENT_Ts...>,
                   meta::type_list<CHILD_Ts...>> {
  using result_type = RESULT_T;
  using parent_types = meta::type_list<PARENT_Ts...>;
  using child_types = meta::type_list<CHILD_Ts...>;

  // Prepares accumulator for a new set of child rows.
  virtual void Reset(view_type_t<PARENT_Ts>... parent_args) = 0;

  // Adds a child row to the state of this accumulator.
  // It must not crash even if `Reset` wasn't called.
  virtual void Add(view_type_t<CHILD_Ts>... child_args) = 0;

  // Adds N identical child rows to the state of this accumulator.
  // Can be used to speed up processing of constant or sparse data.
  // It must not crash even if `Reset` wasn't called.
  // Default implementation just applies `Add` N times.
  virtual void AddN(int64_t N, view_type_t<CHILD_Ts>... child_args) {
    for (int64_t i = 0; i < N; ++i) {
      Add(child_args...);
    }
  }

  // Used only if TYPE==kFull. Called once for each group after adding
  // all rows, before the first `GetResult`.
  virtual void FinalizeFullGroup() {}

  // Gets one result from this accumulator. Depending on the accumulator type
  // this method may be called once per parent row-id or once per non-missing
  // child row-id. The returned value should be valid a least until the next
  // call of any function of the accumulator.
  // Warning: note that if view_type_t<RESULT_T> is different from RESULT_T
  // (e.g for string types) then returning a local variable of type RESULT_T
  // would be an error. It is recommended to have an explicit strings test
  // for every accumulator that supports it.
  virtual view_type_t<RESULT_T> GetResult() = 0;

  // `Add` can not return an error for performance reasons. If an error can
  // happen, it should be accumulated inside of the accumulator and returned
  // in `GetStatus`. `GetStatus` is called only once after last `GetResult`.
  // `Add`, `AddN`, `GetResult` must not crash even if an error was happened.
  // `Reset` shouldn't clean the status.
  virtual absl::Status GetStatus() { return absl::OkStatus(); }

  static constexpr bool IsAggregator() {
    return TYPE == AccumulatorType::kAggregator;
  }
  static constexpr bool IsPartial() {
    return TYPE == AccumulatorType::kPartial;
  }
  static constexpr bool IsFull() { return TYPE == AccumulatorType::kFull; }

 protected:
  virtual ~Accumulator() = default;
};

// Create an `Accumulator` using its constructor. This helper serves as an
// extension point, allowing for the support of additional parameters
// without modifying existing implementations.
template <typename Accumulator, typename... InitArgs>
Accumulator CreateAccumulator(const EvaluationOptions& eval_options,
                              const InitArgs&... init_args) {
  if constexpr (std::is_constructible_v<Accumulator, const EvaluationOptions&,
                                        InitArgs...>) {
    return Accumulator(eval_options, init_args...);
  } else {
    return Accumulator(init_args...);
  }
}

}  // namespace arolla

#endif  // AROLLA_QEXPR_AGGREGATION_OPS_INTERFACE_H_

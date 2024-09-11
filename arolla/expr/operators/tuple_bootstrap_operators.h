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
#ifndef AROLLA_EXPR_OPERATORS_TUPLE_BOOTSTRAP_OPERATORS_H_
#define AROLLA_EXPR_OPERATORS_TUPLE_BOOTSTRAP_OPERATORS_H_

#include "arolla/expr/expr_operator.h"

namespace arolla::expr_operators {

// core.apply_varargs(op, *args, varargs_tuple) operator applies the given
// (literal) operator to the given arguments, unpacking the tuple passed as the
// last argument.
expr::ExprOperatorPtr MakeApplyVarargsOperator();

// core.get_nth(tuple, n).
expr::ExprOperatorPtr MakeCoreGetNthOp();

// core.zip operator scans several tuples in parallel, producing tuples with a
// field from each one.
expr::ExprOperatorPtr MakeCoreZipOp();

// Left-associative reduces operator.
//
// core.reduce_tuple
expr::ExprOperatorPtr MakeCoreReduceTupleOp();

// core.concat_tuples operator concatenates two given tuples into one.
expr::ExprOperatorPtr MakeCoreConcatTuplesOperator();

// core.map_tuple
expr::ExprOperatorPtr MakeCoreMapTupleOp();

// Operator that creates a named tuple
// `namedtuple._make(field_names, tuple)`,
// where
// field_names: literal with field names
//     can be a single string with each fieldname separated by whitespace and/or
//     commas, e.g., 'x y' or 'x, y'.
// tuple: regular tuple with field values
expr::ExprOperatorPtr MakeNamedtupleMakeOp();

// Operator that extracts field by name from a named tuple.
// `namedtuple.get_field(named_tuple, field_name)`
expr::ExprOperatorPtr MakeNamedtupleGetFieldOp();

// Operator that finds the union of two named tuples.
// `namedtuple.union(first, second)`
expr::ExprOperatorPtr MakeNamedtupleUnionOp();

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_TUPLE_BOOTSTRAP_OPERATORS_H_

# Expression library

A Arolla Expression is an immutable directed acyclic graph representing a
computation in the Arolla data model. It is built as a combination of operators
representing specific operations of the computation.

## ExprNode

`::arolla::expr::ExprNode` represents an individual expression node.

A node can be one of the following kinds:

*   *literal* -- represents a fixed value;
*   *leaf* -- represents a link to a data source; the value for the node will be
    provided by a data source during the evaluation
*   *placeholder* -- a represents an unknown variable that will later be
    replaced with another expression.
*   *operator node* -- represents an operator call -- binds an operator with its
    inputs in the computational dag.

`expr/expr.h` provides various utilities to create and manipulate
expressions nodes.

The snippet below creates an expression that adds a leaf and a placeholder:

```
ASSIGN_OR_RETURN(
    ExprNodePtr node, CallOp("math.add", {Leaf("a"), Placeholder("b")}));
```

*   `::arolla::expr::Leaf` -- creates a leaf node with the given key;
*   `::arolla::expr::Placeholder` -- creates a placeholder with the given key;
*   `::arolla::expr::CallOp` -- creates an operator node with the given operator
    and arguments.

## ExprOperator

Each non-terminal expression node holds a pointer to
`::arolla::expr::ExprOperator`, which defines an operation on the node inputs.

`ExprOperator` has these main methods:

*   `ToLowerLevel`: Returns the result of translating the operator to a lower
    level, that is used either for compilation or to use the node on a lower
    level of abstraction.
*   `GetSignature`: Returns expression operator's calling signature, listing the
    parameters that the operator takes.
*   `InferAttributes`: Infers the output attributes for the given inputs. This
    method is used to propagate attributes through the expression dag.

# ABC

It's a set of modules that provide generic QType, QValue, Expr, and
CompiledExpr. These modules should not depend on concrete qtypes and operators
supplied by Arolla standard library.

The primary purpose of these modules is to mitigate cyclic dependency between
convenience level for scalar/array types, and expr/qexpr libraries.

(?) This directory is temporary. And we need to decide where these modules
should properly live.

# QValue Specialization

We would like to highlight two approaches for adding new functionality to QValue
subclasses.

The first approach is based on the expression framework:

```python
class Array(abc_qtype.QValue):
  def __add__(self, other):
    return arolla.eval(M.math.add(L.x, L.y), x=self, y=other)

  def __mul__(self, other):
    return arolla.eval(M.math.multiply(L.x, L.y), x=self, y=other)

  ...

for qtype in ARRAY_QTYPES:
  abc_qtype.register_qvalue_specialization(qtype, Array)
```

Here we leverage availability of operators `math.add` and `math.multiply` and
use them to implement functionality for Array specialization.

The advantages of this approach:

*   no extra C++/Clif code
*   results of such methods are identical to results that the user gets from
    expressions (the same casting rules and computational backend)
*   compatible with dynamic types (like, Tuple)

If the functionality you need is available through expressions, we recommend
using this approach.

If the function you need cannot be coded using expressions (and there are
reasons to not enabling it), you can select a different approach. You can create
a custom view class (or reuse an existing one) and a function that returns it
for a TypedValue object in C++, and export them to Python using CLIF.

Let's assume you have done it for `Point` type:

```python
class PointClif:
  def x(self): ...
  def y(self): ...

def GetPointClif(value: TypedValue) -> PointClif: ...
```

Then a QValue specialization for Point may look like this:

```python
class Point(abc_qtype.QValue):

  __slots__ = ('_clif_point',)  # optional

  def _arolla_init_(self):
    super()._arolla_init_()
    self._clif_point = GetPointClif(self.clif_typed_value)

  @property
  def x(self):
    return self._clif_point.x()

  @property
  def y(self):
    return self._clif_point.y()


abc_qtype.register_qvalue_specialization(QTYPE_POINT, Point)
```

# QValue Initialization Convention

1.  A subclass of `QValue` must have no method `__init__`.

2.  Instead, the custom initialization logic needs to be placed to a method
    `_arolla_init_(self)`. This method takes exactly one parameter (`self`) and
    returns `None`. It gets automatically invoked from C++ code.

3.  A custom constructor of a `QValue` subclass should to be implemented as a
    method `__new__(cls, arg1, arg2, ...)`; this method needs to return a qvalue
    instance constructed using a factory function.

## Motivation behind the QValue Initialization Convention

We have a few requirements for the class `QValue` in RLv2:

*   We need to construct and return its instances from C++ code (like `return
    WrapAsQValue(typed_value)`).

*   We may need additional initialization logic in `QValue` subclasses (e.g.
    pre-load the number of fields for the `Tuple`).

*   We want to create new instances in Python code, using syntax like
    `CustomQValue(value1, arg2=value2)`.

In Python, there are two special methods responsible for object construction and
initialization:

*   `__new__` allocates an object entity and does some basic initialization,
    particularly if the object is immutable.
*   `__init__` is responsible for the foremost initialization logic.

And these methods have some special properties:

*   `__new__` needs to return an entity, and if this entity is an instance of
    the current class, the method `__init__` gets invoked.
*   `__init__` initializes a given entity; it may not construct and return a new
    entity instead of the given one.
*   `__new__` and `__init__` receive the same arguments.

Our `QValue` requirements led to some inconvenience when got mapped to the
methods`__new__` and `__init__`. More specifically, the method `__init__` had to
support both no-argument calls (alternatively, a call with `typed_value`) for
compatibility with the C++ instantiations and custom parameter calls for the
syntax like `CustomQValue(value1, arg2=value2)`.

In practice, we also found that handling the custom parameters at every
`super(self).__init__(...)` call in the inheritance chain leads to noticeable
overhead.

To overcome the inconveniences, we introduced the custom convention described
above.

For completeness, we also need to mention that the base `class QValue` does not
provide a method `__new__`. Instead, all instances have to be constructed from
`typed_value` in C++ code (or using convenience factory functions).

# Type annotation for QValue and Expr

There are two major limitations for type annotations for `QValue` and `Expr`:

1.  The `ReturnType` for eval-like functions cannot be resolved before runtime

    ```python
    def eval(expr, data_provider) -> ReturnType:
      pass
    ```

    because it depends on concrete `expr` and `data_provider`.

2.  The user can provide a custom qvalue specialisation for any concrete qtype
    in run-time:

    ```python
    def make_array(values: List[Any], qtype) ->  ReturnType:
      ...

    abc_qtype.register_qvalue_specialization(ARRAY_INT, MyArrayInt)

    x = make_array([1, 2, 3], ARRAY_INT)
    ```

In many situations the only constraint for `ReturnType` that can be set is that
it is a *subclass* of `QValue` (or `Expr`).

Please pay attention that if you specify

```python
def eval(...) -> abc_qtype.QValue
```

pytype requires that the caller uses only the base `QValue` class property.

# Disabled special methods `__bool__`, `__{eq,ne}__`, and `__hash__` in `QValue`

The special methods `__bool__`, `__eq__`, `__ne__`, and `__hash__` are disabled
in the base `QValue` implementation. It means you cannot use a `qvalue` in
expressions like the following unless you provide a qvalue specialisation that
implements the corresponding methods:

*   `bool(qvalue)` (implicitly `if qvalue: ...`)
*   `hash(qvalue)` (implicitly `dict[qvalue]`)
*   `qvalue == qvalue` (indirectly `self.assertEqual(qvalue,...)`)
*   `qvalue != qvalue`

Please pay attention, that these methods can be called implicitly or indirectly.

If you use RLv2 and see an error message pointing to here, generally, you need
to add an aggregational operator, like `core.agg_all`, to turn an array value to
a scalar. Also, if you get the error from a unittest assertion, you may need to
switch to predicates from `arolla.testing.*`.

## Motivation

The basic `QValue` defines no default semantic for comparison operations nor for
implicit casting to bool. Neither it provides a default implementation for hash.

Different projects want different semantics for these operations. In RLv2
specifically, the equality predicates are based on the operators: `core.equal`
and `core.not_equal`, and there is no implicit casting to bool, and no value
hashing.

The danger of the default implementations is twofold. The defaults may
unintentionally leak, causing confusion for the user. Say, when `if x: ...`
works in Python, but triggers an error inside of expression (or in C++). In
addition, some code can become dependent on it.

## Please don't confuse *hash* and *fingerprint*.

The property of the hash is "`x` equals to `y`" implies that "`hash(x)` equals
to `hash(y)`"; while "`fingerprint(x)` equals to  `fingerprint(y)`" implies that
"`x` is identical to `y`". In other words, *hash* reflects the *semantic* of
the value, while *fingerprint* reflects the *implementation* of the value.

As an illustration, usually `hash(-0.0)` is the same as `hash(0.0)`, but
`fingerprint(-0.0)` is not equal to `fingerprint(0.0)`.

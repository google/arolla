# QType library

This library defines abstractions for types in Arolla and provides a way to
work with types polymorphically. The library is performance-oriented.

The main classes are the following:

* **QType**: the base class for defining Arolla types.
* **TypedValue**: a polymorphic value. Contains two items: a type T
    (instance of `QType`), and a value (instance of `T`).
* **TypedRef**: a polymorphic reference. Similar to `TypedValue`, contains
    two items: a type T (instance of `QType`), and a reference to
    value (`const T*`). Does NOT own the value it is referring to.
* **TypedSlot**: a polymorphic handle to a field inside of a `FrameLayout`
  (a tuple of a `QType` and `FrameLayout::Slot<T>`).


## QType class

`QType` is the base class for defining Arolla types. Every type used in a
polymorphic way (e.g., in expression evaluation) needs to have a corresponding
singleton `QType` instance.

### Accessing QType for primitives
Each `QType` may have a single corresponding C++ class, and different `QType`s can
use the same class.

For primitive C++ types (like `int64_t`) the corresponding `QType` can be
referenced as `::arolla::GetQType<T>()`.

`QType` for `::arolla::OptionalValue<T>` can be referenced in a similar way:
`::arolla::GetQType<::arolla::OptionalValue<T>>()`, or as
`::arolla::GetOptionalQType<T>()`.

### Defining a QType

The macro `AROLLA_DEFINE_SIMPLE_QTYPE(NAME, CPP_TYPE)` can be used in namespace
`arolla` to define `QType` for copy constructible objects.

```c++
/* header file */
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/simple_qtype.h"

namespace my_project {
struct Point { int x, y; };
}  // namespace my_project

namespace arolla {
AROLLA_DECLARE_SIMPLE_QTYPE(POINT, ::my_project::Point);
AROLLA_DECLARE_OPTIONAL_QTYPE(POINT, ::my_project::Point);  // optional
}  // namespace arolla
```

```c++
/* cc file */
namespace arolla {
AROLLA_DEFINE_SIMPLE_QTYPE(POINT, ::my_project::Point);
AROLLA_DEFINE_OPTIONAL_QTYPE(POINT, ::my_project::Point);
}  // namespace arolla

```

After that, `GetQType<Point>()` and `GetOptionalQType<Point>()` can be used.

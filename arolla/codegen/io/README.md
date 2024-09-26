# Codegeneration for `InputLoader` and `SlotListener`

## InputLoader

Loading data to arolla evaluation engine is represented by
`::arolla::InputLoader` interface. Implementing an interface directly could
require a lot of boilerplate code. Codegeneration is one of the way to reduce
code and make it easy to read.

To generate `InputLoader` from a simple struct or a protobuf one needs to load
bzl function from `io.bzl`. The main function is `input_loader`.

```
load(
    "//arolla/codegen/io:io.bzl",
    "input_loader",
    "path_accessor",
)
```

In the following example

```
input_loader(
    name = "input_loader",
    hdrs = ["path/input_lib.h"],
    # accessors = [...],  will be explained later
    input_cls = "::data_namespace::Input",
    loader_name = "::my_namespace::MyLoader",
    deps = [
      "//path:input_lib",
    ],
)
```

Library `:input_loader` will be generated, which can be included as
`input_loader.h`. Single function `my_namespace::MyLoader` with signature
`const ::arolla::InputLoader<::data_namespace::Input>&()` will be present in the
header.

`accessors` parameter represents the way data will be extracted from `Input`.
Following accessors exist now:

* path_accessor(path) providing scalar appending path to input variable as
c++ code.
* protopath_accessor(path) providing OptionalValue by XPath-like string.

Assume that you have a simple struct:

```
struct Input {
  int x;
  std::vector<double> ys;
};
```

Here is an example to generate accessor for `x` and `ys.size()`.

```
input_loader(
    ...
    accessors = [
        [
            "x",
            path_accessor("x"),
        ],
        [
            "ys_size",
            path_accessor("ys.size()"),
        ],
    ],
)
```

In order to generate `InputLoader` for the entire proto one can use
`proto_fields_accessor`.

```
input_loader(
    name = "descriptor_input_loader",
    hdrs = [
        "arolla/proto/testing/test.pb.h",
        "arolla/proto/testing/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.testing.test_pb2.Root",
            py_deps = ["//arolla/proto/testing:test_py_proto"],
        ),
    ],
    array_type = "DenseArray",
    input_cls = "testing_namespace::Root",,
    loader_name = "::my_namespace::GetDescriptorBasedLoader",
    deps = [
        "//arolla/proto/testing:test_cc_proto",
        "//arolla/proto/testing:test_proto3_cc_proto",
    ],
)
```

See more examples in `arolla/codegen/io/testing/BUILD`.

## SlotListener

Reporting intermediate results from the evaluation context can be done using
`::arolla::SlotListener` interface. Implementing an interface directly could
require a lot of boilerplate code. Codegeneration is one of the way to reduce
code and make it easy to read.

To generate `SlotListener` from a protobuf one needs to load
bzl function from `io.bzl`. The main function is `slot_listener`.

```
slot_listener(
    name = "my_slot_listener",
    hdrs = [
        "arolla/proto/testing/test.pb.h",
        "arolla/proto/testing/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.testing.test_pb2.Root",
            py_deps = [
                "//arolla/proto/testing:test_py_proto",
            ],
        ),
    ],
    array_type = "DenseArray",
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetSlotListener",
    deps = [
        "//arolla/proto/testing:test_cc_proto",
        "//arolla/proto/testing:test_proto3_cc_proto",
    ],
)
```

Library `:my_slot_listener` will be generated, which can be included as
`my_slot_listener.h`. Single function `my_namespace::GetSlotListener` with
signature `const ::arolla::SlotListener<::testing_namespace::Root>&()` will be
present in the header.

### Implementation details for the proto accessors.

Proto `InputLoader` and `SlotListener` are quite popular and often the first
choice in case the data is already in the proto.

Generated code is trying to be smart in order to avoid traversing data multiple
times and load (or output) all requested inputs/outputs efficiently.

Both `InputLoader` and `SlotListener` allow to request a subset of supported
fields. And generated code traverses only fields that were requested.

E.g., if generally the following protopaths exist:

```
a/x
a/y
b/q
b/w
```

Generated code will build a tree (initialization time):

```
a -> a/x
  \--> a/y
b -> b/q
  \--> b/w
```

For each intermediate node we will precompute (in the initialization) whenever
any node in the subtree were requested.

Generated code will look like that:

```
constexpr in A_NODE_ID = 0;
constexpr in B_NODE_ID = 1;

const auto& tmp_0 = input;
[&]() {
  if (node_requested_[A_NODE_ID]) {
    if (!tmp_0.has_a()) {
      // Process leaves a/x and a/y as missed
      return;
    }
    const auto& tmp_1 = tmp_0.a();
    // process leaves a/x and a/y
  }
}();
if (node_requested_[B_NODE_ID]) {
  // process leaves b/q and b/w similarly
}
```

As an optimization we do not check in case there is only one child. E.g.,

```
a -> a/q -> a/q/w
        \--->  a/q/e
```

Generated code will look like that:

```
constexpr in A_NODE_ID = 0;
constexpr in AQ_NODE_ID = 1;

const auto& tmp_0 = input;
[&]() {
  if (node_requested_[A_NODE_ID]) {
    if (!tmp_0.has_a()) {
      // Process leaves a/q/w and a/q/e as missed
      return;
    }
    const auto& tmp_1 = tmp_0.a();
    // no need to check AQ_NODE_ID
    if (!tmp_1.has_q()) {
      // Process leaves a/q/w and a/q/e as missed
      return;
    }
    const auto& tmp_2 = tmp_1.q();
    // Process leaves a/q/w and a/q/e
  }
}();
```

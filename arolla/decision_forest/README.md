# What is decision forest?

Decision trees are some of the most easily interpreted models for machine
learning. They consist of a sequence of tests starting from the root node.
For each example, if a test at the current node passes, then we follow the left
subtree, otherwise we follow the right subtree, until we reach a leaf node.
The leaf node determines the appropriate score or label for the example.

Whereas deeper decision trees perform better on the metric evaluated on
the training set, they tend to overfit and do not generalize as well to novel
data. However, shallow decision trees tend to underfit and are susceptible
to inductive biases that prevent them from performing better across all
problems. Instead of using just a single tree, we can usually ensemble many
trees. When multiple trees are combined into a single model, the model is
referred to as a decision forest.

# Subdirectories

* `decision_forest/` - decision forest data structures.

* `decision_forest/split_conditions/` - specific implementations of the
    SplitCondition interface. Needed to construct DecisionForest.

* `decision_forest/pointwise_evaluation/` - optimized pointwise evaluator
    for DecisionForest.

* `decision_forest/batched_evaluation` - batched evaluator for DecisionForest.
    Uses `decision_forest/pointwise_evaluation`.

* `decision_forest/qexpr_operator/` - qexpr interface for
    `decision_forest/pointwise_evaluation/` and
    `decision_forest/batched_evaluation/`.

* `decision_forest/expr_operator/` - expr-level decision forest operator.

* `decision_forest/testing/` - test utils like random generation of
    DecisionForest.

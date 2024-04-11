# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A front-end module for rl.testing.*."""

from arolla.testing import detect_qtype_signatures as _detect_qtype_signatures
from arolla.testing import test_utils as _test_utils

# go/keep-sorted start block=yes
assert_expr_equal_by_fingerprint = _test_utils.assert_expr_equal_by_fingerprint
assert_qvalue_allclose = _test_utils.assert_qvalue_allclose
assert_qvalue_allequal = _test_utils.assert_qvalue_allequal
assert_qvalue_equal_by_fingerprint = (
    _test_utils.assert_qvalue_equal_by_fingerprint
)
# go/keep-sorted end

# go/keep-sorted start block=yes
DETECT_SIGNATURES_DEFAULT_QTYPES = (
    _detect_qtype_signatures.DETECT_SIGNATURES_DEFAULT_QTYPES
)
detect_qtype_signatures = _detect_qtype_signatures.detect_qtype_signatures
# go/keep-sorted end

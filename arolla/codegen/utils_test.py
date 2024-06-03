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

"""Tests for utils."""

import math

from absl.testing import absltest
from absl.testing import parameterized

from arolla.codegen import utils


def return_my_args(*args, **kwargs):
  return dict(my_args=args, my_kwargs=kwargs)


RETURN_MY_ARGS = 'arolla.codegen.utils_test.return_my_args'


class UtilsTest(parameterized.TestCase):

  def test_call_function(self):
    gcd_spec = {'__call_python_func__': 1, 'fn': 'math.gcd', 'args': [57, 76]}
    self.assertEqual(utils.call_function(gcd_spec), 19)
    self.assertEqual(
        utils.call_function({
            '__call_python_func__': 1,
            'fn': 'math.remainder',
            'args': [gcd_spec, 7]
        }), -2.0)
    with self.assertRaises(AssertionError):
      utils.call_function({'fn': 'math.gcd', 'args': [57, 67]})

  def test_call_function_with_kwargs(self):
    func_spec = {
        '__call_python_func__': 1,
        'fn': RETURN_MY_ARGS,
        'args': [
            1, 2, {
                '__call_python_func__': 1,
                'fn': RETURN_MY_ARGS,
                'args': [4, 5, 6],
                'kwargs': {
                    'foo': 'bar'
                }
            }
        ],
        'kwargs': {
            'foo': {
                '__call_python_func__': 1,
                'fn': RETURN_MY_ARGS,
                'args': [7, 8, 9],
                'kwargs': {
                    'foo': 'bar'
                }
            }
        }
    }
    self.assertEqual(
        utils.call_function(func_spec), {
            'my_args': (1, 2, {
                'my_args': (4, 5, 6),
                'my_kwargs': {
                    'foo': 'bar'
                }
            }),
            'my_kwargs': {
                'foo': {
                    'my_args': (7, 8, 9),
                    'my_kwargs': {
                        'foo': 'bar'
                    }
                }
            }
        })
    no_arg_spec = {
        '__call_python_func__': 1,
        'fn': RETURN_MY_ARGS,
    }
    self.assertEqual(
        utils.call_function(no_arg_spec), {
            'my_args': (),
            'my_kwargs': {}
        })

  def test_call_function_with_list_argument(self):
    gcd_spec_1 = {'__call_python_func__': 1, 'fn': 'math.gcd', 'args': [57, 76]}
    gcd_spec_2 = {'__call_python_func__': 1, 'fn': 'math.gcd', 'args': [3, 19]}
    gcd_spec_3 = {
        '__call_python_func__': 1,
        'fn': 'math.gcd',
        'args': [57, 114],
    }
    self.assertEqual(utils.call_function(gcd_spec_1), 19)
    self.assertEqual(utils.call_function(gcd_spec_2), 1)
    self.assertEqual(utils.call_function(gcd_spec_3), 57)
    self.assertEqual(
        utils.call_function({
            '__call_python_func__': 1,
            'fn': 'math.fsum',
            'args': [[gcd_spec_1, gcd_spec_2, gcd_spec_3]]
        }), 77)

  def test_call_function_with_dict_argument(self):
    gcd_spec_1 = {'__call_python_func__': 1, 'fn': 'math.gcd', 'args': [57, 76]}
    gcd_spec_2 = {'__call_python_func__': 1, 'fn': 'math.gcd', 'args': [3, 19]}
    gcd_spec_3 = {
        '__call_python_func__': 1,
        'fn': 'math.gcd',
        'args': [57, 114],
    }
    self.assertEqual(utils.call_function(gcd_spec_1), 19)
    self.assertEqual(utils.call_function(gcd_spec_2), 1)
    self.assertEqual(utils.call_function(gcd_spec_3), 57)
    self.assertEqual(
        utils.call_function({
            '__call_python_func__': 1,
            'fn': RETURN_MY_ARGS,
            'args': [
                dict(first=gcd_spec_1, second=gcd_spec_2, third=gcd_spec_3)
            ],
        }),
        {
            'my_args': ({'first': 19, 'second': 1, 'third': 57},),
            'my_kwargs': {},
        },
    )

  def test_process_call_function_requests(self):
    gcd_spec = {'__call_python_func__': 1, 'fn': 'math.gcd', 'args': [57, 76]}
    context = {
        'list_of_values': [1, gcd_spec, 2],
        'dict_of_values': {
            'nested_call': gcd_spec,
            'other_value': 57,
        },
        'call': gcd_spec,
    }
    self.assertEqual(
        utils.process_call_function_requests(context), {
            'list_of_values': [1, 19, 2],
            'dict_of_values': {
                'nested_call': 19,
                'other_value': 57,
            },
            'call': 19,
        })

  def test_load_function(self):
    self.assertIs(utils.load_function('math.gcd'), math.gcd)

  def test_load_function_invalid_path(self):
    with self.assertRaisesRegex(ImportError, 'this.is.a.fake.path'):
      utils.load_function('this.is.a.fake.path')

  def test_load_function_invalid_function_name(self):
    with self.assertRaisesRegex(AttributeError, 'math.*gcd_fake'):
      utils.load_function('math.gcd_fake')

  def test_load_function_not_a_function(self):
    path = 'arolla.codegen.utils_test.RETURN_MY_ARGS'
    with self.assertRaisesRegex(TypeError, f'{path!r}.*callable.*was.*str'):
      utils.load_function(path)

  def test_merge_dicts(self):
    self.assertEqual(
        utils.merge_dicts(
            {'a': 3, 'b': (20, 10)}, {'c': True, 'b': 0.5}, {'x': False}
        ),
        {'a': 3, 'b': 0.5, 'c': True, 'x': False},
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='empty',
          str_list=[],
          expected=[],
      ),
      dict(
          testcase_name='single_element',
          str_list=['abc'],
          expected=['abc'],
      ),
      dict(
          testcase_name='general',
          str_list=['abc', 'abcde', 'abcde', 'axvf', 'abcde'],
          expected=['abc', 'de', '', 'xvf', 'bcde'],
      ),
  )
  def test_remove_common_prefix_with_previous_string(self, str_list, expected):
    got = utils.remove_common_prefix_with_previous_string(str_list)
    self.assertEqual(expected, got)


if __name__ == '__main__':
  absltest.main()

# Copyright 2011-2012 Yelp
# Copyright 2016 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import platform
import sys

import doloop

try:
    from setuptools import setup
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'provides': ['doloop'],
        'test_suite': 'tests.suite.load_tests',
        'tests_require': ['PyMySQL'],
    }

    # unittest2 is a backport of unittest from Python 2.7
    if sys.version_info < (2, 7):
        setuptools_kwargs['tests_require'].append('unittest2')

    # only add MySQLdb for Python 2
    if sys.version_info < (3, 0):
        setuptools_kwargs['tests_require'].append('MySQL-python')

    # mysql-connector doesn't work on Python 3.2 (this happens for pypy3)
    if sys.version_info < (3, 0) or sys.version_info >= (3, 3):
        setuptools_kwargs['tests_require'].append('mysql-connector')

    # oursql seems not to work right with PyPy
    if platform.python_implementation() == 'CPython':
        if sys.version_info < (3, 0):
            setuptools_kwargs['tests_require'].append('oursql')
        else:
            setuptools_kwargs['tests_require'].append('oursql3')

except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}


setup(
    author='David Marin',
    author_email='dave@yelp.com',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
    ],
    description='Task loop for keeping things updated',
    entry_points=dict(
        console_scripts=[
            'create-doloop-table=doloop:_main_for_create_doloop_table'
        ],
    ),
    license='Apache',
    long_description=open('README.rst').read(),
    name='doloop',
    py_modules=['doloop'],
    url='http://github.com/Yelp/doloop',
    version=doloop.__version__,
    **setuptools_kwargs
)

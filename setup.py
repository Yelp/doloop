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
import sys

import doloop

try:
    from setuptools import setup
    # arguments that distutils doesn't understand
    setuptools_kwargs = {
        'provides': ['doloop'],
        'test_suite': 'tests.suite.load_tests',
        'tests_require': ['PyMySQL', 'mysql-connector', 'oursql'],
    }

    # unittest2 is a backport of unittest from Python 2.7
    if sys.version_info < (2, 7):
        setuptools_kwargs['tests_require'].append('unittest2')

    # only add MySQLdb for Python 2
    if sys.version_info < (3, 0):
        setuptools_kwargs['tests_require'].append('MySQL-python')

except ImportError:
    from distutils.core import setup
    setuptools_kwargs = {}


setup(
    author='David Marin',
    author_email='dave@yelp.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    ],
    description='Task loop for keeping things updated',
    license='Apache',
    long_description=open('README.rst').read(),
    name='doloop',
    py_modules=['doloop'],
    scripts=['bin/create-doloop-table'],
    url='http://github.com/Yelp/doloop',
    version=doloop.__version__,
    **setuptools_kwargs
)

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import doloop

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
    provides=['doloop'],
    py_modules=['doloop'],
    scripts=['bin/create-doloop-table'],
    url='http://github.com/Yelp/doloop',
    version=doloop.__version__,
)

#!/usr/bin/env python
"""
Mezmorize
-----------

Adds function memoization support

"""

from setuptools import setup

setup(
    name='Mezmorize',
    version='0.13',
    url='http://github.com/kazeeki/mezmorize',
    license='BSD',
    author='Thadeus Burgess',
    author_email='thadeusb@thadeusb.com',
    description='Adds function memoization support',
    long_description=__doc__,
    packages=['mezmorize'],
    zip_safe=False,
    platforms='any',
    test_suite='test_cache',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

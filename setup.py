#!/usr/bin/env python
"""
Mezmorize
-----------

Adds function memoization support

"""
import sys

from os import path as p

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


def read(filename, parent=None):
    parent = (parent or __file__)

    try:
        with open(p.join(p.dirname(parent), filename)) as f:
            return f.read()
    except IOError:
        return ''


def parse_requirements(filename, parent=None):
    parent = (parent or __file__)
    filepath = p.join(p.dirname(parent), filename)
    content = read(filename, parent)

    for line_number, line in enumerate(content.splitlines(), 1):
        candidate = line.strip()

        if candidate.startswith('-r'):
            for item in parse_requirements(candidate[2:].strip(), filepath):
                yield item
        else:
            yield candidate

requirements = parse_requirements('requirements.txt')
packages = find_packages(exclude=['tests'])

# Avoid byte-compiling the shipped template
sys.dont_write_bytecode = True

setup(
    name='Mezmorize',
    version='0.14.0',
    url='http://github.com/kazeeki/mezmorize',
    license='BSD',
    author='Thadeus Burgess',
    author_email='thadeusb@thadeusb.com',
    description='Adds function memoization support',
    long_description=__doc__,
    packages=packages,
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
    ],
    install_requires=requirements,
)

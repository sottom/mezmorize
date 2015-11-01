#!/usr/bin/env python
"""
Mezmorize
-----------

Adds function memoization support

"""
import sys
import re

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


def parse_requirements(filename, parent=None, dep=False):
    parent = (parent or __file__)
    filepath = p.join(p.dirname(parent), filename)
    content = read(filename, parent)

    for line_number, line in enumerate(content.splitlines(), 1):
        candidate = line.strip()

        if candidate.startswith('-r'):
            args = [candidate[2:].strip(), filepath, dep]

            for item in parse_requirements(*args):
                yield item
        elif not dep and '#egg=' in candidate:
            yield re.sub('.*#egg=(.*)-(.*)', r'\1==\2', candidate)
        elif dep and '#egg=' in candidate:
            yield candidate.replace('-e ', '')
        elif not dep:
            yield candidate


requirements = list(parse_requirements('requirements.txt'))
readme = read('README')

# Avoid byte-compiling the shipped template
sys.dont_write_bytecode = True

setup(
    name='Mezmorize',
    version='0.16.1',
    url='http://github.com/kazeeki/mezmorize',
    license='BSD License',
    author='Reuben Cummings',
    author_email='reubano@gmail.com',
    description='Adds function memoization support',
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    zip_safe=False,
    platforms=['MacOS X', 'Windows', 'Linux'],
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

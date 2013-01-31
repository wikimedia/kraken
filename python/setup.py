#!python
# -*- coding: utf-8 -*-
import sys, os, re
from os.path import dirname, abspath, join
from setuptools import setup, find_packages

HERE = abspath(dirname(__file__))
readme = open(join(HERE, 'README.md'), 'rU').read()

package_file = open(join(HERE, 'kraken/__init__.py'), 'rU')
__version__ = re.sub(
    r".*\b__version__\s+=\s+'([^']+)'.*",
    r'\1',
    [ line.strip() for line in package_file if '__version__' in line ].pop(0)
)


setup(
    name             = 'kraken',
    version          = __version__,
    description      = 'kraken cluster tools',
    long_description = readme,
    url              = 'http://github/dsc/kraken',
    
    author           = 'David Schoonover',
    author_email     = 'dsc@less.ly',
    
    # packages         = find_packages(),
    py_modules       = [ 'kraken', ],
    # entry_points     = { 'console_scripts':['kraken = kraken:Kraken.main'] },
    
    install_requires = [
        "bunch  >= 1.0",
        "PyYAML >= 3.10",
        "fabric >= 1.4.2",
        "paramiko >= 1.7.7",
        "path.py >= 2.2.2",
    ],
    
    keywords         = 'kraken tools utilities deploy',
    classifiers      = [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Topic :: Utilities"
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
    zip_safe = False,
    license  = "MIT",
)

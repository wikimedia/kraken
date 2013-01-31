#!/usr/bin/env fab
# -*- coding: utf-8 -*-
"Kraken Cluster Tools"

from __future__ import with_statement
import sys
from functools import wraps


# Deal with the fact that we aren't *really* a Python project,
# so we haven't declared python dependencies.
try:
    # To build this project using fabric, you'll need to install fabric (and some other stuff)
    from fabric.api import *
    from path import path as p # renamed to avoid conflict w/ fabric.api.path
    # Dep for the crazy SSH Proxy Gateway monkeypatch
    import paramiko
except ImportError:
    print """ 
        ERROR: You're missing a dependency!
        To build this project using fabric, you'll need to install fabric (and some other stuff):
            
            pip install -U fabric paramiko path.py
        """
    sys.exit(1)

# My mind is blown. This totally works.
# see: https://github.com/fabric/fabric/issues/38#issuecomment-5608014
import monkeypatch_sshproxy
from util import *



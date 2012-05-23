#!/usr/bin/env fab
# -*- coding: utf-8 -*-
"Installation Tasks"

from __future__ import with_statement
from contextlib import contextmanager
from functools import wraps
from path import path as p   # renamed to avoid conflict w/ fabric.api.path

from fabric.api import *
from fabric.colors import white, blue, cyan, green, yellow, red, magenta
from fabric.contrib.files import exists
from fabric.contrib.project import rsync_project

from util import *






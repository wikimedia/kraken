#!/usr/bin/env fab
# -*- coding: utf-8 -*-

from __future__ import with_statement
from contextlib import contextmanager
from functools import wraps
from path import path as p # renamed to avoid conflict w/ fabric.api.path

from fabric.api import *
from fabric.colors import white, blue, cyan, green, yellow, red, magenta

__all__ = (
    'quietly', 'msg', 'branches', 'working_branch',
    'defaults', 'expand', 'expand_env', 'format', 'expand_env',
)


### Context Managers

@contextmanager
def quietly(txt):
    "Wrap a block in a message, suppressing other output."
    puts(txt + "...", end='', flush=True)
    with hide('everything'): yield
    puts("woo.", show_prefix=False, flush=True)


### Decorators

def msg(txt, quiet=False):
    "Decorator to wrap a task in a message, optionally suppressing all output."
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            if quiet:
                puts(green(txt + '...', bold=True), end='', flush=True)
                with hide('everything'):
                    result = fn(*args, **kwargs)
                puts(white('Woo.\n'), show_prefix=False, flush=True)
            else:
                puts(green(txt + '...', bold=True))
                result = fn(*args, **kwargs)
                puts(white('Woo.\n'))
            return result
        return inner
    return outer



### Git Integration

def branches(remotes=False, all=False):
    "List of git branchs."
    opts = {
        'remotes' : '--remotes' if remotes else '',
        'all'     : '--all' if all else ''
    }
    return [ branch.strip().split()[-1] for branch in local('git branch %(remotes)s %(all)s' % opts, capture=True).split('\n') ]

def working_branch():
    "Determines the working branch."
    return [ branch.split()[1] for branch in local('git branch', capture=True).split('\n') if '*' in branch ][0]




### Misc

def defaults(target, *sources):
    "Update target dict using `setdefault()` for each key in each source."
    for source in sources:
        for k, v in source.iteritems():
            target.setdefault(k, v)
    return target

def expand(s):
    "Recursively expands given string using the `env` dict."
    is_path = isinstance(s, p)
    prev = None
    while prev != s:
        prev = s
        s = s % env
    return p(s) if is_path else s

def format(s):
    "Recursively formats string using the `env` dict."
    is_path = isinstance(s, p)
    prev = None
    while prev != s:
        prev = s
        s = s.format(**env)
    return p(s) if is_path else s


@runs_once
def _expand_env():
    for k, v in env.iteritems():
        if not isinstance(v, basestring): continue
        env[k] = expand(env[k])

def expand_env(fn):
    "Decorator expands all strings in `env`."
    
    @wraps(fn)
    def wrapper(*args, **kwargs):
        _expand_env()
        return fn(*args, **kwargs)
    
    return wrapper


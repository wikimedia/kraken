#!/usr/bin/env fab
# -*- coding: utf-8 -*-
"User Env Tasks"

from __future__ import with_statement
from contextlib import contextmanager
from functools import wraps
from path import path as p   # renamed to avoid conflict w/ fabric.api.path

from fabric.api import *
from fabric.colors import white, blue, cyan, green, yellow, red, magenta
from fabric.contrib.files import exists
from fabric.contrib.project import rsync_project

from util import *


# - Users
#     - Create Users: `cass`, `etl`, `zk`, `cdh4`, `cdh3`, `bundler`
#     - Create Group: `kraken`
#     - Add users to group `kraken`
#     - Create ssh keys for each user
#     - Aggregate & propagate `.ssh/authorized_keys`
#     - Push keys into repo



def groupadd(groupname):
    "Create a group."
    sudo('groupadd %s' % groupname)

def useradd(username, groups=''):
    "Create a user."
    groups = ','.join(groups) if groups else ''
    sudo('useradd --create-home --user-group --groups %(groups)s %(username)s' % locals())

def ssh_keygen(username, comment=''):
    with cd('~'):
        sudo('ssh-keygen -t rsa %s' % ("-C '%s'" % comment if comment else ''), user=username)

@task
def create_users():
    pass




# coding: utf-8
from __future__ import absolute_import
from __future__ import unicode_literals

import re
import time
import collections
import warnings

import fs
import semantic_version

try:
    import docker
    docker_client = docker.from_env(version='auto')
except Exception as err:
    warnings.warn("Failed to start Docker client: {}".format(err))
    docker_client = None

try:
    from unittest import mock   # pylint: disable=unused-import
except ImportError:
    import mock                 # pylint: disable=unused-import


def fs_version():
    return semantic_version.Version(fs.__version__)


def startServer(docker_client, user, pasw, port):
    sftp_container = docker_client.containers.run(
        "lscr.io/linuxserver/openssh-server", detach=True, ports={'2222/tcp': port},
        environment={'USER_NAME': user, 'USER_PASSWORD': pasw, "PASSWORD_ACCESS": "true"},
    )
    time.sleep(1)
    return sftp_container


def stopServer(server_container):
    server_container.kill()
    server_container.remove()

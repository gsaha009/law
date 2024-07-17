# coding: utf-8

"""
Pbs utilities.
"""

__all__ = ["get_pbs_version"]


import re
import subprocess
import threading

from law.util import no_value, interruptable_popen


_pbs_version = no_value
_pbs_version_lock = threading.Lock()


def get_pbs_version():
    """
    Returns the version of the Pbs installation in a 3-tuple. The value is cached to accelerate
    repeated function invocations.
    """
    global _pbs_version

    if _pbs_version == no_value:
        version = None
        with _pbs_version_lock:
            code, out, info = interruptable_popen("qstat --version", shell=True,
                executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if code == 0:
                first_line = info.strip().split("\n")[0]
                m = re.match(r"^Version: (\d+)\.(\d+)\.(\d+).*$", first_line.strip())
                if m:
                    version = tuple(map(int, m.groups()))

            _pbs_version = version

    return _pbs_version

#  Copyright (C) 2011  Equinor ASA, Norway.
#
#  The file '__init__.py' is part of ERT - Ensemble based Reservoir Tool.
#
#  ERT is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  ERT is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE.
#
#  See the GNU General Public License at <http://www.gnu.org/licenses/gpl.html>
#  for more details.
"""
The job_queue package contains modules and classes for running
external commands.
"""

# Getting LSF to work properly is quite painful. The situation
# is a mix of build complexity and LSF specific requirements:
#
#   1. The LSF libraries are accessed from the libjob_queue.so
#      library, but observe that the dependancy on the liblsf and
#      libbat libraries is through dlopen(), i.e. runtime. This module
#      will therefore load happily without access to the lsf libraries.
#
#      If you at a later stage create a lsf driver the runtime
#      environment must be able to locate the liblsf.so, libbat.so and
#      libnsl.so shared libraries, either through LD_LIBRARY_PATH or
#      other means.
#
#   2. To actually use LSF you need a whole list of environment
#      variables to be set: LSF_BINDIR , LSF_LIBDIR , XLDF_UIDDIR ,
#      LSF_SERVERDIR, LSF_ENVDIR - this is an LSF requirement and not
#      related to ERT or the Python bindings. The normal way to
#      achieve this is by sourcing a shell script.
#
#      If the environment variable LSF_HOME is set we set the
#      remaining LSF variables according to:
#
#           LSF_BINDIR    = $LSF_HOME/bin
#           LSF_LIBDIR    = $LSF_HOME/lib
#           XLSF_UIDDIR   = $LSF_HOME/lib/uid
#           LSF_SERVERDIR = $LSF_HOME/etc
#           LSF_ENVDIR    = $LSF_HOME/conf
#           PATH          = $PATH:$LSF_BINDIR
#
#      Observe that none of these variables are modified if they
#      already have a value, furthermore it should be observed that
#      the use of an LSF_HOME variable is something invented with ERT,
#      and not standard LSF approach.


import os

import res
from cwrap import Prototype


def setenv(var, value):
    if not os.getenv(var):
        os.environ[var] = value


# Set up the full LSF environment - based onf LSF_HOME
LSF_HOME = os.getenv("LSF_HOME")
if LSF_HOME:
    setenv("LSF_BINDIR", "%s/bin" % LSF_HOME)
    setenv("LSF_LIBDIR", "%s/lib" % LSF_HOME)
    setenv("XLSF_UIDDIR", "%s/lib/uid" % LSF_HOME)
    setenv("LSF_SERVERDIR", "%s/etc" % LSF_HOME)
    setenv("LSF_ENVDIR", "%s/conf" % LSF_HOME)  # This is wrong: Equinor: /prog/LSF/conf

from .driver import Driver, LocalDriver, LSFDriver, QueueDriverEnum, RSHDriver
from .environment_varlist import EnvironmentVarlist
from .ert_plugin import CancelPluginException, ErtPlugin
from .ert_script import ErtScript
from .ext_job import ExtJob
from .ext_joblist import ExtJoblist
from .external_ert_script import ExternalErtScript
from .forward_model import ForwardModel
from .forward_model_status import ForwardModelJobStatus, ForwardModelStatus
from .function_ert_script import FunctionErtScript
from .job import Job
from .job_queue_manager import JobQueueManager
from .job_queue_node import JobQueueNode
from .job_status_type_enum import JobStatusType
from .job_submit_status_type_enum import JobSubmitStatusType
from .queue import JobQueue
from .run_status_type_enum import RunStatusType
from .thread_status_type_enum import ThreadStatus
from .workflow import Workflow
from .workflow_job import WorkflowJob
from .workflow_joblist import WorkflowJoblist
from .workflow_runner import WorkflowRunner

__all__ = [
    "JobStatusType",
    "RunStatusType",
    "ThreadStatus",
    "JobSubmitStatusType",
    "Job",
    "Driver",
    "QueueDriverEnum",
    "JobQueueNode",
    "JobQueue",
    "JobQueueManager",
    "QueueDriverEnum",
    "Driver",
    "LSFDriver",
    "RSHDriver",
    "LocalDriver",
    "ExtJob",
    "ExtJoblist",
    "EnvironmentVarlist",
    "ForwardModel",
    "ForwardModelJobStatus",
    "ForwardModelStatus",
    "ErtScript",
    "ErtPlugin",
    "CancelPluginException",
    "FunctionErtScript",
    "ExternalErtScript",
    "WorkflowJob",
    "WorkflowJoblist",
    "Workflow",
    "WorkflowRunner",
]

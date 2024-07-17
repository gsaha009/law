# coding: utf-8

"""
Pbs workflow implementation. 
See: 
  https://albertsk.org/wp-content/uploads/2011/12/pbs.pdf
  https://www.nrel.gov/hpc/assets/pdfs/pbs-to-slurm-translation-sheet.pdf
"""

__all__ = ["PbsWorkflow"]


import os
from abc import abstractmethod
from collections import OrderedDict

import luigi

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile
from law.task.proxy import ProxyCommand
from law.target.file import get_path, get_scheme, FileSystemDirectoryTarget
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR
from law.util import law_src_path, merge_dicts, DotDict
from law.logger import get_logger

from law.contrib.pbs.job import PbsJobManager, PbsJobFileFactory


logger = get_logger(__name__)


class PbsWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "pbs"

    def create_job_manager(self, **kwargs):
        return self.task.pbs_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.pbs_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        print("inside create_job_file")
        task = self.task

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)

        # create the config
        c = self.job_file_factory.get_config()
        c.input_files = {}
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.pbs_wrapper_file()
        law_job_file = task.pbs_job_file()
        if wrapper_file and get_path(wrapper_file) != get_path(law_job_file):
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            c.executable = law_job_file
        c.input_files["job_file"] = law_job_file

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_pbs_workflow |
            {"workflow", "effective_workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers", "local-scheduler", task.task_family + "-*"],
        )
        if task.pbs_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.pbs_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            workers=task.job_workers,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.job_data.attempts.get(job_num, 0)),
        )
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.pbs_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.pbs_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # logging
        # we do not use pbs's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        c.stdout = "/dev/null"
        c.stderr = None
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # when the output dir is local, we can run within this directory for easier output file
        # handling and use absolute paths for input files
        output_dir = task.pbs_output_directory()
        if not isinstance(output_dir, FileSystemDirectoryTarget):
            output_dir = get_path(output_dir)
            if get_scheme(output_dir) in (None, "file"):
                output_dir = LocalDirectoryTarget(output_dir)
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.custom_content.append(("chdir", output_dir.abspath))

        # job name
        c.job_name = "{}{}".format(task.live_task_id, postfix)

        # task arguments
        if task.pbs_partition and task.pbs_partition != NO_STR:
            c.partition = task.pbs_partition

        # task hook
        c = task.pbs_job_config(c, job_num, branches)

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(output_dir.abspath, c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self):
        info = super(PbsWorkflowProxy, self).destination_info()

        info = self.task.pbs_destination_info(info)

        return info


class PbsWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = PbsWorkflowProxy

    pbs_workflow_run_decorators = None
    pbs_job_manager_defaults = None
    pbs_job_file_factory_defaults = None

    pbs_partition = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target queue partition; default: empty",
    )

    pbs_job_kwargs = ["pbs_partition"]
    pbs_job_kwargs_submit = None
    pbs_job_kwargs_cancel = None
    pbs_job_kwargs_query = None

    exclude_params_branch = {"pbs_partition"}

    exclude_params_pbs_workflow = set()

    exclude_index = True

    @abstractmethod
    def pbs_output_directory(self):
        return None

    def pbs_workflow_requires(self):
        return DotDict()

    def pbs_bootstrap_file(self):
        return None

    def pbs_wrapper_file(self):
        return None

    def pbs_job_file(self):
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def pbs_stageout_file(self):
        return None

    def pbs_output_postfix(self):
        return ""

    def pbs_job_manager_cls(self):
        return PbsJobManager

    def pbs_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.pbs_job_manager_defaults, kwargs)
        return self.pbs_job_manager_cls()(**kwargs)

    def pbs_job_file_factory_cls(self):
        return PbsJobFileFactory

    def pbs_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.pbs_job_file_factory_defaults, kwargs)
        return self.pbs_job_file_factory_cls()(**kwargs)

    def pbs_job_config(self, config, job_num, branches):
        return config

    def pbs_check_job_completeness(self):
        return False

    def pbs_check_job_completeness_delay(self):
        return 0.0

    def pbs_use_local_scheduler(self):
        return False

    def pbs_cmdline_args(self):
        return {}

    def pbs_destination_info(self, info):
        return info

    def pbs_job_resources(self, jobnum, branches):
        return {"select":1, "ncpus":1}

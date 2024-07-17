# coding: utf-8

"""
Function returning the config defaults of the pbs package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "pbs_job_file_dir": None,
            "pbs_job_file_dir_mkdtemp": None,
            "pbs_job_file_dir_cleanup": False,
            "pbs_chunk_size_cancel": 25,
            "pbs_chunk_size_query": 25,
        },
    }

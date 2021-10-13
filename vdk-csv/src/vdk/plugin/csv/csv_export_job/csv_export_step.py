# Copyright 2021 VMware, Inc.
# SPDX-License-Identifier: Apache-2.0
import logging
import pathlib
from vdk.api.job_input import IJobInput

from vdk.internal.core import errors

log = logging.getLogger(__name__)



def run(job_input: IJobInput) -> None:
    csv_file = pathlib.Path(job_input.get_arguments().get("file"))
    query = job_input.get_arguments().get("query", None)
    if not query:
        raise errors.log_and_throw(errors.ResolvableBy.USER_ERROR, log,
                                   "Invalid query.", "Query is empty.", "Cannot export data with SQL query.", "Specify query.")
    import pandas as pd
    df = pd.read_sql_query(query, job_input.get_managed_connection())
    df.to_csv(pathlib.Path(csv_file))

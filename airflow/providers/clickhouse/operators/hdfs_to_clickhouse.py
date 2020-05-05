#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import json

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

try:
    from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook
except ImportError:
    from airflow.hooks.hdfs_hook import HDFSHook
from airflow.providers.clickhouse.hooks.clickhouse import ClickhouseHook, ClickhouseFormat
from airflow.utils.decorators import apply_defaults


class HdfsToClickhouseTransfer(BaseOperator):
    """
    Moves data from hdfs files to Clickhouse table.

    :param hdfs_paths: hdfs paths to read from. (templated)
    :type hdfs_paths: [Union[str, List[str]]]
    :param clickhouse_table: Clickhouse table. (templated)
    :type clickhouse_table: str
    :param hdfs_conn_id: source connection
    :type hdfs_conn_id: str
    :param clickhouse_conn_id: Clickhouse connection
    :type clickhouse_conn_id: str
    :param row_format: input format
    :type row_format: str
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('clickhouse_table', 'hdfs_paths')
    ui_color = '#b0f07c'

    @apply_defaults
    def __init__(
            self,
            hdfs_paths,
            clickhouse_table,
            hdfs_conn_id='hdfs_default',
            clickhouse_conn_id='clickhouse_default',
            row_format=ClickhouseFormat.TABSEPARATED,
            timeout=30,
            auth=None,
            *args, **kwargs):
        super(HdfsToClickhouseTransfer, self).__init__(*args, **kwargs)
        self.hdfs_paths = hdfs_paths if isinstance(hdfs_paths, list) else [hdfs_paths]
        self.clickhouse_table = clickhouse_table
        self.hdfs_conn_id = hdfs_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id
        self.row_format = row_format
        self.timeout = timeout
        self.auth = auth

    def execute(self, context):
        """
        Executes the transfer operation from hdfs to Clickhouse.

        :param context: The context that is being provided when executing.
        :type context: dict
        """
        self.log.info("Connecting to hdfs using %s connection." % self.clickhouse_conn_id)
        clickhouse = ClickhouseHook(clickhouse_conn_id=self.clickhouse_conn_id, auth=self.auth)

        self.log.info("Connecting to hdfs using %s connection." % self.hdfs_conn_id)
        try:
            hdfs = HDFSHook(hdfs_conn_id=self.hdfs_conn_id)
            hdfs_client = hdfs.get_conn()
        except Exception as e:
            raise AirflowException("Failed to retireve hdfs client.", e)

        self.log.info("Checking hdfs paths %s" % ', '.join(self.hdfs_paths))
        try:
            ls = list(hdfs_client.ls(self.hdfs_paths))
            self.log.info("Total files: %s" % len(ls))
            total_size = sum([f.get('blocksize', 0) for f in ls])
            if total_size == 0:
                self.log.warning("Files are empty, skipping insert.")
                return
        except Exception as e:
            raise AirflowException("Error checking hdfs paths.", e)

        self.log.info("Reading from hdfs.")
        try:
            hdfs_stdout_gen = hdfs_client.cat(self.hdfs_paths)
            self.log.info("Loading into clickhouse table %s..." % self.clickhouse_table)
            total_rows = 0
            for hdfs_stdout in hdfs_stdout_gen:
                r = clickhouse.insert_rows(table=self.clickhouse_table, data=hdfs_stdout,
                                           row_format=self.row_format, timeout=self.timeout,
                                           params={'send_progress_in_http_headers': 1})
                written_rows = json.loads(r.headers['X-ClickHouse-Summary']).get('written_rows')
                if written_rows:
                    total_rows += int(written_rows)
                else:
                    self.log.warning('Failed to retrieve row count.')
            if total_rows == 0:
                raise AirflowException("Inserted rows: 0")
        except Exception as e:
            raise AirflowException("Error inserting into clickhouse", e)
        self.log.info("Successfully inserted %s rows into %s." % (total_rows, self.clickhouse_table))

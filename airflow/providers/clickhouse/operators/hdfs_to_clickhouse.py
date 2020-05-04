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

from typing import List, Union, Optional, Dict, Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.hdfs.hooks.hdfs import HDFSHook, HDFSHookException
from airflow.providers.clickhouse.hooks.clickhouse import ClickhouseHook, ClickhouseFormat, ClickhouseException
from airflow.utils.decorators import apply_defaults


class HdfsToClickhouseTransfer(BaseOperator):
    """
    Moves data from hdfs files to Clickhouse table.

    :param hdfs_paths: hdfs paths to read from. (templated)
    :type hdfs_paths: str or list[str]
    :param clickhouse_table: Clickhouse table. (templated)
    :type clickhouse_table: str
    :param hdfs_conn_id: source connection
    :type hdfs_conn_id: str
    :param clickhouse_conn_id: Clickhouse connection
    :type clickhouse_conn_id: str
    :param format: input format
    :type format: str
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
        hdfs_paths: [Union[str, List[str]]],
        clickhouse_table: str,
        hdfs_conn_id='hdfs_default',
        clickhouse_conn_id='clickhouse_default',
        format=ClickhouseFormat.TABSEPARATED,
        extra_options: Optional[Dict[str, Any]] = None,
        *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hdfs_paths = hdfs_paths
        self.clickhouse_table = clickhouse_table
        self.hdfs_conn_id = hdfs_conn_id
        self.clickhouse_conn_id = clickhouse_conn_id
        self.format = format
        self.extra_options = extra_options

    def execute(self, context):
        """
        Executes the transfer operation from hdfs to Clickhouse.

        :param context: The context that is being provided when executing.
        :type context: dict
        """
        self.log.info("Loading %s to clickhouse table %s..." % (','.join(self.hdfs_paths), self.clickhouse_table))

        hdfs = HDFSHook(hdfs_conn_id=self.hdfs_conn_id)
        clickhouse = ClickhouseHook(clickhouse_conn_id=self.clickhouse_conn_id)

        self.log.info("Reading from hdfs...")
        try:
            hdfs_client = hdfs.get_conn()
            paths = hdfs_client.cat(self.hdfs_paths)
        except HDFSHookException as e:
            raise AirflowException("Error reading from paths", e)

        self.log.info("Loading to clickhouse...")
        try:
            r = clickhouse.insert_rows(self.clickhouse_table, data=paths, format=self.format,
                                       extra_options=self.extra_options)
        except ClickhouseException as e:
            raise AirflowException("Error inserting to clickhouse", e)
        # todo count inserted rows
        self.log.info("Done")

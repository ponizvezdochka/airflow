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

import requests
from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth, DISABLED

from airflow.configuration import conf
from airflow.providers.http.hooks.http import HttpHook
from airflow.security import utils


class ClickhouseException(Exception):
    """
    Clickhouse exception
    """


class ClickhouseHook(HttpHook):
    """
    Interact with Clickhouse.
    """

    conn_name_attr = 'clickhouse_conn_id'
    default_conn_name = 'clickhouse_default'

    def __init__(self,
                 clickhouse_conn_id='clickhouse_default',
                 *args,
                 **kwargs
                 ):
        super(ClickhouseHook).__init__(method='POST', http_conn_id=clickhouse_conn_id, *args, **kwargs)

    def get_conn(self, headers=None):
        """
        Returns http session for use with requests

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        session = requests.Session()

        if self.clickhouse_conn_id:
            conn = self.get_connection(self.clickhouse_conn_id)

            if conn.extra_dejson.get('kerberos'):
                session.auth = HTTPKerberosAuth(DISABLED)
            elif conn.password:
                session.auth = HTTPBasicAuth(conn.login, conn.password)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.extra:
                try:
                    session.headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning('Connection to %s has invalid extra field.', conn.host)
        if headers:
            session.headers.update(headers)

        return session

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def run(self, sql, extra_options=None, **request_kwargs):
        """
        Execute the statement against Clickhouse. Can be used to set parameters.

        :param sql: the sql code to be executed. (templated)
        :type sql: Can receive a str representing a sql statement,
            a list of str (sql statements), or reference to a template file.
            Template reference are recognized by str ending in '.sql'
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :type extra_options: dict
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        self.log.info("Starting query: %s", sql)
        data = self._strip_sql(sql),
        return super(ClickhouseHook).run(endpoint=None, data=data, extra_options=extra_options, **request_kwargs)

    def get_records(self, sql, format=None, extra_options=None, **request_kwargs):
        """
        Get a set of records from Clickhouse

        :param sql: the sql code to be executed. (templated)
        :type sql: Can receive a str representing a sql statement,
            a list of str (sql statements), or reference to a template file.
            Template reference are recognized by str ending in '.sql'
        :param format: Input format
        :type format: str
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :type extra_options: dict
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        query = self._strip_sql(sql)
        if format:
            query = "%s FORMAT %s" % (query, format)
        self.log.info("Starting query: %s", query)
        return super(ClickhouseHook).run(endpoint=None, data=query, extra_options=extra_options, **request_kwargs)

    def insert_rows(self, table, data=None, format=None, extra_options=None, **request_kwargs):
        """
        A generic way to insert rows from input stream into a table.

        :param table: Name of the target table
        :type table: str
        :param data: The rows to insert into the table
        :type data: iterable of str
        :param format: Input format
        :type format: str
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :type extra_options: dict
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        query = "INSERT INTO %s " % table
        if format:
            query += 'FORMAT %s ' % format
        self.log.info("Starting insert: %s", query)
        if data:
            return super(ClickhouseHook).run(endpoint=None, data=data, extra_options=extra_options, params={'query': query}, **request_kwargs)
        else:  # insert values
            return super(ClickhouseHook).run(endpoint=None, data=query, extra_options=extra_options, **request_kwargs)

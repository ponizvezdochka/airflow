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
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from requests.auth import HTTPBasicAuth
from requests_kerberos import HTTPKerberosAuth, DISABLED


class ClickhouseException(Exception):
    """
    Clickhouse exception
    """


class ClickhouseFormat:
    """
    Helper class with supported formats.
    """
    TABSEPARATED = "TabSeparated"
    TABSEPARATEDRAW = "TabSeparatedRaw"
    TABSEPARATEDWITHNAMES = "TabSeparatedWithNames"
    TABSEPARATEDWITHNAMESANDTYPES = "TabSeparatedWithNamesAndTypes"
    TEMPLATE = "Template"
    TEMPLATEIGNORESPACES = "TemplateIgnoreSpaces"
    CSV = "CSV"
    CSVWITHNAMES = "CSVWithNames"
    CUSTOMSEPARATED = "CustomSeparated"
    VALUES = "Values"
    VERTICAL = "Vertical"
    JSON = "JSON"
    JSONCOMPACT = "JSONCompact"
    JSONEACHROW = "JSONEachRow"
    TSKV = "TSKV"
    PRETTY = "Pretty"
    PRETTYCOMPACT = "PrettyCompact"
    PRETTYCOMPACTMONOBLOCK = "PrettyCompactMonoBlock"
    PRETTYNOESCAPES = "PrettyNoEscapes"
    PRETTYSPACE = "PrettySpace"
    PROTOBUF = "Protobuf"
    PARQUET = "Parquet"
    ORC = "ORC"
    ROWBINARY = "RowBinary"
    ROWBINARYWITHNAMESANDTYPES = "RowBinaryWithNamesAndTypes"
    NATIVE = "Native"
    NULL = "Null"
    XML = "XML"
    CAPNPROTO = "CapnProto"


class ClickhouseHook(BaseHook):
    """
    Interact with Clickhouse.
    """

    def __init__(self, clickhouse_conn_id='clickhouse_default', auth=None):
        self.clickhouse_conn_id = clickhouse_conn_id
        self.base_url = None
        self.auth = auth
        self.get_conn()

    def get_conn(self):
        """
        Sets authentification object for use with requests
        """

        if self.clickhouse_conn_id:
            conn = self.get_connection(self.clickhouse_conn_id)

            if self.auth:
                pass
            elif conn.extra_dejson.get('kerberos'):
                self.auth = HTTPKerberosAuth(DISABLED, force_preemptive=True)
            elif conn.password:
                self.auth = HTTPBasicAuth(conn.login, conn.password)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)

    @staticmethod
    def _strip_sql(sql):
        return sql.strip().rstrip(';')

    def _concat_url_w_endpoint(self, endpoint=None):
        if self.base_url and not self.base_url.endswith('/') and \
           endpoint and not endpoint.startswith('/'):
            url = self.base_url + '/' + endpoint
        else:
            url = (self.base_url or '') + (endpoint or '')
        return url

    def _check_response(self, response):
        """
        Checks the status code and raise an AirflowException exception on non 2XX or 3XX
        status codes

        :param response: A requests response object
        :type response: requests.response
        """
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)

    def run(self, sql, endpoint=None, row_format=None, data=None, **request_kwargs):
        """
        Execute sql statement against Clickhouse.

        :param sql: the sql code to be executed. (templated)
        :type sql: str
        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param row_format: Input format
        :type row_format: str
        :param data: data to pass through request body
        :type data: dict or str or iterable
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        url = self._concat_url_w_endpoint(endpoint)
        params = request_kwargs.pop('params', {})
        query = self._strip_sql(sql)
        if row_format:
            query += ' FORMAT %s' % row_format
        params['query'] = query

        self.log.info("Sending query '%s' to url: %s", query, url)
        try:
            response = requests.post(
                url,
                data=data,
                params=params,
                stream=request_kwargs.pop("stream", False),
                verify=request_kwargs.pop("verify", False),
                proxies=request_kwargs.pop("proxies", {}),
                cert=request_kwargs.pop("cert", None),
                timeout=request_kwargs.pop("timeout", 10),
                allow_redirects=request_kwargs.pop("allow_redirects", True),
                auth=self.auth,
                **request_kwargs)

            self._check_response(response)
            return response

        except requests.exceptions.ConnectionError as ex:
            self.log.warning('%s Error quering clickhouse', ex)
            raise AirflowException(ex)

    def insert_rows(self, sql=None, table=None, endpoint=None, data=None, row_format=None, **request_kwargs):
        """
        A generic way to insert rows from input stream or values from query into a table.

        :param sql: the sql code with values to insert. (templated)
        :type sql: str
        :param table: Name of the target table
        :type table: str
        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param data: The rows to insert into the table
        :type data: iterable of str
        :param row_format: Input format
        :type row_format: str
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        if sql:
            query = self._strip_sql(sql)
        elif table:
            query = "INSERT INTO %s" % table
        else:
            self.log.error("Neither sql nor table name is specified.")
            raise AirflowException("Insert failed.")

        self.log.info("Insert query: %s%s" % (query, ' FORMAT %s' % row_format or ''))
        return self.run(sql=query, endpoint=endpoint, row_format=row_format, data=data, **request_kwargs)

    def get_records(self, sql, endpoint=None, row_format=None, **request_kwargs):
        """
        A generic way to get records from Clickhouse.

        :param sql: the sql code to retrieve records. (templated)
        :type sql: str
        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :type endpoint: str
        :param row_format: Input format
        :type row_format: str
        :param  \**request_kwargs: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``requests.Request(json=obj)``
        """
        self.log.info("Select query: %s", sql)
        return self.run(sql=sql, endpoint=endpoint, row_format=row_format, **request_kwargs)

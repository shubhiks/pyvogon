import itertools
import json
import time
import requests

from vogon import exceptions

QUERY_SUBMIT = '/api/spark/sql'
QUERY_PROGRESS = '/api/spark/progress'
QUERY_RESULT = '/api/spark/result/v2'
QUERY_CANCEL = '/api/spark/cancel'
SCHEDULER_RUNS = '/api/scheduler/newruns'


class Type(object):
    STRING = 1
    NUMBER = 2
    BOOLEAN = 3


def connect(
    host="vogon.reports.mn",
    port=9090,
    path="/api/spark/sql/",
    scheme="https",
    user=None,
    password=None,
    context=None,
    header=False,
    ssl_verify_cert=True,
    ssl_client_cert=None,
    proxies=None,
):  # noqa: E125
    """
    Constructor for creating a connection to the database.

        >>> conn = connect('localhost', 8082)
        >>> curs = conn.cursor()

    """
    context = context or {}

    return Connection(
        host,
        port,
        path,
        scheme,
        user,
        password,
        context,
        header,
        ssl_verify_cert,
        ssl_client_cert,
        proxies,
    )


def check_closed(f):
    """Decorator that checks if connection/cursor is closed."""

    def g(self, *args, **kwargs):
        if self.closed:
            raise exceptions.Error(
                "{klass} already closed".format(klass=self.__class__.__name__)
            )
        return f(self, *args, **kwargs)

    return g


def check_result(f):
    """Decorator that checks if the cursor has results from `execute`."""

    def g(self, *args, **kwargs):

        if self._results is None:
            raise exceptions.Error("Called before `execute`")
        return f(self, *args, **kwargs)

    return g


def get_description_from_row(row):
    """
    Return description from a single row.

    We only return the name, type (inferred from the data) and if the values
    can be NULL. String columns in Druid are NULLable. Numeric columns are NOT
    NULL.
    """
    return [
        (
            name,  # name
            get_type(value),  # type_code
            None,  # [display_size]
            None,  # [internal_size]
            None,  # [precision]
            None,  # [scale]
            get_type(value) == Type.STRING,  # [null_ok]
        )
        for name, value in row.items()
    ]


def get_type(value):
    """
    Infer type from value.

    Note that bool is a subclass of int so order of statements matter.
    """

    if isinstance(value, str) or value is None:
        return Type.STRING
    elif isinstance(value, bool):
        return Type.BOOLEAN
    elif isinstance(value, (int, float)):
        return Type.NUMBER

    raise exceptions.Error("Value of unknown type: {value}".format(value=value))


class Connection(object):
    """Connection to a Druid database."""

    def __init__(
        self,
        host="vogon.reports.mn",
        port=9090,
        path="/api/spark/sql/",
        scheme="https",
        user=None,
        password=None,
        context=None,
        header=False,
        ssl_verify_cert=True,
        ssl_client_cert=None,
        proxies=None,
    ):
        netloc = "{host}:{port}".format(host=host, port=port)
        self.url = "https://vogon.reports.mn"
        self.context = context or {}
        self.closed = False
        self.cursors = []
        self.header = header
        self.user = user
        self.password = password
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_client_cert = ssl_client_cert
        self.proxies = proxies

    @check_closed
    def close(self):
        """Close the connection now."""
        self.closed = True
        for cursor in self.cursors:
            try:
                cursor.close()
            except exceptions.Error:
                pass  # already closed

    @check_closed
    def commit(self):
        """
        Commit any pending transaction to the database.

        Not supported.
        """
        pass

    @check_closed
    def cursor(self):
        """Return a new Cursor Object using the connection."""

        cursor = Cursor(
            self.url,
            self.user,
            self.password,
            self.context,
            self.header,
            self.ssl_verify_cert,
            self.ssl_client_cert,
            self.proxies,
        )

        self.cursors.append(cursor)

        return cursor

    @check_closed
    def execute(self, operation, parameters=None):
        cursor = self.cursor()
        return cursor.execute(operation, parameters)

    def __enter__(self):
        return self.cursor()

    def __exit__(self, *exc):
        self.close()


class Cursor(object):
    """Connection cursor."""

    def __init__(
        self,
        url,
        user=None,
        password=None,
        context=None,
        header=False,
        ssl_verify_cert=True,
        proxies=None,
        ssl_client_cert=None,
    ):
        self.url = url
        self.context = context or {}
        self.header = header
        self.user = user
        self.password = password
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_client_cert = ssl_client_cert
        self.proxies = proxies

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description = None

        # this is set to an iterator after a successful query
        self._results = None

    @property
    @check_result
    @check_closed
    def rowcount(self):
        # consume the iterator
        results = list(self._results)
        n = len(results)
        self._results = iter(results)
        return n

    @check_closed
    def close(self):
        """Close the cursor."""
        self.closed = True

    @check_closed
    def execute(self, operation, parameters=None):
        query = apply_parameters(operation, parameters)
        query = replace_quotes(query)
        rows = self._stream_query(query)
        self._results = iter(rows)
        return self

    @check_closed
    def executemany(self, operation, seq_of_parameters=None):
        raise exceptions.NotSupportedError(
            "`executemany` is not supported, use `execute` instead"
        )

    @check_result
    @check_closed
    def fetchone(self):
        """
        Fetch the next row of a query result set, returning a single sequence,
        or `None` when no more data is available.
        """
        try:
            return next(self._results)
        except StopIteration:
            return None

    @check_result
    @check_closed
    def fetchmany(self, size=None):
        """
        Fetch the next set of rows of a query result, returning a sequence of
        sequences (e.g. a list of tuples). An empty sequence is returned when
        no more rows are available.
        """
        size = size or self.arraysize
        return list(itertools.islice(self._results, size))

    @check_result
    @check_closed
    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result, returning them as a
        sequence of sequences (e.g. a list of tuples). Note that the cursor's
        arraysize attribute can affect the performance of this operation.
        """
        return list(self._results)

    @check_closed
    def setinputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def setoutputsizes(self, sizes):
        # not supported
        pass

    @check_closed
    def __iter__(self):
        return self

    @check_closed
    def __next__(self):
        return next(self._results)

    next = __next__

    def _stream_query(self, query):

        self.description = None

        session = requests.Session()
        headers = {
            "Content-Type": "application/json",
            "vogon-auth-token": "3037562c-78f7-4aa4-ae37-640c43647c03"
        }
        session.headers.update(headers)
        response = execute_sync(session, query)

        rows = response["rows"]
        lis = list()

        for row in rows:
            lis.append(tuple(row))
        return lis


def __get_response_value(response: requests.Response, key: str):
    try:
        res_json = response.json()
        return res_json[key]
    except ValueError as ve:
        raise ve


def __get_response(response: requests.Response):
    try:
        res_json = response.json()
        return res_json
    except ValueError as ve:
        raise ve


def __run_and_check(session, request: requests.Request) -> requests.Response:
    prepped_request = session.prepare_request(request)
    try:
        response = session.send(prepped_request, verify=False, allow_redirects=True)
        __check_response_status(response)
        return response
    except requests.exceptions.ConnectionError as ex:
        raise ex


def __check_response_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        raise ValueError(str(response.status_code) + ":" + response.reason)


def submit(session, query):

    data = {'sql': query}
    req = requests.Request('POST', 'https://vogon.reports.mn/api/spark/sql', data=json.dumps(data))
    res = __run_and_check(session, req)
    if __get_response_value(res, 'status') == 'SUBMITTED':
        return __get_response_value(res, 'id')
    raise ValueError(f'query is not submitted successfully, check with Vogon Admin')


def progress(session,  job_id: str):
    req = requests.Request('GET', f'https://vogon.reports.mn/api/spark/progress?id={job_id}')
    res = __run_and_check(session, req)
    return str(__get_response_value(res, 'status'))


def error_message(session, job_id: str):
    req = requests.Request('GET', f'https://vogon.reports.mn/api/spark/progress?id={job_id}')
    res = __run_and_check(session, req)
    return __get_response_value(res, 'error')


def results(session, job_id: str, start: int, total: int):
    req = requests.Request('GET', f'https://vogon.reports.mn/api/spark/result/v2?id={job_id}&start={str(start)}&total={str(total)}')
    res = __run_and_check(session, req)
    return __get_response(res)


def cancel(session, job_id: str):
    req = requests.Request("GET", f'https://vogon.reports.mn/api/spark/cancel?id={job_id}')
    __run_and_check(session, req)
    return True


def apply_parameters(operation, parameters):
    if not parameters:
        return operation

    escaped_parameters = {key: escape(value) for key, value in parameters.items()}
    return operation % escaped_parameters


def replace_quotes(query):
    str_query = str(query)
    return str_query.replace('"', '`')


def execute_sync(session,
                 query: str,
                 poll_interval_sec: int = 60,
                 query_timeout_min: int = 1440,
                 total_result_rows: int = 100,
                 raise_error: bool = True):
    if query_timeout_min < 2:
        raise ValueError("query_timeout_min for vogon query cannot be less than 2 minutes")
    if poll_interval_sec < 45:
        raise ValueError("poll_interval_sec for vogon query cannot be less than 45 seconds")

    job_id = submit(session, query)
    timeout = query_timeout_min
    to_print_status = 1
    current_status = 'SUBMITTED'
    while current_status in ['RUNNING', 'QUEUED', 'UNKNOWN', 'SUBMITTED']:
        time.sleep(poll_interval_sec)
        try:
            current_status = progress(session, job_id)
        except Exception as e:
            print(str(e))
        if to_print_status == 1:
            print(f'id - {job_id}, status - {current_status}')
        to_print_status = to_print_status ^ 1
        timeout = timeout - poll_interval_sec
        if timeout < 0 and current_status in ['RUNNING', 'QUEUED', 'UNKNOWN', 'SUBMITTED']:
            print(f'job running time has exceeded timeout limit ({query_timeout_min} mins), cancelling')
            cancel(session, job_id)
            current_status = 'TIMEOUT'
            break
    if current_status == 'SUCCEEDED':
        return results(session, job_id, 0, total_result_rows)
    print(f'vogon job failed to succeed, status - {current_status}')
    try:
        err = error_message(session, job_id)
        print(f'vogon error message - {err}')
    except Exception as e:
        print("couldn't fetch vogon error message", e)
    if raise_error:
        raise ValueError('vogon job failed to succeed')
    return None


def escape(value):
    """
    Escape the parameter value.

    Note that bool is a subclass of int so order of statements matter.
    """
    if value == "*":
        return value
    elif isinstance(value, str):
        return "'{}'".format(value.replace("'", "``"))
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (list, tuple)):
        return ", ".join(escape(element) for element in value)


paramstyle = "pyformat"


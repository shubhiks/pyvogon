from sqlalchemy import types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

import pyvogon.base

RESERVED_SCHEMAS = []

type_map = {
    "char": types.String,
    "varchar": types.String,
    "string": types.String,
    "float": types.Float,
    "decimal": types.Float,
    "real": types.Float,
    "double": types.Float,
    "boolean": types.Boolean,
    "tinyint": types.BigInteger,
    "smallint": types.BigInteger,
    "integer": types.BigInteger,
    "bigint": types.BigInteger,
    "timestamp": types.TIMESTAMP,
    "date": types.DATE,
    "other": types.BLOB,
}


class UniversalSet(object):
    def __contains__(self, item):
        return True


class VogonIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class VogonCompiler(compiler.SQLCompiler):
    pass


class VogonTypeCompiler(compiler.GenericTypeCompiler):
    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        return "LONG"

    def visit_TIME(self, type_, **kwargs):
        return "LONG"

    def visit_BLOB(self, type_, **kwargs):
        return "COMPLEX"

    visit_CLOB = visit_BLOB
    visit_NCLOB = visit_BLOB
    visit_VARBINARY = visit_BLOB
    visit_BINARY = visit_BLOB


class VogonDialect(default.DefaultDialect):

    name = "vogon"
    scheme = "https"
    driver = "rest"
    user = None
    password = None
    preparer = VogonIdentifierPreparer
    statement_compiler = VogonCompiler
    type_compiler = VogonTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(self, context=None, *args, **kwargs):
        super(VogonDialect, self).__init__(*args, **kwargs)
        self.context = context or {}

    @classmethod
    def dbapi(cls):
        return pyvogon.base

    def do_rollback(self, dbapi_connection):
        pass

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 9090,
            "user": url.username or None,
            "password": url.password or None,
            "path": url.database,
            "scheme": self.scheme,
            "context": self.context,
            "header": url.query.get("header") == "true",
        }
        return ([], kwargs)

    def get_schema_names(self, connection, **kwargs):
        return ["cm.rts_customer_stats", "cm.rts_portfolio_stats"]

    def get_table_names(self, connection, schema=None, **kwargs):
        return ["cm.rts_customer_stats", "cm.rts_portfolio_stats"]

    def get_view_names(self, connection, schema=None, **kwargs):
        return []

    def get_columns(self, connection, table_name, schema=None, **kwargs):

        return [
            {
                "name": "customer_id",
                "type": "string",
                "nullable": True,
                "default": "-1",
            },
            {
                "name": "net_bid",
                "type": "double",
                "nullable": True,
                "default": 0,
            }

        ]


dialect = VogonDialect

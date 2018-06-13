from airflow.hooks.dbapi_hook import DbApiHook
from pydruid.client import PyDruid
from pydruid.db import connect


class DruidBrokerHook(DbApiHook):
    """
    Interact with a Druid broker.

    This hook is purely for users to query druid broker.
    For ingestion, please use druidHook.

    This hook is a variation on airflow.hooks.druidhook.DruidDbApiHook.
    It offers a Connection as well as a PyDruid client to interact with brokers
    """
    conn_name_attr = 'druid_broker_conn_id'
    default_conn_name = 'druid_broker_default'
    supports_autocommit = False

    def __init__(self, *args, **kwargs):
        super(DruidBrokerHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        """
        Establish a connection to druid broker.
        """
        conn = self.get_connection(self.druid_broker_conn_id)
        druid_broker_conn = connect(
            host=conn.host,
            port=conn.port,
            path=conn.extra_dejson.get('endpoint', '/druid/v2/sql'),
            scheme=conn.extra_dejson.get('schema', 'http')
        )
        self.log.info('Get the connection to druid '
                      'broker on {host}'.format(host=conn.host))
        return druid_broker_conn

    def get_uri(self):
        """
        Get the connection uri for druid broker.

        e.g: druid://localhost:8082/druid/v2/sql/
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        conn_type = 'druid' if not conn.conn_type else conn.conn_type
        endpoint = conn.extra_dejson.get('endpoint', 'druid/v2/sql')
        return '{conn_type}://{host}/{endpoint}'.format(
            conn_type=conn_type, host=host, endpoint=endpoint)

    def get_client(self):
        conn = self.get_connection(self.druid_broker_conn_id)
        druid_client = PyDruid(
            url="{conn.schema}://{conn.host}".format(conn=conn),
            endpoint=conn.extra_dejson.get("endpoint", "druid/v2/"),
        )
        druid_client.set_basic_auth_credentials(
            username=conn.login,
            password=conn.password,
        )
        return druid_client

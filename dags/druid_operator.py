from airflow.models import BaseOperator
from druid_broker_hook import DruidBrokerHook
from pydruid.utils.aggregators import longsum


class DruidStatsOperator(BaseOperator):
    """
    Get a days worth of data and store it in tmp as csv
    :param druid_broker_conn_id: The connection id of the Druid broker to query
    :type druid_broker_conn_id: str
    """
    template_fields = ('intervals',)

    def __init__(
            self,
            druid_broker_conn_id='druid_broker_default',
            intervals=None,
            *args, **kwargs):
        super(DruidStatsOperator, self).__init__(*args, **kwargs)
        self.conn_id = druid_broker_conn_id
        self.intervals = intervals

    def execute(self, context):
        client = DruidBrokerHook(druid_broker_conn_id=self.conn_id).get_client()
        self.log.info("Getting raw data from Druid")
        stats = client.groupby(
            datasource='Bids',
            dimensions=['BackendName'],
            granularity='hour',
            intervals=self.intervals,
            aggregations={
                'bids': longsum('Bids'),
                'wins': longsum('Wins'),
            }
        ).export_pandas().rename(columns={
            'timestamp': 'hour',
            'BackendName': 'backend_name',
        })
        stats['hour'] = stats.hour.apply(lambda x: x[11:13])
        self.log.info("Storing raw data from Druid")
        stats.to_csv(
            "/tmp/winrate_stats/{}/stats.csv".format(self.intervals.split('/')[0]),
            index=False
        )

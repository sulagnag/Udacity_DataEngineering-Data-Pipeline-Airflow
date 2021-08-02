from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tables=kwargs['params']['tables']
        self.cols=kwargs['params']['cols']

    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table,col in zip(self.tables, self.cols):
            self.log.info('Checks for {} table'.format(table))
            sql="""select count(*) from {}""".format(table)
            result=redshift.get_records(sql)
            if result[0][0] == 0:
                raise ValueError("{} table empty".format(table))
            else :
                self.log.info("{} table has {} records".format(table,result[0][0]))
                
            sql ="""select count(*) from {} where {} is NULL""".format(table,col)
            result=redshift.get_records(sql)
            if result[0][0] > 0:
                raise ValueError("{} table has {} NULL values for {} column".format(table, result[0][0], col))
            else :
                self.log.info("{} Table has no records with null values for the {} column".format(table,col))
                
        
        
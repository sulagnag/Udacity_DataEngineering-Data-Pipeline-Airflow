from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_operation="",
                 append="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_operation=sql_operation
        self.append=append

    def execute(self, context):
        self.log.info('Loading Dimension table {}'.format(self.table))
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append :
            sql_stat_final= """INSERT INTO public.{} 
            {}""".format(self.table,self.sql_operation)
        else:
            sql_stat_final="""TRUNCATE TABLE public.{};
            INSERT INTO public.{} 
            {}""".format(self.table, self.table, self.sql_operation)
        
        redshift.run(sql_stat_final)    
                      

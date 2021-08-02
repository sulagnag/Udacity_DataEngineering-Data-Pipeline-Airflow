from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql_oper="",                 
                append="True",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_oper=sql_oper
        self.append=append
        
    def execute(self, context):
        if self.append == "True" :
            sql_st="""INSERT INTO songplays (playid,start_time,userid, level,songid,artistid,sessionid,location,user_agent) {}""".format(self.sql_oper)
        else :
            sql_st="""TRUNCATE TABLE public.songplays; 
            INSERT INTO public.songplays 
            (playid,start_time,userid, level,songid,artistid,sessionid,location,user_agent) {}
            """.format(self.sql_oper)
        self.log.info('Loading songplays table {}')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(sql_st)
        redshift.run(sql_st)

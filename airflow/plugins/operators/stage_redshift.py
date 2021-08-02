from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields=("s3_key","year", "month","date_ds")
    
    copy_command= """COPY public.{}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}' 
    COMPUPDATE OFF
    REGION '{}'
    TIMEFORMAT 'epochmillisecs'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_key="",
                 s3_bucket="",
                 table="",
                 json_option="",
                 region="us-west-2",
                 timestamped=False,
                 year="",
                 month="",
                 date_ds="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_key=s3_key
        self.s3_bucket=s3_bucket
        self.table=table
        self.json_option=json_option
        self.region=region
        self.timestamped=timestamped
        self.year=year
        self.month=month
        self.date_ds=date_ds
        
        

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        sql_st= "TRUNCATE TABLE public.{}".format(self.table)
        redshift.run(sql_st)
     
        self.log.info("copy data from s3 to staging files")
        rendered_key = self.s3_key.format(**context)
        
        if self.timestamped :
            s3_path = "s3://{}/{}{}/{}/{}-events.json".format(self.s3_bucket, rendered_key, self.year,self.month,self.date_ds)
            
        else:    
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        sql_st=StageToRedshiftOperator.copy_command.format(
            self.table, 
            s3_path, 
            credentials.access_key,
            credentials.secret_key,
            self.json_option,
            self.region)
        redshift.run(sql_st) 

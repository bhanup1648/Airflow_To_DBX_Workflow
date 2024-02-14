from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator


from datetime import datetime
import requests
import pandas as pd
import os
import ast

# Define the API endpoint URL
api_url = "http://api.tvmaze.com/search/shows?q=postman"
base_path = '/opt/airflow/output/'


# Define the default_args dictionary to set the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'televison_airflow_dag',
    default_args=default_args,
    description='An example Airflow DAG',
    schedule_interval='@daily',  # You can adjust the schedule_interval as needed
)

# Define the function to be executed by the PythonOperator
def call_api():
    # Make an API request
    response = requests.get(api_url)
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON data from the response
        api_data = response.json()
        # Convert the data to a DataFrame
        df = pd.json_normalize(api_data, sep='_')
        df = df.astype(str)
        # Now, you can work with the DataFrame as needed
        print('df count', df.count)
        # csv_file_path = os.path.join(base_path, 'telivision_new_csv.csv')
        print('Writing df to file system and catalogue table')
        # df.to_csv(csv_file_path, index=False)
        # print('csv file saved to location',)
    else:
        print("API request failed with status code:", response.status_code)


def load_data():
    base_path = '/opt/airflow/output/'
    # Get a list of all files in the directory
    files = os.listdir(base_path)
    csv_files = [file for file in files if file.endswith('.csv')]
    print(csv_files)
    all_data = pd.DataFrame()
    # Iterate through each CSV file and read it into a DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(base_path, csv_file)
        
        # Read the CSV file into a DataFrame using pandas
        df = pd.read_csv(file_path)
        
        # Concatenate the data to the existing DataFrame
        all_data = pd.concat([all_data, df], ignore_index=True)

        # Now you can work with the concatenated DataFrame 'all_data' as needed
        print("Contents of all CSV files:")
        print("Before Explode")
        df['show_genres'] = df['show_genres'].apply(ast.literal_eval)
        all_data  = all_data.explode("show_genres")

        # all_data["show_genres"] = all_data["show_genres"].str.strip()
        # Convert string representations to lists
        all_data['show_schedule_days'] = all_data['show_schedule_days'].apply(ast.literal_eval)
        all_data  = all_data.explode("show_schedule_days")
        
        # all_data["show_schedule_days"] = all_data["show_schedule_days"].str.strip()
        print("After Explode")
        print(all_data['show_schedule_days'])
        print(pd.__version__)
        print(all_data.dtypes)
        all_data.to_csv(os.path.join(base_path,'transeformed_files/telivision_transeformed.csv'), index=False)
        print("Transeformedfiles Moved into Directory")
        # Define the PythonOperator for API call task
api_operator = PythonOperator(
    task_id='call_api_task',
    python_callable=call_api,
    dag=dag,
)

load_data_dag = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)


# copy_task = PostgresOperator(
#     task_id='copy_from_csv_to_postgres',
#     postgres_conn_id='postgres_localhost',  # Specify your PostgreSQL connection ID
#     sql=f'''
#         copy tv_show FROM '/opt/airflow/output/transeformed_files/telivision_trn_csv.csv'  WITH CSV HEADER DELIMITER ',';

#     ''',
#     dag=dag
# )

stg_trt_load = PostgresOperator(
        task_id='stg_trt_load',
        postgres_conn_id='postgres_localhost',
        sql="""

        UPDATE tv_show
        SET
            score =stg.score, 
            show_id =stg.show_id,
            show_url =stg.show_url,
            show_name =stg.show_name,
            show_type =stg.show_type,
            show_language =stg.show_language,
            show_genres =stg.show_genres,
            show_status =stg.show_status,
            show_runtime =stg.show_runtime,
            show_average_runtime =stg.show_average_runtime,
            show_premiered =stg.show_premiered,
            show_ended =stg.show_ended,
            show_official_site =stg.show_official_site,
            show_schedule_time =stg.show_schedule_time,
            show_schedule_days =stg.show_schedule_days,
            show_rating_average =stg.show_rating_average,
            show_weight =stg.show_weight,
            show_network_id =stg.show_network_id,
            show_network_name =stg.show_network_name,
            show_network_country_name =stg.show_network_country_name,
            show_network_country_code =stg.show_network_country_code,
            show_network_country_timezone =stg.show_network_country_timezone,
            show_network_official_site =stg.show_network_official_site,
            show_web_channel =stg.show_web_channel,
            show_dvd_country =stg.show_dvd_country,
            show_externals_tvrage =stg.show_externals_tvrage,
            show_externals_thetvdb =stg.show_externals_thetvdb,
            show_externals_imdb =stg.show_externals_imdb,
            show_image_medium =stg.show_image_medium,
            show_image_original =stg.show_image_original,
            show_summary =stg.show_summary,
            show_updated =stg.show_updated,
            show_links_self_href =stg.show_links_self_href,
            show_links_prev_episode_href =stg.show_links_prev_episode_href,
            show_links_next_episode_href =stg.show_links_next_episode_href,
            show_network_id_fk =stg.show_network_id_fk,
            show_web_channel_id_fk =stg.show_web_channel_id_fk,
            show_web_channel_name =stg.show_web_channel_name,
            show_web_channel_country_name =stg.show_web_channel_country_name,
            show_web_channel_country_code =stg.show_web_channel_country_code,
            show_web_channel_country_timezone =stg.show_web_channel_country_timezone,
            show_web_channel_official_site =stg.show_web_channel_official_site,
            show_averageruntime =stg.show_averageruntime,
            show_officialsite =stg.show_officialsite,
            show_network_officialsite =stg.show_network_officialsite,
            show_webchannel =stg.show_webchannel,
            show_dvdcountry =stg.show_dvdcountry,
            show__links_self_href =stg.show__links_self_href,
            show__links_previousepisode_href =stg.show__links_previousepisode_href,
            show__links_nextepisode_href =stg.show__links_nextepisode_href,
            show_network =stg.show_network,
            show_webchannel_id =stg.show_webchannel_id,
            show_webchannel_name =stg.show_webchannel_name,
            show_webchannel_country_name =stg.show_webchannel_country_name,
            show_webchannel_country_code =stg.show_webchannel_country_code,
            show_webchannel_country_timezone =stg.show_webchannel_country_timezone,
            show_webchannel_officialsite =stg.show_webchannel_officialsite

        FROM tv_show_stg stg
        join tv_show md on
        (
            md.score = stg.score
            AND md.show_id = stg.show_id
            AND md.show_genres = stg.show_genres
            AND md.show_schedule_days = stg.show_schedule_days
            

        ); 

        insert into tv_show
            (
            score ,
            show_id ,
            show_url ,
            show_name ,
            show_type ,
            show_language ,
            show_genres ,
            show_status ,
            show_runtime ,
            show_average_runtime ,
            show_premiered ,
            show_ended ,
            show_official_site ,
            show_schedule_time ,
            show_schedule_days ,
            show_rating_average ,
            show_weight ,
            show_network_id ,
            show_network_name ,
            show_network_country_name ,
            show_network_country_code ,
            show_network_country_timezone ,
            show_network_official_site ,
            show_web_channel ,
            show_dvd_country ,
            show_externals_tvrage ,
            show_externals_thetvdb ,
            show_externals_imdb ,
            show_image_medium ,
            show_image_original ,
            show_summary ,
            show_updated ,
            show_links_self_href ,
            show_links_prev_episode_href ,
            show_links_next_episode_href ,
            show_network_id_fk ,
            show_web_channel_id_fk ,
            show_web_channel_name ,
            show_web_channel_country_name ,
            show_web_channel_country_code ,
            show_web_channel_country_timezone ,
            show_web_channel_official_site ,
            show_averageruntime ,
            show_officialsite ,
            show_network_officialsite ,
            show_webchannel ,
            show_dvdcountry ,
            show__links_self_href ,
            show__links_previousepisode_href ,
            show__links_nextepisode_href ,
            show_network ,
            show_webchannel_id ,
            show_webchannel_name ,
            show_webchannel_country_name ,
            show_webchannel_country_code ,
            show_webchannel_country_timezone ,
            show_webchannel_officialsite
        )
        select
            score ,
            show_id ,
            show_url ,
            show_name ,
            show_type ,
            show_language ,
            show_genres ,
            show_status ,
            show_runtime ,
            show_average_runtime ,
            show_premiered ,
            show_ended ,
            show_official_site ,
            show_schedule_time ,
            show_schedule_days ,
            show_rating_average ,
            show_weight ,
            show_network_id ,
            show_network_name ,
            show_network_country_name ,
            show_network_country_code ,
            show_network_country_timezone ,
            show_network_official_site ,
            show_web_channel ,
            show_dvd_country ,
            show_externals_tvrage ,
            show_externals_thetvdb ,
            show_externals_imdb ,
            show_image_medium ,
            show_image_original ,
            show_summary ,
            show_updated ,
            show_links_self_href ,
            show_links_prev_episode_href ,
            show_links_next_episode_href ,
            show_network_id_fk ,
            show_web_channel_id_fk ,
            show_web_channel_name ,
            show_web_channel_country_name ,
            show_web_channel_country_code ,
            show_web_channel_country_timezone ,
            show_web_channel_official_site ,
            show_averageruntime ,
            show_officialsite ,
            show_network_officialsite ,
            show_webchannel ,
            show_dvdcountry ,
            show__links_self_href ,
            show__links_previousepisode_href ,
            show__links_nextepisode_href ,
            show_network ,
            show_webchannel_id ,
            show_webchannel_name ,
            show_webchannel_country_name ,
            show_webchannel_country_code ,
            show_webchannel_country_timezone ,
            show_webchannel_officialsite
        FROM  (
        SELECT *
                ,row_number() OVER (PARTITION BY score,
                show_id,show_genres,show_schedule_days ORDER BY score DESC) as RN
        FROM  tv_show_stg
        ) stg
        where RN = 1 
        AND  NOT EXISTS (
            SELECT 1
            FROM  tv_show tgt
            WHERE  tgt.score = stg.score
                    AND tgt.show_id = stg.show_id
                    AND tgt.show_genres = stg.show_genres
                    AND tgt.show_schedule_days = stg.show_schedule_days
                    ); 

    """,
        dag=dag
    )


# Define start and end dummy tasks
start_dummy = DummyOperator(
    task_id='start_dummy',
    dag=dag,
)

end_dummy = DummyOperator(
    task_id='end_dummy',
    dag=dag,
)

# Set task dependencies
start_dummy >> api_operator  >> load_data_dag >> stg_trt_load >> end_dummy



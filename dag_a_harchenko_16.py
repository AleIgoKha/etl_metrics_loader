import pandahouse as ph
import pandas as pd

from datetime import timedelta, datetime
from airflow.decorators import dag, task

default_args = {
    'owner': 'a.harchenko-16',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 2, 19),
}

schedule_interval = '0 0 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def daily_actions_gender_age_os():
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def get_feed_actions_df(connection):
    
        # query to receive likes, views and data for different slices
        query_feed_actions = """
        SELECT toDate(time) AS event_date,
                user_id,
                gender,
                age,
                os,
                SUM(action = 'view') AS views,
                SUM(action = 'like') AS likes
        FROM simulator_20250120.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY toDate(time), user_id, gender, age, os
        """
        feed_actions_df = ph.read_clickhouse(query=query_feed_actions, connection=connection)
        return feed_actions_df
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def get_message_actions_df(connection):
        
        # query to receive data about messages
        query_message_actions = """
        WITH messages_sent_table AS
            (SELECT user_id,
                    COUNT(1) AS messages_sent
            FROM simulator_20250120.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id),

            messages_received_table AS
            (SELECT receiver_id AS user_id,
                    COUNT(1) AS messages_received
            FROM simulator_20250120.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY receiver_id),

            users_received_table AS
            (SELECT receiver_id AS user_id,
                    COUNT(DISTINCT user_id) AS users_received
            FROM simulator_20250120.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY receiver_id),

            users_sent_table AS
            (SELECT user_id,
                    COUNT(DISTINCT receiver_id) AS users_sent
            FROM simulator_20250120.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id),

            all_users_data_table AS
            (SELECT yesterday() AS event_date,
                all_users.user_id AS user_id,
                GREATEST(fa.age, ma.age) AS age,
                GREATEST(fa.gender, ma.gender) AS gender,
                GREATEST(fa.os, ma.os) AS os
            FROM simulator_20250120.feed_actions AS fa RIGHT JOIN
                (SELECT DISTINCT receiver_id AS user_id
                FROM simulator_20250120.message_actions
                WHERE toDate(time) = yesterday()
                UNION ALL
                SELECT DISTINCT user_id AS user_id
                FROM simulator_20250120.message_actions
                WHERE toDate(time) = yesterday()) AS all_users ON all_users.user_id = fa.user_id
                LEFT JOIN simulator_20250120.message_actions AS ma ON all_users.user_id = ma.user_id
            GROUP BY user_id, age, gender, os)

        SELECT event_date,
                all_users_data_table.user_id AS user_id,
                age,
                gender,
                os,
                messages_sent_table.messages_sent AS messages_sent,
                messages_received_table.messages_received AS messages_received,
                users_received_table.users_received AS users_received,
                users_sent_table.users_sent AS users_sent
        FROM all_users_data_table LEFT JOIN messages_sent_table ON all_users_data_table.user_id = messages_sent_table.user_id
        LEFT JOIN messages_received_table ON all_users_data_table.user_id = messages_received_table.user_id
        LEFT JOIN users_received_table ON all_users_data_table.user_id = users_received_table.user_id
        LEFT JOIN users_sent_table ON all_users_data_table.user_id = users_sent_table.user_id
        """

        message_actions_df = ph.read_clickhouse(query=query_message_actions, connection=connection)
        return message_actions_df
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def feed_merge_message_actions(feed_actions_df, message_actions_df):
        
        feed_message_actions_df = feed_actions_df.merge(message_actions_df,
                                                        how='outer',
                                                        on=['event_date', 'user_id', 'age', 'gender', 'os']).fillna(0)
        feed_message_actions_df[['likes',
                             'views',
                             'messages_sent',
                             'messages_received',
                             'users_received',
                             'users_sent']] = feed_message_actions_df[['likes',
                                                                     'views',
                                                                     'messages_sent',
                                                                     'messages_received',
                                                                     'users_received',
                                                                     'users_sent']].astype(int)
        return feed_message_actions_df
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def gender_slice_feed_message_actions(feed_message_actions_df):
        gender_slice = feed_message_actions_df.drop(['age', 'os', 'user_id'], axis=1) \
                                                .groupby(['event_date', 'gender'], as_index=False) \
                                                .sum() \
                                                .rename(columns={'gender': 'dimension_value'})
        gender_slice['dimension'] = 'gender'
        
        return gender_slice
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def age_slice_feed_message_actions(feed_message_actions_df):
        age_slice = feed_message_actions_df.drop(['gender', 'os', 'user_id'], axis=1) \
                                            .groupby(['event_date', 'age'], as_index=False) \
                                            .sum() \
                                            .rename(columns={'age': 'dimension_value'})
        age_slice['dimension'] = 'age'
        return age_slice
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def os_slice_feed_message_actions(feed_message_actions_df):
        os_slice = feed_message_actions_df.drop(['gender', 'age', 'user_id'], axis=1) \
                                            .groupby(['event_date', 'os'], as_index=False) \
                                            .sum() \
                                            .rename(columns={'os': 'dimension_value'})
        os_slice['dimension'] = 'os'
        return os_slice
    
    @task(retries=3, retry_delay=timedelta(minutes=10))
    def load_to_clickhouse(gender_slice, age_slice, os_slice, connection_test):
        df_list=[gender_slice, age_slice, os_slice]
        all_slices = pd.concat(df_list, ignore_index=True)[['event_date',
                                                           'dimension',
                                                           'dimension_value',
                                                           'views',
                                                           'likes',
                                                           'messages_received',
                                                           'messages_sent',
                                                           'users_received',
                                                           'users_sent']]
        
        ph.to_clickhouse(df=all_slices, table='a_harcenco_actions_gender_age_os', index=False, connection=connection_test)
    
    connection = {
    'host': '**********************',
    'password': '************',
    'user': '*********',
    'database': '***********'
    }
    
    connection_test = {
    'host': '**************************',
    'database':'****',
    'user':'******-**', 
    'password':'*********'
    }
    
    # extraction
    feed_actions_df = get_feed_actions_df(connection)
    message_actions_df = get_message_actions_df(connection)
    
    # transformation
    feed_message_actions_df = feed_merge_message_actions(feed_actions_df, message_actions_df)
    gender_slice = gender_slice_feed_message_actions(feed_message_actions_df)
    age_slice = age_slice_feed_message_actions(feed_message_actions_df)
    os_slice = os_slice_feed_message_actions(feed_message_actions_df)
    
    # loading
    load_to_clickhouse(gender_slice, age_slice, os_slice, connection_test)

    
daily_actions_gender_age_os = daily_actions_gender_age_os()
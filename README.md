# etl_metrics_loader
Airflow DAG that runs daily, processing data for the previous day

The goal was to create an Airflow DAG that runs daily, processing data for the previous day.

Task Breakdown
Processing Two Tables in Parallel:

From the feed_actions table, calculate the number of content views and likes per user.
From the message_actions table, calculate per user:
The number of messages sent and received.
The number of unique users they sent messages to.
The number of unique users they received messages from.
Each extraction should be handled in a separate task.

Merging Data:
Combine the two processed tables into a single dataset.

Aggregations by Different Dimensions:
Calculate all metrics separately by:
Gender
Age
Operating System (OS)
Each aggregation should be performed in a dedicated task.

Storing the Final Data in ClickHouse:
The final dataset should be written into a dedicated table in ClickHouse.
The table should be incrementally updated daily with new data.
Final Table Structure
event_date	- The date of the events
dimension	- Type of segmentation (os, gender, or age)
dimension_value -	Value of the segmentation
views -	Number of content views
likes -	Number of likes
messages_received	- Number of messages received
messages_sent	- Number of messages sent
users_received -	Number of unique users the messages were received from
users_sent -	Number of unique users the messages were sent to
A dimension refers to OS, gender, or age.

The expected submission for this task is the table's name in the test schema.

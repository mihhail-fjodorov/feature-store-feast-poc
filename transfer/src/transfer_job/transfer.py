import time
import psycopg2
from apscheduler.schedulers.background import BackgroundScheduler


def get_db_connection_customer():
    conn = psycopg2.connect(
        host="feast-customer_db-1",
        port=5432,
        database="postgres",
        user="postgres",
        password="mysecretpassword",
    )
    return conn


def get_db_connection_offline_store():
    conn = psycopg2.connect(
        host="feast-offline_store-1",
        port=5432,
        database="postgres",
        user="postgres",
        password="mysecretpassword",
    )
    return conn


get_transaction_data = """
WITH minutes AS (
  SELECT generate_series(
    date_trunc('minute', NOW() - INTERVAL '10 minutes'),
    date_trunc('minute', NOW()),
    '1 minute'::INTERVAL
  ) AS event_time
)
SELECT 
  tr.customer_uid,
  m.event_time,
  count(*) FILTER (WHERE amount >= 50 AND direction = 'OUT') as count_50_last_hour,
  count(*) FILTER (WHERE amount >= 200 AND direction = 'OUT') as count_200_last_hour,
  count(*) FILTER (WHERE amount >= 500 AND direction = 'OUT') as count_500_last_hour,
  sum(amount) FILTER (WHERE amount >= 50 AND direction = 'OUT') as sum_50_last_hour,
  sum(amount) FILTER (WHERE amount >= 200 AND direction = 'OUT') as sum_200_last_hour,
  sum(amount) FILTER (WHERE amount >= 500 AND direction = 'OUT') as sum_500_last_hour
FROM 
  minutes m
JOIN 
  transaction tr ON m.event_time = date_trunc('minute', tr.created_at)
GROUP BY 
  tr.customer_uid,
  m.event_time
ORDER BY
  tr.customer_uid,
  m.event_time
"""

insert_transaction_stats = """
INSERT INTO public.transaction_minutly_stats (customer_uid, event_time, count_50_last_hour, count_200_last_hour, count_500_last_hour, sum_50_last_hour, sum_200_last_hour, sum_500_last_hour, created_at) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (customer_uid, event_time) 
DO UPDATE SET 
    customer_uid        = EXCLUDED.customer_uid,
    event_time          = EXCLUDED.event_time, 
    count_50_last_hour  = EXCLUDED.count_50_last_hour,
    count_200_last_hour = EXCLUDED.count_200_last_hour,
    count_500_last_hour = EXCLUDED.count_500_last_hour,
    sum_50_last_hour    = EXCLUDED.sum_50_last_hour,
    sum_200_last_hour   = EXCLUDED.sum_200_last_hour,
    sum_500_last_hour   = EXCLUDED.sum_500_last_hour,
    created_at          = EXCLUDED.created_at
"""


def transafer():
    # get database connections
    conn_source = get_db_connection_customer()
    conn_dest = get_db_connection_offline_store()
    # get cursors to the database
    cur_source = conn_source.cursor()
    cur_dest = conn_dest.cursor()
    try:
        # get source data
        cur_source.execute(get_transaction_data)
        transaction_features = cur_source.fetchall()

        # write features to the destination
        for row in transaction_features:
            print(row[0])
            cur_dest.execute(
                insert_transaction_stats,
                (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]),
            )

        print(f"Inserted {len(transaction_features)} rows in to the offline store")
        # flush data
        conn_source.commit()
        conn_dest.commit()

    except Exception as ex:
        print(ex)
        raise RuntimeError() from ex
    finally:
        print("closing database resources")
        # clean up database resources
        cur_source.close()
        cur_dest.close()
        conn_source.close()
        conn_dest.close()


def main():
    scheduler = BackgroundScheduler()
    scheduler.add_job(transafer, "interval", minutes=10)
    scheduler.start()

    try:
        while True:
            time.sleep(3)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


main()

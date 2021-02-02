import os
import json
import psycopg2
from datetime import datetime, timedelta
import clickhouse_driver


execution_date = os.environ['execution_date']
update_status = os.environ['update_status']


def main(d):

    with open("./credentials.json", "r+") as credJson:
        refs = json.loads(credJson.read())

    ps_conn = psycopg2.connect(dbname=refs["postgres_db"],
                               user=refs["postgres_user"],
                               password=refs["postgres_pw"],
                               host=refs["postgres_host"])
    ps_cursor = ps_conn.cursor()

    ch_conn = clickhouse_driver.connect(user=refs["clickhouse_user"],
                                        password=refs["clickhouse_pw"],
                                        host=refs["clickhouse_host"],
                                        port='9000',
                                        database=refs["clickhouse_db"])
    ch_cursor = ch_conn.cursor()

    d = datetime.strptime(d, '%Y-%m-%d')
    start_date = (d - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    stop_date = d.strftime("%Y-%m-%d %H:%M:%S")

    print(f"EXECUTION PERIOD {start_date} AND {stop_date}")

    query = """ SELECT uid, id_track, first_time_data, last_time_data, serial_number_equip
                FROM public.noise_tracks
                WHERE max_slow_time is null and max_slow = 0
                        AND last_time_data between '{}' and '{}'
                ORDER BY last_time_data DESC """.format(start_date, stop_date)
    ps_cursor.execute(query)
    arr = ps_cursor.fetchall()

    for line in arr:
        uid, id_track, start, stop, sn = line
        start = start.strftime("%Y-%m-%d %H:%M:%S")
        stop = stop.strftime("%Y-%m-%d %H:%M:%S")
        query = """ SELECT E.time_stamp, E.slow
                    FROM eco_monitoring.noise_raw_data as E
                    INNER JOIN (
                         SELECT max(slow) as max_slow
                         FROM eco_monitoring.noise_raw_data
                         WHERE time_stamp between '{}' and '{}' and serial_number_equip = '{}'
                    ) as M on E.slow = M.max_slow
                    WHERE E.time_stamp between '{}' and '{}'
                            and E.serial_number_equip = '{}'; """.format(start, stop, sn,
                                                                         start, stop, sn)
        ch_cursor.execute(query)
        record = ch_cursor.fetchone()
        if record is not None:
            max_slow_time, max_slow = record
            print(f"uid: {uid}, id_track: {id_track}, first_time_data: {start}, last_time_data: {stop}, max_slow: {max_slow}, max_slow_time: {max_slow_time}")
            if update_status == 'true':
                postgres_update_query = """ UPDATE public.noise_tracks
                                            SET max_slow = %s, max_slow_time = %s
                                            WHERE uid = %s """
                ps_cursor.execute(postgres_update_query, (max_slow, max_slow_time, uid))
                ps_conn.commit()
        else:
            print(f"Attention, for uid {uid} and id_track {id_track} no max_slow")

    ps_cursor.close()
    ps_conn.close()
    ch_cursor.close()
    ch_conn.close()


if __name__ == '__main__':
    main(execution_date)
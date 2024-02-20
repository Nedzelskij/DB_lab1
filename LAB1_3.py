import psycopg2
import concurrent.futures
import time

username = 'nedzelsky'
password = '3030'
database = 'DB_LAB_1'

query_00 = '''
DROP TABLE IF EXISTS user_counter
'''

query_0 = '''
CREATE TABLE user_counter (
    user_id INT PRIMARY KEY,
    counter INT,
    version INT
)
'''

query_1 = '''
INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:                      
    cursor = conn.cursor()

    cursor.execute(query_00)

    cursor.execute(query_0)

    cursor.execute(query_1)

def row_level_locking_update(user_thread_id):
    conn = psycopg2.connect(user=username, password=password, dbname=database)

    with conn:                   
        cursor = conn.cursor()

        for _ in range(10_000):
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s FOR UPDATE", (user_thread_id[0],))
            counter = cursor.fetchone()[0]
            counter += 1
            cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, user_thread_id[0]))

        conn.commit()

start_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
        executor.map(row_level_locking_update, [(1, i) for i in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Total time: {total_time} seconds")
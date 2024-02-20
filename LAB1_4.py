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

def optimistic_concurrency_control_update(user_thread_id):
    conn = psycopg2.connect(user=username, password=password, dbname=database)
    
    with conn:
        cursor = conn.cursor()

        for i in range(10_000):
            while True:
                cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = %s", (user_thread_id[0],))
                current_values = cursor.fetchone()
                counter, version = current_values[0], current_values[1]

                counter += 1
                cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                            (counter, version + 1, user_thread_id[0], version))

                conn.commit()
                count = cursor.rowcount
                if count > 0:
                    break

start_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
        executor.map(optimistic_concurrency_control_update, [(1, i) for i in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Total time: {total_time} seconds")
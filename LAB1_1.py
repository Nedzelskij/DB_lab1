import psycopg2
import time
import concurrent.futures

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

query_001 = '''
DELETE FROM user_counter
'''

query_1 = '''
INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)
                       
cursor = conn.cursor()

cursor.execute(query_00)

cursor.execute(query_0)

cursor.execute(query_1)

def lost_update(user_id):
    for _ in range(10_000):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (user_id,))
        counter = cursor.fetchone()[0]
        counter += 1
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, user_id))
    conn.commit()

start_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
        executor.map(lost_update, [1 for _ in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Total time: {total_time} seconds")

cursor.close()
conn.close()
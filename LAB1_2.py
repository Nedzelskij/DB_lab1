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

query_1 = '''
INSERT INTO user_counter (user_id, counter, version) VALUES (1, 0, 0)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)
                       
cursor = conn.cursor()

cursor.execute(query_00)

cursor.execute(query_0)

cursor.execute(query_1)

def in_place_update(user_id):
    for _ in range(10_000):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = %s", (user_id,))
    conn.commit()

start_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(in_place_update, [1 for _ in range(10)])

end_time = time.time()
total_time = end_time - start_time

print(f"Total time: {total_time} seconds")

cursor.close()
conn.close()
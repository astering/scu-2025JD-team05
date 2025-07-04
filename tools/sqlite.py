import sqlite3

def main():
    # conn = sqlite3.connect('.local/playground/track_metadata.db')
    conn = sqlite3.connect('.local/playground/mxm_dataset.db')
    c = conn.cursor()
    c2 = conn.cursor()

    # q = "SELECT name FROM sqlite_master WHERE type='table';"
    # res = c.execute(q)
    # for row in res:
    #     for i in row:
    #         print(i) # 打印表名
    #         # continue
    #         q = f"PRAGMA table_info({i});"
    #         res2 = c2.execute(q)
    #         for row2 in res2:
    #             # print(len(row2)) # 元组长度
    #             print(row2)

    q = ''
    # q = "show databases;" # 不行
    # q = "PRAGMA table_info('sqlite_master');"
    # q = "SELECT * FROM sqlite_master;"
    # q = "SELECT * FROM sqlite_master WHERE type='table';"
    # q = "PRAGMA table_info('songs');"
    # q = "SELECT * FROM sqlite_master WHERE type='index' ORDER BY name;"
    # q = "SELECT * FROM sqlite_master WHERE type='view' ORDER BY name;"
    # q = "SELECT * FROM sqlite_master WHERE type='trigger' ORDER BY name;"
    # q = "SELECT count(*) FROM songs;"
    # q = "SELECT count(*) FROM words;"
    # q = "SELECT count(*) FROM lyrics;"
    # q = "SELECT * FROM words limit 5;"
    # q = "SELECT * FROM lyrics limit 200;"
    q = "SELECT word, sum(count) as count FROM lyrics where word = 'you' group by word limit 5;"
    # q = "SELECT word, sum(count) as count FROM lyrics group by word having word = i limit 5;"
    # q = "SELECT word, sum(count) as count FROM lyrics group by word order by count desc limit 5;"

    if q:
        res = c.execute(q)
        indexes = res.fetchall()
        print(len(indexes))
        # print(indexes)
        for index in indexes:
            print(index)

    print()
    q = ''

    # q = "SELECT * FROM songs limit 1;"
    # q = "SELECT * FROM songs WHERE title='Innocence' and artist_name = 'Avril Lavigne'"
    # q = "SELECT * FROM songs WHERE title='Lemon Tree'"
    # q = "SELECT * FROM songs WHERE title='You Are Not Alone'"
    # q = "SELECT * FROM songs WHERE title='My Heart Will Go On'"
    # q = "SELECT * FROM songs WHERE title='The Phoenix'"
    # q = "SELECT * FROM songs WHERE track_id='TRBWGTT128F428211F'"
    # q = "SELECT * FROM songs WHERE artist_name='Michael Jackson'"

    if q:
        # res = conn.execute(q)
        res = c.execute(q)
        # print(res)
        for row in res: # res是数组，row是元组，i是元素
            # print(row)
            print(f"{row[1]} | {row[6]} | {row[3]} | {row[7]}") # 打印固定字段
            # for i in row:
                # print(i)

    print()
    q = ''

    # q = "SELECT distinct song_id FROM songs WHERE title='Innocence' and artist_name = 'Avril Lavigne'"
    # q = "SELECT distinct track_id FROM songs WHERE title='Innocence' and artist_name = 'Avril Lavigne'"
    # q = "SELECT count(*) FROM songs WHERE artist_name='Michael Jackson'"
    # q = "SELECT * FROM songs WHERE artist_name='Michael Jackson' limit 3"

    if q:
        res = c.execute(q)
        # print(res)
        # print(res.fetchall())
        # return
        for row in res: # res是数组，row是元组，i是元素
            print(row)
            # for i in row:
            #     print(i)

main()
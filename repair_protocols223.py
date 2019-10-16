import pymysql

USER_BD = ''
PASS_DB = ''
NAME_DB = ''


def connect_bd(baza):
    con = pymysql.connect(host="localhost", user=USER_BD, passwd=PASS_DB, db=baza, charset='utf8',
                          init_command='SET NAMES UTF8', cursorclass=pymysql.cursors.DictCursor)
    con.autocommit(True)
    return con


def change_type(con, list_prot):
    cur3 = con.cursor()
    cur4 = con.cursor()
    for item in list_prot:
        cur3.execute("""SELECT xml FROM protocols223 WHERE id = %s""", (item['id'],))
        res_select = cur3.fetchone()
        split_list = res_select['xml'].split('/')
        if len(split_list) > 3:
            cur4.execute("""UPDATE protocols223 SET type_ftp = %s WHERE id = %s""", (split_list[4], item['id']))

    cur4.close()
    cur3.close()


def main():
    con = connect_bd(NAME_DB)
    cur = con.cursor()
    cur.execute("""SELECT COUNT(id) AS c FROM protocols223 WHERE type_ftp = ''""")
    res_count = cur.fetchone()
    if res_count['c'] == 0:
        cur.close()
        con.close()
        return
    page = int(res_count['c'] / 10000)
    cur.close()
    cur2 = con.cursor()
    if page == 0:
        cur2.execute("""SELECT id FROM protocols223""")
        res_cur2 = cur2.fetchall()
        change_type(con, res_cur2)
    else:
        for i in range(0, page + 1):
            cur2.execute("""SELECT id FROM protocols223 WHERE type_ftp = '' LIMIT 10000 OFFSET %s""", (i * 10000,))
            res_cur2 = cur2.fetchall()
            change_type(con, res_cur2)

    cur2.close()
    con.close()


if __name__ == '__main__':
    main()

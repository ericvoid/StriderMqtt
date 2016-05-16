import sqlite3

class opencursor(object):
    def __init__(self, conn):
        self.conn = conn
        self.curs = None

    def __enter__(self):
        self.curs = self.conn.cursor()
        return self.curs

    def __exit__(self, type, value, traceback):
        self.curs.close()

def check_outgoing(conn):
    print '  checking outgoing messages'
    sent = list()
    with opencursor(conn) as cur:
        result = cur.execute("SELECT number FROM published_numbers ORDER BY id ASC")
        for row in result:
            sent.append(row['number'])

    i = 1
    check = set(range(min(sent), max(sent) + 1))
    for a, b in zip(sent[:-1], sent[1:]):
        if a in check:
            check.remove(a)
        if b in check:
            check.remove(b)
        if b < a:
            print '   ', b, 'in wrong order'
    print '    missing: ', ', '.join([str(x) for x in check])




def check_incoming(conn, qos):
    print '  checking incoming messages'
    topics = get_received_topics(conn)
    for t in topics:
        check_incoming_from_topic(conn, qos, t)


def get_received_topics(conn):
    res = list()
    with opencursor(conn) as cur:
        result = cur.execute("SELECT DISTINCT topic FROM received_numbers ORDER BY id ASC")
        for row in result:
            res.append(row['topic'])
    return res


def check_incoming_from_topic(conn, qos, t):
    allowed_dups = 0
    with opencursor(conn) as cur:
        result = cur.execute("SELECT number FROM received_numbers WHERE topic = ? ORDER BY id ASC", (t, ))
        next_number = 1
        for row in result:
            if row['number'] == next_number - 1:
                if qos == 1:
                    allowed_dups += 1
                else:
                    print '    [%s] : received duplicate %s' % (t, next_number)
            elif row['number'] != next_number:
                print '    [%s] : expected %s, got %s' % (t, next_number, row['number'])
                next_number = row['number'] + 1
            else:
                next_number += 1
    if allowed_dups > 0:
        print '    %s allowed dups' % allowed_dups


def check(runid, qos, client_i):
    dbname = 'client-%s-%d.sqlite.' % (runid, client_i)
    print 'checking', dbname
    with sqlite3.connect(dbname) as connection:
        connection.row_factory = sqlite3.Row
        check_outgoing(connection)
        check_incoming(connection, qos)


if __name__ == '__main__':
    import sys
    args = sys.argv
    runid = args[1]
    qos = int(args[2])
    clients_count = int(args[3])

    for i in range(clients_count):
        check(runid, qos, i)

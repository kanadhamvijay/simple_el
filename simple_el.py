import petl as etl, psycopg2 as pg, sys
from sqlalchemy import *
import importlib

importlib.reload(sys)

#sys.setdefaultencoding('utf8')

dbcnxnx = {
  'kvkredb' : "dbname = kvkredb user = postgres host = 127.0.0.1",
  'postgres': "dbname = postgres user = postgres host = 127.0.0.1"
}

sourceConn = pg.connect(dbcnxnx['kvkredb'])
targetConn = pg.connect(dbcnxnx['postgres'])
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()

sourceCursor.execute("""
select table_name from information_schema.columns where table_schema = 'public' 
and table_name in ('auth_permission')
""")
#('listings_listing', 'realtors_realtor')
sourceTables = sourceCursor.fetchall()

for t in sourceTables:
  targetCursor.execute("drop table if exists %s" % (t[0]))
  sourceDs = etl.fromdb(sourceConn, "select * from %s" % (t[0]))
  etl.todb(sourceDs, targetConn, t[0], create=True, sample = 1000)
from sqlalchemy.dialects import registry
from sqlalchemy import create_engine
from sqlalchemy import text

registry.register("vogon", "vogon", "VogonDialect")
engine = create_engine('vogon://vogon.reports.mn')
conn = engine.raw_connection()
cursor = conn.cursor()
text_query = text("SELECT customer_id, net_bid from cm.rts_customer_stats where ts = '2021071600'")
results = cursor.execute(text_query).fetchall()

for row in results:
    print(row)

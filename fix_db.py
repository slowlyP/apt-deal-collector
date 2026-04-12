import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.getenv('DB_URL'))

with engine.begin() as conn:
    res = conn.execute(text("UPDATE apt_deals SET is_cancelled = FALSE, cancel_date = '' WHERE trim(cancel_date) = ''"))
    print(f"Updated {res.rowcount} rows.")

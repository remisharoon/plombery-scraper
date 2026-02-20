from sqlalchemy import create_engine, text
from src.config import read_config


def get_neon_engine():
    neondb_config = read_config()['PostgresDB']
    connection_string = neondb_config['connection_string']
    return create_engine(connection_string)


try:
    engine = get_neon_engine()
    # Create a connection object
    conn = engine.connect()

    # Execute a SQL query
    result = conn.execute(text('SELECT 1'))

    # Print the result
    print(result.fetchone()[0])

    res = conn.execute(text("SELECT now()")).fetchall()
    print(res)

    # Close the connection
    conn.close()
except Exception as e:
    print("error --- ",e)


import duckdb
import glob

DATABASE = "lake.duckdb"
RAW_SCHEMA = "raw"

def load_to_duckdb():
    conn = duckdb.connect(DATABASE)

    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};")

    parquet_files = glob.glob("data/*.parquet")
    for file in parquet_files:
        table_name = file.split("/")[-1].replace(".parquet", "")
        conn.execute(f"""
            CREATE TABLE {RAW_SCHEMA}.{table_name} AS SELECT * FROM read_parquet('{file}');
        """)
        print(f"Table {RAW_SCHEMA}.{table_name} créée avec succès.")

    conn.close()

if __name__ == "__main__":
    load_to_duckdb()

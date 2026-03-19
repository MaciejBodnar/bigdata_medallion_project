import argparse
from pathlib import Path
import duckdb


SQL_FILES = [
    "sql/00_init.sql",
    "sql/01_load_raw.sql",
    "sql/02_create_silver.sql",
    "sql/03_create_gold.sql",
]


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    print(f"[run] {path}")
    sql = path.read_text(encoding="utf-8")
    con.execute(sql)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="warehouse.duckdb")
    parser.add_argument("--data-dir", default="data/raw")
    args = parser.parse_args()

    con = duckdb.connect(args.db)
    con.execute(f"SET home_directory='{Path('.').resolve().as_posix()}';")

    for sql_file in SQL_FILES:
        run_sql_file(con, Path(sql_file))

    print("[done] pipeline finished")


if __name__ == "__main__":
    main()

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    host = os.getenv("PGHOST")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pw = os.getenv("PGPASSWORD")
    port = int(os.getenv("PGPORT", "5432"))

    assert all([host, db, user, pw, port]), "Missing one or more Postgres env vars in .env"

    return psycopg2.connect(
        host=host,
        database=db,
        user=user,
        password=pw,
        port=port,
        sslmode="require",
    )


class DatabaseClient:
    def __init__(self):
        self._conn = None

    def __enter__(self):
        self._conn = get_conn()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._conn:
            self._conn.close()

    @property
    def conn(self):
        return self._conn

    # -----------------------------
    # Schema / tables
    # -----------------------------
    def create_tables(self):
        """
        Creates:
          1) departments
          2) employees (raw; allows dirty values)
          3) projects
          4) employee_projects (bridge table)
        """
        sql = """
        -- 1) Departments
        CREATE TABLE IF NOT EXISTS departments (
            department_id SERIAL PRIMARY KEY,
            department_name TEXT NOT NULL,
            location TEXT NOT NULL,
            budget INTEGER NOT NULL
        );

        -- Make department_name unique so ON CONFLICT works
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'departments_department_name_key'
            ) THEN
                ALTER TABLE departments
                ADD CONSTRAINT departments_department_name_key UNIQUE (department_name);
            END IF;
        END $$;

        -- 2) Employees (RAW table; no salary/date check constraints)
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT,
            position TEXT,
            start_date DATE,
            salary INTEGER,
            department_id INTEGER REFERENCES departments(department_id)
        );

        -- Add is_dirty if missing
        ALTER TABLE employees
            ADD COLUMN IF NOT EXISTS is_dirty BOOLEAN DEFAULT FALSE;

        -- 3) Projects
        CREATE TABLE IF NOT EXISTS projects (
            project_id SERIAL PRIMARY KEY,
            project_name TEXT NOT NULL,
            project_type TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            budget INTEGER NOT NULL
        );

        -- Make project_name unique so ON CONFLICT works
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'projects_project_name_key'
            ) THEN
                ALTER TABLE projects
                ADD CONSTRAINT projects_project_name_key UNIQUE (project_name);
            END IF;
        END $$;

        -- 4) Bridge table
        CREATE TABLE IF NOT EXISTS employee_projects (
            employee_id INTEGER REFERENCES employees(employee_id),
            project_id INTEGER REFERENCES projects(project_id),
            role_on_project TEXT,
            PRIMARY KEY (employee_id, project_id)
        );
        """

        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

    # -----------------------------
    # Departments
    # -----------------------------
    def seed_departments(self, departments):
        """
        departments: list of tuples (department_name, location, budget)
        """
        sql = """
        INSERT INTO departments (department_name, location, budget)
        VALUES (%s, %s, %s)
        ON CONFLICT (department_name) DO UPDATE
        SET location = EXCLUDED.location,
            budget = EXCLUDED.budget;
        """
        with self.conn.cursor() as cur:
            cur.executemany(sql, departments)
        self.conn.commit()

    def fetch_departments(self):
        return pd.read_sql(
            "SELECT department_id, department_name, location, budget FROM departments ORDER BY department_id;",
            self.conn,
        )

    # -----------------------------
    # Employees (SAFE INSERT)
    # -----------------------------
    def insert_employees_df(self, df: pd.DataFrame):
        """
        Inserts employees safely:
        - converts numeric fields
        - clamps integers to Postgres INT range
        - converts NaN/<NA>/NaT -> None
        - rollbacks on error so connection doesn't get stuck in aborted state
        - if still failing, prints the exact bad row
        """
        required = ["employee_id", "name", "position", "start_date", "salary", "department_id", "is_dirty"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for insert: {missing}")

        PG_INT_MIN = -2147483648
        PG_INT_MAX = 2147483647

        df2 = df.copy()

        # Safe numeric conversion
        df2["employee_id"] = pd.to_numeric(df2["employee_id"], errors="coerce")
        df2["salary"] = pd.to_numeric(df2["salary"], errors="coerce")
        df2["department_id"] = pd.to_numeric(df2["department_id"], errors="coerce")

        # Clamp to Postgres INT range
        for col in ["employee_id", "salary", "department_id"]:
            df2.loc[df2[col] > PG_INT_MAX, col] = PG_INT_MAX
            df2.loc[df2[col] < PG_INT_MIN, col] = PG_INT_MIN

        # Dates -> python date (NaT becomes NaN -> then None below)
        df2["start_date"] = pd.to_datetime(df2["start_date"], errors="coerce").dt.date

        # Convert pandas missing values to None for psycopg2
        df2 = df2.where(pd.notnull(df2), None)

        sql = """
        INSERT INTO employees (employee_id, name, position, start_date, salary, department_id, is_dirty)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (employee_id) DO NOTHING;
        """

        rows = list(df2[required].itertuples(index=False, name=None))

        with self.conn.cursor() as cur:
            try:
                cur.executemany(sql, rows)
            except Exception as e:
                # ✅ rollback fixes "InFailedSqlTransaction"
                self.conn.rollback()

                # Find exact failing row (row-by-row)
                for i, r in enumerate(rows):
                    try:
                        cur.execute(sql, r)
                    except Exception:
                        self.conn.rollback()
                        print("❌ Bad row at index:", i)
                        print("❌ Values:", r)
                        raise
                raise e

        self.conn.commit()

    def load_employees_joined(self):
        q = """
        SELECT e.employee_id, e.name, e.position, e.start_date, e.salary, e.is_dirty,
               d.department_name, d.location, d.budget
        FROM employees e
        LEFT JOIN departments d ON e.department_id = d.department_id
        ORDER BY e.employee_id;
        """
        return pd.read_sql(q, self.conn)

    def count_employees(self):
        df = pd.read_sql("SELECT COUNT(*) AS n FROM employees;", self.conn)
        return int(df.loc[0, "n"])

    # -----------------------------
    # Projects
    # -----------------------------
    def insert_projects_df(self, df: pd.DataFrame):
        """
        Expected columns:
          project_name, project_type, start_date, end_date, budget
        """
        required = ["project_name", "project_type", "start_date", "end_date", "budget"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for projects insert: {missing}")

        df2 = df.copy()
        df2["budget"] = pd.to_numeric(df2["budget"], errors="coerce")
        df2["start_date"] = pd.to_datetime(df2["start_date"], errors="coerce").dt.date
        df2["end_date"] = pd.to_datetime(df2["end_date"], errors="coerce").dt.date
        df2 = df2.where(pd.notnull(df2), None)

        sql = """
        INSERT INTO projects (project_name, project_type, start_date, end_date, budget)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (project_name) DO UPDATE
        SET project_type = EXCLUDED.project_type,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            budget = EXCLUDED.budget;
        """
        rows = list(df2[required].itertuples(index=False, name=None))

        with self.conn.cursor() as cur:
            try:
                cur.executemany(sql, rows)
            except Exception:
                self.conn.rollback()
                raise

        self.conn.commit()

    def fetch_projects(self):
        return pd.read_sql(
            "SELECT project_id, project_name, project_type, start_date, end_date, budget FROM projects ORDER BY project_id;",
            self.conn,
        )

    # -----------------------------
    # Employee <-> Projects assignments
    # -----------------------------
    def insert_employee_projects_df(self, df: pd.DataFrame):
        """
        Expected columns:
          employee_id, project_id, role_on_project
        """
        required = ["employee_id", "project_id", "role_on_project"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns for employee_projects insert: {missing}")

        df2 = df.copy()
        df2["employee_id"] = pd.to_numeric(df2["employee_id"], errors="coerce")
        df2["project_id"] = pd.to_numeric(df2["project_id"], errors="coerce")
        df2 = df2.where(pd.notnull(df2), None)

        sql = """
        INSERT INTO employee_projects (employee_id, project_id, role_on_project)
        VALUES (%s, %s, %s)
        ON CONFLICT (employee_id, project_id) DO NOTHING;
        """
        rows = list(df2[required].itertuples(index=False, name=None))

        with self.conn.cursor() as cur:
            try:
                cur.executemany(sql, rows)
            except Exception:
                self.conn.rollback()
                raise

        self.conn.commit()

    def load_employee_projects_joined(self):
        q = """
        SELECT e.employee_id, e.name, e.position, e.start_date, e.salary, e.is_dirty,
               d.department_name,
               p.project_name, p.project_type, p.budget AS project_budget,
               ep.role_on_project
        FROM employee_projects ep
        JOIN employees e ON ep.employee_id = e.employee_id
        LEFT JOIN departments d ON e.department_id = d.department_id
        JOIN projects p ON ep.project_id = p.project_id;
        """
        return pd.read_sql(q, self.conn)

    # -----------------------------
    # Debug helpers
    # -----------------------------
    def list_tables(self):
        return pd.read_sql(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name;
            """,
            self.conn,
        )

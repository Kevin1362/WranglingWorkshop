import random
from datetime import date
import numpy as np
import pandas as pd
from faker import Faker

from .config import POSITIONS, NON_IT_TITLES


class EmployeeDataGenerator:
    """Generates synthetic employee records and injects dirty/incomplete/illogical data."""

    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        np.random.seed(seed)

    def _random_date_2015_2024(self):
        start = date(2015, 1, 1).toordinal()
        end = date(2024, 12, 31).toordinal()
        return date.fromordinal(random.randint(start, end))

    def generate(self, n: int = 500, dirty_frac: float = 0.20, department_ids=None) -> pd.DataFrame:
        if department_ids is None:
            department_ids = [None]

        rows = []
        used_ids = set()

        dirty_n = int(round(n * dirty_frac))
        dirty_idx = set(random.sample(range(n), dirty_n))

        for i in range(n):
            emp_id = random.randint(100000, 999999)
            while emp_id in used_ids:
                emp_id = random.randint(100000, 999999)
            used_ids.add(emp_id)

            name = self.fake.name()
            position = random.choice(POSITIONS)
            start_date = self._random_date_2015_2024()
            salary = random.randint(60000, 200000)
            dept_id = random.choice(department_ids)
            is_dirty = i in dirty_idx

            if is_dirty:
                issue_type = random.choice(["missing", "salary", "date", "title", "mixed"])

                if issue_type in ["missing", "mixed"]:
                    if random.random() < 0.6: name = None
                    if random.random() < 0.6: position = None
                    if random.random() < 0.6: start_date = None
                    if random.random() < 0.6: salary = None

                if issue_type in ["salary", "mixed"]:
                    # Dirty but safe for Postgres INTEGER
                    salary = random.choice([-5000, 0, 45000, 350000, 2000000])

                if issue_type in ["date", "mixed"]:
                    start_date = random.choice([date(2010, 5, 1), date(2030, 1, 1), date(2025, 12, 31)])

                if issue_type in ["title", "mixed"]:
                    position = random.choice(NON_IT_TITLES)

            rows.append({
                "employee_id": emp_id,
                "name": name,
                "position": position,
                "start_date": start_date,
                "salary": salary,
                "department_id": dept_id,
                "is_dirty": is_dirty
            })

        return pd.DataFrame(rows)

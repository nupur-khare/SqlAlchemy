import os
import unittest
from unittest.mock import patch

from Script.employee import EmployeeDatabase


class TestEmployeeDatabase(unittest.TestCase):
    def setUp(self):
        self.db_url = "sqlite:///test_db.db"
        self.employee_db = EmployeeDatabase(self.db_url)

    def test_populate_employee_data(self):
        employee_data = [
            (101, "Test User", "Tester", 80000),
            (102, "Another User", "Developer", 100000),
        ]

        self.employee_db.insert_employee_data(employee_data)

        result = self.employee_db.get_all_employees_dataframe()
        self.assertEqual(len(result), 2)

    def test_populate_employee_data_exception(self):
        employee_data = [
            (101, "Test User", "Tester"),
            (102, "Another User", "Developer", 100000),
        ]
        with self.assertRaises(Exception):
            self.employee_db.insert_employee_data(employee_data)

    def test_calculate_average_salary_per_role(self):
        employee_data = [
            (101, "Test User", "Tester", 80000),
            (102, "Another User", "Tester", 120000),
            (103, "Third User", "Developer", 100000),
        ]

        self.employee_db.insert_employee_data(employee_data)
        result = self.employee_db.calculate_average_salary_per_role()

        expected_result = [('Tester', 100000.0), ('Developer', 100000.0)]
        self.assertCountEqual(result, expected_result)

    def test_calculate_average_salary_per_role_exception(self):
        with patch.object(self.employee_db.session, 'query', side_effect=Exception("Test exception")):
            with self.assertRaises(Exception) as context:
                self.employee_db.calculate_average_salary_per_role()
        self.assertEqual(str(context.exception), "Error calculating average salary per role: Test exception")

    def test_get_all_employees_dataframe_exception(self):
        with patch.object(self.employee_db.session, 'query', side_effect=Exception("Test exception")):
            with self.assertRaises(Exception) as context:
                self.employee_db.get_all_employees_dataframe()
        self.assertEqual(str(context.exception), "Error getting all employees data: Test exception")

    def test_get_combined_dataframe(self):
        employee_data = [
            (101, "Test User", "Tester", 80000),
            (102, "Another User", "Tester", 120000),
            (103, "Third User", "Developer", 100000),
        ]

        self.employee_db.insert_employee_data(employee_data)
        result = self.employee_db.get_combined_dataframe()

        expected_columns = ['ID', 'Name', 'Role', 'Salary', 'Average Salary']
        self.assertCountEqual(result.columns, expected_columns)

    def test_get_combined_dataframe_exception(self):
        with patch.object(self.employee_db, 'calculate_average_salary_per_role', side_effect=Exception("Test exception")):
            with patch.object(self.employee_db, 'get_all_employees_dataframe', side_effect=Exception("Test exception")):
                with self.assertRaises(Exception) as context:
                    self.employee_db.get_combined_dataframe()
        self.assertEqual(str(context.exception), "Error getting combined dataframe: Test exception")

    def tearDown(self):
        self.employee_db.close_session()
        os.remove("test_db.db")

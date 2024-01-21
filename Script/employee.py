import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Sequence, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from exceptions import AppException


class EmployeeDatabase:
    def __init__(self, db_url):
        self.db_url = db_url
        self.engine = create_engine(db_url, echo=True)
        self.Base = declarative_base()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

        self.Employee = self.create_employee_model()

        self.Base.metadata.create_all(self.engine)

    def create_employee_model(self):
        class Employee(self.Base):
            __tablename__ = 'employees'

            id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
            name = Column(String(50))
            role = Column(String(50))
            salary = Column(Integer)
        return Employee

    def insert_employee_data(self, employee_data):
        """
        inserts data into the table
        :param employee_data: employee details
        """
        try:
            for data in employee_data:
                employee = self.Employee(id=data[0], name=data[1], role=data[2], salary=data[3])
                self.session.add(employee)
            self.session.commit()

        except Exception as e:
            raise AppException(f"Error inserting employee data: {str(e)}")

    def calculate_average_salary_per_role(self):
        """
        calculates average salary of every employee by role
        """
        try:
            return self.session.query(self.Employee.role, func.avg(self.Employee.salary).label('average_salary')).group_by(
                self.Employee.role).all()

        except Exception as e:
            raise AppException(f"Error calculating average salary per role: {str(e)}")

    def get_all_employees_dataframe(self):
        """
        Creates a dataframe for the table
        """
        try:
            all_employees = self.session.query(self.Employee).all()
            return pd.DataFrame(
                [(employee.id, employee.name, employee.role, employee.salary) for employee in all_employees],
                columns=['ID', 'Name', 'Role', 'Salary'])

        except Exception as e:
            raise AppException(f"Error getting all employees data: {str(e)}")

    def get_combined_dataframe(self):
        """
        Combines the dataframe with average salary as a new column
        """
        try:
            average_salary_per_role = self.calculate_average_salary_per_role()
            df_average_salary = pd.DataFrame(average_salary_per_role, columns=['Role', 'Average Salary'])
            df_employees = self.get_all_employees_dataframe()
            return pd.merge(df_employees, df_average_salary, on='Role')

        except Exception as e:
            raise AppException(f"Error getting combined dataframe: {str(e)}")

    def close_session(self):
        self.session.close()

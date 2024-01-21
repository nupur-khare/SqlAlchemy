from employee import EmployeeDatabase

if __name__ == "__main__":
    db_url = "sqlite:///company.db"

    employee_data = [
        (101, "Nupur Khare", "Data Engineer", 900000),
        (102, "Ria Sharma", "Software Engineer", 1000000),
        (103, "Aditya K", "Platform Engineer", 1800000),
        (104, "Shubhi Das", "Solutions Architect", 2500000),
        (105, "John Doe", "System Engineer", 600000),
        (106, "Rishi S", "System Engineer", 500000),
        (107, "Suman K", "Platform Engineer", 2900000),
        (108, "Zain C", "Data Engineer", 1700000),
        (109, "Yuzi L", "Software Engineer", 1300000),
        (110, "Krish N", "Solutions Architect", 2900000)
    ]

    employee_db = EmployeeDatabase(db_url)

    employee_db.insert_employee_data(employee_data)

    combined_df = employee_db.get_combined_dataframe()
    print("\nCombined DataFrame - Employee Data with Average Salary per Role:")
    print(combined_df)

    employee_db.close_session()

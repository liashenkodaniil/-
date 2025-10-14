# Логіка виводу інформації користувачу
# pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org tabulate
from tabulate import tabulate


class View:
    def __init__ (self):
        self.name_table = ""
        self.columns = ""
        self.conditions = ""
        self.order_by = ""
        self.number_rows = ""
        self.values = ""

    def get_input(self):
        return input("Choose your option: ")

    def clear_console(self):
        print("\033[2J\033[H", end="")

    def get_types(self, number_table: int, info_list: list):
        return [(types[1] for types in info_list[number_table])]

    def get_names(self, number_table: int, info_list: list):
        return [names[0] for names in info_list[number_table]]

    def print_action_menu(self):
        print("\n----- MENU -----")
        print("1. Get table data")
        print("2. Add new data")
        print("3. Delete data")
        print("4. Update data")
        print("5. Generate random data")
    
    def get_table_description(self):
        self.name_table = input("\nFROM 'table_name': ").strip()
        self.columns = input("SELECT 'columns_names' or 'ALL': ").strip()
        self.conditions = input("WHERE 'conditions' or 'NONE': ").strip()
        self.order_by = input("ORDER BY 'columns_names' or 'NONE': ").strip()
        self.number_rows = input("LIMIT 'number_rows' or 'ALL': ").strip()
        return self.name_table, self.columns, self.number_rows, self.order_by, self.conditions

    def get_generate_info(self):
        return input("Enter number of new data: ").strip()

    def get_ADD_description(self):
        self.name_table = input("\nINSERT INTO 'table_name': ").strip()
        self.values = input("VALUES 'value_1,value_2,...;': ").strip()
        return self.name_table, self.values

    def get_DELETE_description(self):
        self.name_table = input("FROM 'table_name': ").strip()
        self.conditions = input("WHERE 'conditions': ").strip()
        return self.name_table, self.conditions

    def get_UPDATE_description(self):
        self.name_table = input("UPDATE 'table_name': ").strip()
        self.columns = input("SET 'column_1, column_2, column_3, ...': ").strip()
        self.values = input("VALUES 'value_1, value_2, value_3, ...': ").strip()
        self.conditions = input("WHERE 'conditions': ").strip()
        return self.name_table, self.columns, self.values, self.conditions

    def show_base_info(self, info_list: list):
        print("### --- You can choose out of these tables:")
        print("### --- TABLE:  Users")
        print(tabulate(self.get_types(0, info_list), headers = self.get_names(0, info_list), tablefmt = "psql"))
        print("### --- TABLE: Review")
        print(tabulate(self.get_types(1, info_list), headers = self.get_names(1, info_list), tablefmt = "psql"))
        print("### --- TABLE: Realty")
        print(tabulate(self.get_types(2, info_list), headers = self.get_names(2, info_list), tablefmt = "psql"))
        print("### --- TABLE: Property owner")
        print(tabulate(self.get_types(3, info_list), headers = self.get_names(3, info_list), tablefmt = "psql"))
        print("### --- TABLE: Booking")
        print(tabulate(self.get_types(4, info_list), headers = self.get_names(4, info_list), tablefmt = "psql"))
    
    def show_table_data(self, columns_names: list, data_row: list):
        print(f'### --- TABLE: {self.name_table}')
        print(tabulate(data_row, headers = columns_names, tablefmt = "psql"))
        input()
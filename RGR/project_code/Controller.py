# Логіка спілкування з користувачем
from View import View
from Model import Model
from time import sleep


class Controller:
    def __init__ (self):
        self.view = View()
        self.model = Model()
        self.main_input = ""
        self.action_input = ""
    
    def run(self):
        try:
            self.model.start_script_method()
        except:
            print("database is already created\n")
            sleep(3)
        while True:
            try:
                self.view.clear_console()
                self.view.show_base_info(self.model.get_tables_info())
                self.view.print_action_menu()
                self.action_input = self.view.get_input()
                match self.action_input:
                    case "1":
                        self.model.get_table_data(*self.view.get_table_description())
                        self.view.clear_console()
                        self.view.show_table_data(*self.model.analysis_table_data())
                    case "2":
                        self.model.add_table_data(*self.view.get_ADD_description())
                    case "3":
                        self.model.delete_table_data(*self.view.get_DELETE_description())
                    case "4":
                        self.model.update_table_data(*self.view.get_UPDATE_description())
                    case "5":
                        self.model.generate_data(self.view.get_generate_info())
            except:
                print("\n# - Syntax ERROR . . .")
                sleep(2)
                self.model.connect.rollback()

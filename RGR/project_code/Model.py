# Логіка роботи та взаємодії із базою даних
# pip install psycopg
import psycopg

class Model:
    def __init__(self):
        self.connect = psycopg.connect(
            dbname="postgres",
            user="postgres",
            password="11111",
            host="localhost",
            port="5432"
        )
        self.cursor = self.connect.cursor()
        self.types_dict = {
            23: "INT",          # integer
            1043: "CHAR VAR",   # character varying
            1184: "TS WITH TZ"  # timestamp with timezone
        }
        self.search_fk_query = """
        SELECT
            kcu.column_name AS foreign_key_column,
            ccu.table_name AS parent_table,
            ccu.column_name AS parent_column
        FROM
            information_schema.key_column_usage AS kcu
        JOIN
            information_schema.referential_constraints AS rc
                ON kcu.constraint_name = rc.constraint_name
        JOIN
            information_schema.constraint_column_usage AS ccu
                ON rc.unique_constraint_name = ccu.constraint_name
        WHERE
            kcu.table_schema = 'public'
            AND kcu.table_name = %s;
        """
        self.start_script = """
        BEGIN;

        CREATE TABLE IF NOT EXISTS public."Property owner"
        (
        property_owner_id integer NOT NULL,
        first_name character varying(50) NOT NULL,
        last_name character varying(50) NOT NULL,
        data_registration timestamp with time zone NOT NULL,
        email character varying(50) NOT NULL,
        CONSTRAINT property_owner_id_pk PRIMARY KEY (property_owner_id)
        );
        CREATE TABLE IF NOT EXISTS public."Users"
        (
        user_id integer NOT NULL,
        first_name character varying(50) NOT NULL,
        last_name character varying(50) NOT NULL,
        data_registration timestamp with time zone NOT NULL,
        email character varying(50) NOT NULL,
        CONSTRAINT user_id_pk PRIMARY KEY (user_id)
        );
        CREATE TABLE IF NOT EXISTS public."Realty"
        (
        realty_id integer NOT NULL,
        property_owner_id integer NOT NULL,
        city_name character varying(50) NOT NULL,
        street_name character varying(50) NOT NULL,
        type_realty character varying(50) NOT NULL,
        status_realty character varying(50) NOT NULL,
        minimum_rental_period integer NOT NULL,
        deposit integer NOT NULL,
        permitted_conditions character varying(50) NOT NULL,
        price integer NOT NULL,
        payment_term character varying(50) NOT NULL,
        CONSTRAINT realty_id_pk PRIMARY KEY (realty_id)
        );
        CREATE TABLE IF NOT EXISTS public."Review"
        (
        review_id integer NOT NULL,
        user_id integer NOT NULL,
        realty_id integer NOT NULL,
        rating integer NOT NULL,
        CONSTRAINT review_id_pk PRIMARY KEY (review_id)
        );
        CREATE TABLE IF NOT EXISTS public."Booking"
        (
        booking_id integer NOT NULL,
        user_id integer NOT NULL,
        realty_id integer NOT NULL,
        property_owner_id integer NOT NULL,
        data_start timestamp with time zone NOT NULL,
        data_end timestamp with time zone NOT NULL,
        status_booking character varying(50) NOT NULL,
        price_booking integer NOT NULL,
        CONSTRAINT booking_id_pk PRIMARY KEY (booking_id)
        );
        ALTER TABLE IF EXISTS public."Realty"
        ADD CONSTRAINT property_owner_fk FOREIGN KEY (property_owner_id)
        REFERENCES public."Property owner" (property_owner_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

        ALTER TABLE IF EXISTS public."Review"
        ADD CONSTRAINT user_id_fk FOREIGN KEY (user_id)
        REFERENCES public."Users" (user_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

        ALTER TABLE IF EXISTS public."Review"
        ADD CONSTRAINT realty_id_fk FOREIGN KEY (realty_id)
        REFERENCES public."Realty" (realty_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

        ALTER TABLE IF EXISTS public."Booking"
        ADD CONSTRAINT user_id_fk FOREIGN KEY (user_id)
        REFERENCES public."Users" (user_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

        ALTER TABLE IF EXISTS public."Booking"
        ADD CONSTRAINT realty_id_fk FOREIGN KEY (realty_id)
        REFERENCES public."Realty" (realty_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;

        ALTER TABLE IF EXISTS public."Booking"
        ADD CONSTRAINT property_owner_id_fk FOREIGN KEY (property_owner_id)

        REFERENCES public."Property owner" (property_owner_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID;
        END;
        """

    def start_script_method(self):
        self.cursor.execute(self.start_script)
        self.connect.commit()

    def get_table_schema_info(self, table_name: str) -> list:
        command = f'SELECT * FROM "{table_name}" LIMIT 0;'
        self.cursor.execute(command)
        columns_names = [column_info[0] for column_info in self.cursor.description]
        types_row = [self.types_dict.get(column_info[1], "UNKNOWN") for column_info in self.cursor.description]
        return list(zip(columns_names, types_row))

    def get_tables_info(self) -> list:
        table_names = ["Users", "Review", "Realty", "Property owner", "Booking"]
        return [self.get_table_schema_info(name) for name in table_names]

    def generate_data(self, number_of_rows_str: str):
        try:
            num = int(number_of_rows_str)
            if num <= 0:
                print("\n# -> Кількість рядків повинна бути додатним числом.")
                input("Натисніть Enter для продовження...")
                return
        except (ValueError, TypeError):
            print("\n# -> ПОМИЛКА: Введіть коректне число.")
            input("Натисніть Enter для продовження...")
            return
        users_query = """
        INSERT INTO "Users" (user_id, first_name, last_name, data_registration, email)
        SELECT
            row_number() OVER () + (SELECT COALESCE(MAX(user_id), 0) FROM "Users"),
            fname,
            lname,
            NOW() - (random() * interval '365 days'),
            lower(fname || '.' || lname || (random()*1000)::int) || '@generated.com'
        FROM
            (SELECT unnest(array['John', 'Jane', 'Peter', 'Alice', 'Chris', 'Eva']) as fname) f,
            (SELECT unnest(array['Smith', 'Doe', 'Jones', 'Williams', 'Brown', 'Davis']) as lname) l
        ORDER BY random() LIMIT %s;
        """
        
        property_owner_query = """
        INSERT INTO "Property owner" (property_owner_id, first_name, last_name, data_registration, email)
        SELECT
            row_number() OVER () + (SELECT COALESCE(MAX(property_owner_id), 0) FROM "Property owner"),
            fname,
            lname,
            NOW() - (random() * interval '365 days'),
            lower(fname || '.' || lname || (random()*1000)::int) || '@owner.com'
        FROM
            (SELECT unnest(array['Michael', 'Sarah', 'David', 'Laura', 'James', 'Linda']) as fname) f,
            (SELECT unnest(array['Miller', 'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas']) as lname) l
        ORDER BY random() LIMIT %s;
        """

        realty_query = """
        INSERT INTO "Realty" (realty_id, property_owner_id, city_name, street_name, type_realty, status_realty,
                              minimum_rental_period, deposit, permitted_conditions, price, payment_term)
        SELECT
            n + (SELECT COALESCE(MAX(realty_id), 0) FROM "Realty"),
            (SELECT property_owner_id FROM "Property owner" ORDER BY random() LIMIT 1),
            (SELECT city FROM unnest(array['Kyiv', 'Lviv', 'Odesa', 'Kharkiv']) city ORDER BY random() LIMIT 1),
            (SELECT street FROM unnest(array['Main St', 'Oak Ave', 'Pine Ln', 'Maple Dr']) street ORDER BY random() LIMIT 1),
            (SELECT type FROM unnest(array['Apartment', 'House', 'Cottage']) type ORDER BY random() LIMIT 1),
            (SELECT status FROM unnest(array['Available', 'Rented', 'Under repair']) status ORDER BY random() LIMIT 1),
            (1 + random() * 11)::int,
            (1000 + random() * 4000)::int,
            (SELECT cond FROM unnest(array['Pets allowed', 'No smoking', 'Families only']) cond ORDER BY random() LIMIT 1),
            (5000 + random() * 15000)::int,
            (SELECT term FROM unnest(array['Monthly', 'Quarterly']) term ORDER BY random() LIMIT 1)
        FROM generate_series(1, %s) as s(n);
        """

        review_query = """
        INSERT INTO "Review" (review_id, user_id, realty_id, rating)
        SELECT
            n + (SELECT COALESCE(MAX(review_id), 0) FROM "Review"),
            (SELECT user_id FROM "Users" ORDER BY random() LIMIT 1),
            (SELECT realty_id FROM "Realty" ORDER BY random() LIMIT 1),
            (1 + random() * 4)::int
        FROM generate_series(1, %s) as s(n);
        """
        
        booking_query = """
        INSERT INTO "Booking" (booking_id, user_id, realty_id, property_owner_id, data_start, data_end, 
                               status_booking, price_booking)
        SELECT
            row_number() OVER () + (SELECT COALESCE(MAX(booking_id), 0) FROM "Booking"),
            (SELECT user_id FROM "Users" ORDER BY random() LIMIT 1),
            realty.realty_id,
            realty.property_owner_id,
            NOW() + (random() * interval '10 days'),
            NOW() + (random() * interval '20 days' + interval '11 days'),
            (SELECT status FROM unnest(array['Confirmed', 'Pending', 'Cancelled']) status ORDER BY random() LIMIT 1),
            (5000 + random() * 15000)::int
        FROM "Realty" as realty
        ORDER BY random() LIMIT %s;
        """

        queries_in_order = [
            ("Users", users_query),
            ("Property owner", property_owner_query),
            ("Realty", realty_query),
            ("Review", review_query),
            ("Booking", booking_query)
        ]

        try:
            print("\n# -> Починаю генерацію даних...")
            for table_name, query in queries_in_order:
                if table_name in ["Realty", "Review", "Booking"]:
                    parent_check = {
                        "Realty": ('SELECT 1 FROM "Property owner" LIMIT 1', "Property owner"),
                        "Review": ('SELECT 1 FROM "Users" LIMIT 1', "Users"),
                        "Booking": ('SELECT 1 FROM "Realty" LIMIT 1', "Realty")
                    }
                    if table_name in parent_check:
                        check_q, parent_name = parent_check[table_name]
                        self.cursor.execute(check_q)
                        if self.cursor.fetchone() is None:
                            print(f"# -> Пропускаю \"{table_name}\", оскільки батьківська таблиця \"{parent_name}\" порожня.")
                            continue

                print(f"# -> Заповнюю таблицю \"{table_name}\"...")
                self.cursor.execute(query, (num,))
            
            self.connect.commit()
            print("\n# -> Успішно згенеровано та додано дані до всіх таблиць!")
        
        except psycopg.Error as e:
            print(f"\n# -> СТАЛАСЯ ПОМИЛКА: {e.diag.message_primary}")
            print("# -> Відкочую всі зміни, щоб зберегти цілісність бази даних.")
            self.connect.rollback()
        
        input("Натисніть Enter для продовження...")

    def _insert_data_safely(self, table_name: str, data: list):
        schema_info = self.get_table_schema_info(table_name)
        columns = [col[0] for col in schema_info]
        cols_str = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({placeholders})'
        try:
            self.cursor.executemany(insert_query, data)
            self.connect.commit()
            print("\n# -> Дані успішно додано!")
        except Exception as e:
            print(f"\n# -> Помилка під час вставки: {e}")
            self.connect.rollback()
        input("Натисніть Enter для продовження...")

    def add_table_data(self, name_table: str, values: str):
        schema_info = self.get_table_schema_info(name_table)
        columns_names = [col[0] for col in schema_info]
        # Конвертуємо вхідний рядок у список кортежів
        try:
            data_to_insert = [tuple(row.split(',')) for row in values.strip(';').split(';')]
        except ValueError:
            print("\n# -> Помилка формату вхідних даних.")
            input("Натисніть Enter для продовження...")
            return
        # Знаходимо всі зовнішні ключі для цієї таблиці
        self.cursor.execute(self.search_fk_query, (name_table,))
        foreign_keys = self.cursor.fetchall()
        if not foreign_keys:
            # Якщо зовнішніх ключів немає, просто вставляємо дані
            self._insert_data_safely(name_table, data_to_insert)
            return
        valid_rows_to_insert = []
        for row_data in data_to_insert:
            is_row_valid = True
            for fk in foreign_keys:
                fk_column_name, parent_table, parent_column = fk
                try:
                    # Знаходимо індекс стовпця, що є зовнішнім ключем
                    fk_index = columns_names.index(fk_column_name)
                    value_to_check = row_data[fk_index]
                    
                    # Безпечний запит для перевірки існування батьківського запису
                    check_query = f'SELECT 1 FROM "{parent_table}" WHERE "{parent_column}" = %s LIMIT 1;'
                    self.cursor.execute(check_query, (value_to_check,))
                    
                    if self.cursor.fetchone() is None:
                        print(f'\n# -> ПОМИЛКА: Значення "{value_to_check}" для стовпця "{fk_column_name}"')
                        print(f'# -> не існує в батьківській таблиці "{parent_table}". Рядок не буде додано.')
                        is_row_valid = False
                        break  # Перериваємо перевірку для поточного рядка
                except ValueError:
                    print(f"# -> КРИТИЧНА ПОМИЛКА: Стовпець {fk_column_name} не знайдено в таблиці {name_table}")
                    is_row_valid = False
                    break
            
            if is_row_valid:
                valid_rows_to_insert.append(row_data)

        if valid_rows_to_insert:
            self._insert_data_safely(name_table, valid_rows_to_insert)
        else:
            print("\n# -> Жодного рядка не було додано через помилки валідації.")
            input("Натисніть Enter для продовження...")

    def delete_table_data(self, name_table: str, conditions: str):
        # Перевіряємо, чи умови не порожні
        if not conditions.strip():
            print("\n# -> ПОМИЛКА: Умови для видалення не можуть бути порожніми!")
            input("Натисніть Enter для продовження...")
            return
        # Створюємо базову команду DELETE
        command = f'DELETE FROM "{name_table}" WHERE {conditions}'
        try:
            # Особлива логіка ТІЛЬКИ для батьківської таблиці "Realty"
            if name_table == "Realty":
                # 1. Знайти ID об'єктів нерухомості, які підпадають під умови видалення
                select_ids_query = f'SELECT "realty_id" FROM "Realty" WHERE {conditions}'
                self.cursor.execute(select_ids_query)
                realty_ids_to_delete = self.cursor.fetchall()
                if not realty_ids_to_delete:
                    print("\n# -> Жодного запису не знайдено за вашими умовами.")
                    input("Натисніть Enter для продовження...")
                    return
                # Перетворюємо список кортежів [(1,), (2,)] у простий список [1, 2]
                ids_list = [item[0] for item in realty_ids_to_delete]
                # 2. Перевірити, чи існують посилання на ці ID в дочірніх таблицях
                # Перевірка в таблиці "Review"
                check_review_query = 'SELECT 1 FROM "Review" WHERE "realty_id" = ANY(%s) LIMIT 1'
                self.cursor.execute(check_review_query, (ids_list,))
                if self.cursor.fetchone():
                    print(f'\n# -> ПОМИЛКА: Неможливо видалити. Існують відгуки, пов\'язані з цією нерухомістю.')
                    self.connect.rollback()
                    input("Натисніть Enter для продовження...")
                    return
                # Перевірка в таблиці "Booking"
                check_booking_query = 'SELECT 1 FROM "Booking" WHERE "realty_id" = ANY(%s) LIMIT 1'
                self.cursor.execute(check_booking_query, (ids_list,))
                if self.cursor.fetchone():
                    print(f'\n# -> ПОМИЛКА: Неможливо видалити. Існують бронювання, пов\'язані з цією нерухомістю.')
                    self.connect.rollback()
                    input("Натисніть Enter для продовження...")
                    return
            # 3. Виконуємо видалення (для "Realty" - після перевірок, для інших - одразу)
            self.cursor.execute(command)
            # Перевіряємо, чи було щось видалено
            if self.cursor.rowcount > 0:
                print(f"\n# -> Успішно видалено {self.cursor.rowcount} рядків.")
                self.connect.commit() # Підтверджуємо транзакцію
            else:
                print("\n# -> Жодного рядка не було видалено (можливо, умови не знайшли збігів).")
                self.connect.rollback()
        except psycopg.Error as e:
            print(f"\n# -> Сталася помилка бази даних: {e}")
            self.connect.rollback() # Відкочуємо зміни у разі будь-якої помилки
        input("Натисніть Enter для продовження...")

    def update_table_data(self, name_table: str, columns: str, values: str, conditions: str):
        # 1. Валідація та парсинг вхідних даних
        if not all([name_table, columns, values, conditions]):
            print("\n# -> ПОМИЛКА: Всі поля (таблиця, стовпці, значення, умови) мають бути заповнені!")
            input("Натисніть Enter для продовження...")
            return

        cols_list = [c.strip() for c in columns.split(',')]
        vals_list = [v.strip() for v in values.split(',')]

        if len(cols_list) != len(vals_list):
            print("\n# -> ПОМИЛКА: Кількість стовпців не відповідає кількості значень!")
            input("Натисніть Enter для продовження...")
            return
        try:
            # 2. Перевірка цілісності даних ПЕРЕД виконанням запиту
            self.cursor.execute(self.search_fk_query, (name_table,))
            foreign_keys = {fk[0]: (fk[1], fk[2]) for fk in self.cursor.fetchall()}
            for i, col_name in enumerate(cols_list):
                if col_name in foreign_keys:
                    parent_table, parent_column = foreign_keys[col_name]
                    value_to_check = vals_list[i]
                    # Перевіряємо, чи існує нове значення в батьківській таблиці
                    check_query = f'SELECT 1 FROM "{parent_table}" WHERE "{parent_column}" = %s LIMIT 1;'
                    self.cursor.execute(check_query, (value_to_check,))
                    if self.cursor.fetchone() is None:
                        print(f'\n# -> ПОМИЛКА: Неможливо оновити стовпець "{col_name}".')
                        print(f'# -> Значення "{value_to_check}" не існує в батьківській таблиці "{parent_table}".')
                        self.connect.rollback()
                        input("Натисніть Enter для продовження...")
                        return
            set_clause = ", ".join([f'"{col}" = %s' for col in cols_list])
            command = f'UPDATE "{name_table}" SET {set_clause} WHERE {conditions}'
            # Передаємо значення для SET як параметри, щоб уникнути ін'єкцій
            self.cursor.execute(command, tuple(vals_list))
            if self.cursor.rowcount > 0:
                print(f"\n# -> Успішно оновлено {self.cursor.rowcount} рядків.")
                self.connect.commit()
            else:
                print("\n# -> Жодного рядка не було оновлено (умови не знайшли збігів).")
                self.connect.rollback()
        except psycopg.Error as e:
            self.connect.rollback()
            # Надаємо користувачу зрозуміле повідомлення про помилку
            if e.pgcode == '23503': # Код помилки для foreign_key_violation
                 print(f"\n# -> ПОМИЛКА ОНОВЛЕННЯ: Неможливо змінити цей запис,")
                 print(f"# -> оскільки на нього посилаються дані в інших таблицях (наприклад, бронювання або відгуки).")
            else:
                 print(f"\n# -> Сталася помилка бази даних: {e}")
        input("Натисніть Enter для продовження...")

    def get_table_data(self, name_table: str, columns: str, number_rows: str, order: str, conditions: str):
        selected_columns = columns if columns != "ALL" else "*"
        command = f'SELECT {selected_columns} FROM "{name_table}"'
        if conditions != "NONE":
            command += f' WHERE {conditions}'
        if order != "NONE":
            command += f' ORDER BY {order}'
        if number_rows != "ALL":
            if number_rows.isdigit():
                command += f' LIMIT {number_rows}'
        
        self.cursor.execute(command)

    def analysis_table_data(self) -> tuple:
        if not self.cursor.description:
            return [], []
        columns_names = [column_info[0] for column_info in self.cursor.description]
        types_row = [self.types_dict.get(column_info[1], "UNKNOWN") for column_info in self.cursor.description]
        gotten_data = self.cursor.fetchall()
        final_data_rows = [tuple(types_row)] + gotten_data
        return columns_names, final_data_rows
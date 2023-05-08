import psycopg2


class CreateBD:

    def __init__(self, database_name):
        self.database_name = database_name

    def create_db(self, params):
        """
        Создание базы данных
        :param params: параметры для подключения к PostgreSQL
        :return: None
        """
        conn = psycopg2.connect(dbname='postgres', **params)
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(f'DROP DATABASE {self.database_name}')
            cur.execute(f'CREATE DATABASE {self.database_name}')
        except psycopg2.errors.ObjectInUse:
            cur.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = {self.database_name}
            AND pid <> pg_backend_pid();
                """)
            cur.execute(f'DROP DATABASE {self.database_name}')
            cur.execute(f'CREATE DATABASE {self.database_name}')
        except psycopg2.errors.InvalidCatalogName:
            cur.execute(f'CREATE DATABASE {self.database_name}')

        conn.close()

    def create_vac_table(self, params):
        """
        Создание таблиц в БД
        :param params: параметры для подключения к PostgreSQL
        :return: None
        """
        with psycopg2.connect(dbname=self.database_name, **params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE employer (
                company_id int PRIMARY KEY,
                employer_name varchar(100)
                )
                """)

            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE vacancies (
                company_id int PRIMARY KEY REFERENCE employer(company_id),
                vacancy_name varchar(100),
                salary_from int,
                salary_to int,
                salary_currency varchar(10),
                responsibility text
                )
                """)

    def insert_table(self, data, **params):
        """
        Заполнение таблиц данными
        :param data: response по API - вакансии по определенным параметрам с hh.ru
        :param params: параметры для подключения к PostgreSQ
        :return: None
        """
        with psycopg2.connect(dbname=self.database_name, **params) as conn:
            with conn.cursor() as cur:
                for employ in data:
                    employer_data = employ['employer']
                    cur.execute(
                        """
                        INSERT INTO employer (company_id, employer_name)
                        VALUES (%s, %s)
                        RETURNING company_id
                        """,
                        (employer_data['id'], employer_data['name'])
                    )

            with conn.cursor() as cur:
                for vacancy in data:
                    cur.execute(
                        """
                        INSERT INTO vacancies (company_id, vacancy_name, salary_from, salary_to, salary_currency, 
                        responsibility)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (vacancy['employer']['id'], vacancy['name'], vacancy['salary']['from'], vacancy['salary']['to'],
                         vacancy['salary']['currency'], vacancy['snippet']['responsibility'])
                    )

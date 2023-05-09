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
                cur.execute(f'DROP TABLE vacancies')
                cur.execute("""
                CREATE TABLE vacancies (        
                company_id int,
                employer_name varchar(100),
                vacancy_name varchar(100),
                salary_from int,
                salary_to int,
                salary_currency varchar(10),
                responsibility text,
                url text
                )
                """)

    def insert_table(self, data: dict, params):
        """
        Заполнение таблиц данными
        :param data: response по API - вакансии по определенным параметрам с hh.ru
        :param params: параметры для подключения к PostgreSQ
        :return: None
        """
        with psycopg2.connect(dbname=self.database_name, **params) as conn:
            with conn.cursor() as cur:
                for vacancy in data:
                    cur.execute(
                        """
                        INSERT INTO vacancies (company_id, employer_name, vacancy_name, salary_from, salary_to, 
                        salary_currency, responsibility, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (vacancy['employer']['id'], vacancy['employer']['name'], vacancy['name'],
                         vacancy['salary']['from'], vacancy['salary']['to'],
                         vacancy['salary']['currency'], vacancy['snippet']['responsibility'], vacancy['alternate_url'])
                    )


class DBManager:

    def get_companies_and_vacancies_count(self, database_name, params):
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        :return:
        """
        with psycopg2.connect(dbname=database_name, **params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT employer_name, COUNT(*)
                FROM vacancies
                GROUP BY employer_name
                """)
                return cur.fetchall()

    def get_all_vacancies(self, database_name, limit, params):
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        :return:
        """
        with psycopg2.connect(dbname=database_name, **params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT employer_name, vacancy_name, salary_from, salary_to, url
                FROM vacancies
                LIMIT {limit}
                """)
                return cur.fetchall()

    def get_avg_salary(self, database_name, params):
        """
        Получает среднюю зарплату по вакансиям.
        :return:
        """
        with psycopg2.connect(dbname=database_name, **params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT AVG(salary_from)
                FROM vacancies
                WHERE salary_from IS NOT NULL
                """)
                return int(*cur.fetchone())

    def get_vacancies_with_higher_salary(self, database_name, params):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        :return:
        """
        with psycopg2.connect(dbname=database_name, **params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT vacancy_name, salary_from
                FROM vacancies
                WHERE salary_from >
                (
                    SELECT AVG(salary_from) FROM vacancies
                    WHERE salary_from IS NOT NULL
                )
                """)
                return cur.fetchall()

    def get_vacancies_with_keyword(self, database_name, keyword, params):
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”.
        :return:
        """
        with psycopg2.connect(dbname=database_name, **params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                SELECT vacancy_name, salary_from
                FROM vacancies
                WHERE vacancy_name LIKE '%{keyword}%'
                """)
                return cur.fetchall()

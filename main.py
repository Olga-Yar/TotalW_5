import psycopg2
from psycopg2 import errors

from scr.config import config
from scr.data_psg import CreateBD, DBManager
from scr.item import APIKey


def main():
    employers_id = [
        5923,    # Ренессанс Банк
        586,    # Банк Русский стандарт
        3529,    # СБЕР
        3776,    # ПАО МТС
        11680,   # АО ДОМ.РФ
        1740,    # Яндекс
        10156,   # Интерфакс
        1102601,  # Группа Самолет
        2863076,  # Skillbox
        8620,    # Rambler&Co
        15478    # VK
    ]
    params = config()
    keyword = input('Введите слово для поиска: ').title()
    db_name = input('Введите название БД для хранения вакансий: ').lower()

    base = CreateBD(db_name)  # создание экземпляра класса для создания БД
    try:                      # проверка наличия БД - при отсутствии - создание, при наличии - пропуск действия
        base.create_db(params)
    except psycopg2.errors.DuplicateDatabase:
        pass

    base.create_vac_table(params)   # создание таблицы в БД

    vac = APIKey()  # создание экземпляра класса для работы с АПИ ключом

    for i in range(5):
        data = vac.api(keyword, employers_id)     # подключение к hh.ru по ключу
        base.insert_table(data, params)           # заполнение данными таблицы

    user_vacancy_count = input('Вывести список всех компаний и количество вакансий у каждой компании: [Y / N]')
    user = DBManager()
    if user_vacancy_count.capitalize() == 'Y':
        vacancy = user.get_companies_and_vacancies_count(db_name, params)
        for row in vacancy:
            print('название компании: ', row[0])
            print('количество вакансий: ', row[1])

    user_all_vac = input('Список всех вакансий с указанием названия компании, названия вакансии и зарплаты,'
                         ' и ссылки на вакансию: [Y / N]')
    if user_all_vac.capitalize() == 'Y':
        vacancy = user.get_all_vacancies(db_name, 2, params)
        for row in vacancy:
            print('компания: ', row[0])
            print('вакансия: ', row[1])
            print('зарплата от: ', row[2])
            print('зарплата до: ', row[3])
            print('ссылка на вакансию: ', row[4])

    user_avg = input('Вывести среднюю зарплату по вакансиям: [Y / N]')
    if user_avg.capitalize() == 'Y':
        vacancy = user.get_avg_salary(db_name, params)
        print(vacancy)

    user_high_salary = input('Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям: [Y / N]')
    if user_high_salary.capitalize() == 'Y':
        vacancy = user.get_vacancies_with_higher_salary(db_name, params)
        for row in vacancy:
            print('вакансия: ', row[0])
            print('зарплата от: ', row[1])

    user_vac_keyword = input('Вывести список всех вакансий, в названии которых содержится слово: ')
    vacancy = user.get_vacancies_with_keyword(db_name, user_vac_keyword, params)
    for row in vacancy:
        print('вакансия: ', row[0])
        print('обязанности: ', row[1])


if __name__ == '__main__':
    main()

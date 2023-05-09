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

    # Взаимодействие с пользователем

    user_vacancy_count = input('Вывести список всех компаний и количество вакансий у каждой компании: [Y / N]\n')
    user = DBManager()
    if user_vacancy_count.capitalize() == 'Y':
        vacancy = user.get_companies_and_vacancies_count(db_name, params)
        for row in vacancy:
            print(row[0], row[1], 'шт.')

    user_all_vac = input('\nСписок всех вакансий с указанием названия компании, названия вакансии и зарплаты,'
                         ' и ссылки на вакансию: [Y / N]\n')
    if user_all_vac.capitalize() == 'Y':
        limit = int(input('Сколько вакансий вывести: '))
        vacancy = user.get_all_vacancies(db_name, limit, params)
        for row in vacancy:
            print('__________________')
            print('\nкомпания: ', row[0])
            print('вакансия: ', row[1])
            print('зарплата от: ', row[2])
            print('зарплата до: ', row[3])
            print('ссылка на вакансию: ', row[4])

    user_avg = input('\nВывести среднюю зарплату по вакансиям: [Y / N]\n')
    if user_avg.capitalize() == 'Y':
        vacancy = user.get_avg_salary(db_name, params)
        print(vacancy)

    user_high_salary = input('\nВывести список всех вакансий, у которых зарплата выше средней по всем вакансиям:'
                             ' [Y / N]\n')
    if user_high_salary.capitalize() == 'Y':
        vacancy = user.get_vacancies_with_higher_salary(db_name, params)
        for row in vacancy:
            print('__________________')
            print('вакансия: ', row[0])
            print('зарплата от: ', row[1])

    user_vac_keyword = input('\nВывести список всех вакансий, в названии которых содержится слово: \n')
    vacancy = user.get_vacancies_with_keyword(db_name, user_vac_keyword, params)
    for row in vacancy:
        print('__________________')
        print('\nвакансия: ', row[0])
        print('зарплата от: ', row[1])


if __name__ == '__main__':
    main()

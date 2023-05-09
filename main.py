from scr.config import config
from scr.data_psg import CreateBD
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
    vac = APIKey()
    base = CreateBD('vacancy_hh')
    # base.create_db(params)

    base.create_vac_table(params)

    data = vac.api('Python', employers_id)
    base.insert_table(data, params)
    #
    # while True:
    #     for i in range(2):
    #         data = vac.api('Python', employers_id)
    #         base.insert_table(data, params)






if __name__ == '__main__':
    main()

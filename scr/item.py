import requests


class APIKey:
    def api(self, keyword, employers_id):
        """
        Подключение по API к hh.ru
        :return: вакансии по определенным критериям
        """
        url_api = 'https://api.hh.ru/vacancies'
        params = {
            "employer_id": tuple(employers_id),
            "only_with_salary": True,
            "currency": "RUR",
            "text": keyword,
            "per_page": 100,
            "area": 113,
            "page": 0
        }

        if requests.get(url_api, params=params).status_code == 200:
            return requests.get(url_api, params=params).json()['items']
        else:
            return f'Error: {requests.get(url_api, params=params).status_code}'


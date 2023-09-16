import psycopg2 as psycopg2
import requests
import tqdm as tqdm
from config import config


class DBManager:

    def __init__(self, keyword: str):
        self.url = 'https://api.hh.ru/employers'
        self.companies = [
            'Яндекс',
            'VK',
            'Мегафон',
            '1С',
            'Ростелеком',
            'Тинькофф',
            'МТС',
            'Сбер',
            'Газпром',
            'Skyeng'
        ]
        self.params_db = config()
        self.database_name = 'vacancies'
        self.keyword = keyword

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        companies_data = []
        for employer in self.companies:
            params_api = {
                'text': employer,
                'sort_by': 'by_vacancies_open',
                'per_page': 5
            }
            response = requests.get(self.url, params=params_api)
            companies_data.extend(response.json()['items'])

        conn = psycopg2.connect(dbname='postgres', **self.params_db)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE IF EXISTS {self.database_name}')
        cur.execute(f'CREATE DATABASE {self.database_name}')
        cur.close()
        conn.close()

        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE companies (
                    companies_id SERIAL PRIMARY KEY,
                    company_name VARCHAR NOT NULL,
                    vacancies_count INT,
                    vacancies_url VARCHAR(100)
                )
            """)

        with conn.cursor() as cur:
            for company in companies_data:
                if company['open_vacancies'] > 1000:
                    cur.execute(
                        """
                        INSERT INTO companies (company_name, vacancies_count, vacancies_url)
                        VALUES (%s, %s, %s)
                        """,
                        (company['name'], company['open_vacancies'], company['vacancies_url'])
                    )
        conn.commit()
        conn.close()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vacancies_url FROM companies
            """)
            companies = cur.fetchall()
        conn.commit()
        conn.close()

        vacancies_data = []
        for company in tqdm.tqdm(companies, desc='Идет загрузка вакансий: '):
            for page in range(10):
                params_api = {
                    'per_page': 100,
                    'page': page,
                    'archived': False,
                    'only_with_salary': True
                }
                response = requests.get(company[0], params=params_api)
                if not response.json()['items']:
                    break
                vacancies_data.extend(response.json()['items'])
        print(f'Загружено {len(vacancies_data)} вакансий')

        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE vacancies (
                            vacancies_id SERIAL PRIMARY KEY,
                            company_name VARCHAR NOT NULL,
                            vacancy_name VARCHAR NOT NULL,
                            salary_from INT,
                            vacancies_url VARCHAR(100)
                        )
                    """)
        conn.commit()
        conn.close()

        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        with conn.cursor() as cur:
            for vacancies in vacancies_data:
                if vacancies['salary']['currency'] == 'RUR':
                    cur.execute(
                        """
                        INSERT INTO vacancies (company_name, vacancy_name, salary_from, vacancies_url)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (vacancies['employer']['name'],
                         vacancies['name'],
                         vacancies['salary']['from'],
                         vacancies['alternate_url'])
                    )
        conn.commit()
        conn.close()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT AVG(salary_from) FROM vacancies
            """)
            avg_salary = cur.fetchone()
        conn.commit()
        conn.close()
        print(f'Средняя зарплата по вакансиям: {int(avg_salary[0])} руб')

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vacancy_name, salary_from FROM vacancies
                WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies)
            """)
            best_vacancies = cur.fetchall()
        conn.commit()
        conn.close()
        print(f'Вакансий с зарплатой выше средней: {len(best_vacancies)} шт')

    def get_vacancies_with_keyword(self):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        conn = psycopg2.connect(dbname=self.database_name, **self.params_db)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT vacancy_name, salary_from, company_name, vacancies_url FROM vacancies
                WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies)
                AND LOWER(vacancy_name) LIKE LOWER('%{self.keyword}%')
                ORDER BY salary_from DESC
            """)
            vacancies_by_keyword = cur.fetchall()
        conn.commit()
        conn.close()
        if len(vacancies_by_keyword) != 0:
            print(f'Вакансии, найденные по ключевому слову "{self.keyword}", с зарплатой выше средней:')
            print(f'|{"№ п/п":^5}|{"Наименование вакансии":^100}|{"Зарплата":^8}|'
                  f'{"Наименование компании":^26}|{"Ссылка на вакансию":^31}|')
            i = 0
            for vacancy in vacancies_by_keyword:
                i += 1
                print(f'|{i:^5}|{vacancy[0]:<100}|{vacancy[1]:^8}|{vacancy[2]:<26}|{vacancy[3]:^31}|')
        else:
            print(f'Вакансий по ключевому слову "{self.keyword}" не найдено')

from utils import DBManager


if __name__ == '__main__':
    keyword = input('Введите ключевое слово для фильтрации вакансий: ')
    vac = DBManager(keyword)
    vac.get_companies_and_vacancies_count()
    vac.get_all_vacancies()
    vac.get_avg_salary()
    vac.get_vacancies_with_higher_salary()
    vac.get_vacancies_with_keyword()

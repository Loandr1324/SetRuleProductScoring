# Author Loik Andrey mail: loikand@mail.ru
from config import FILE_NAME_LOG
from google_table.google_tb_work import WorkGoogle
from loguru import logger


# Задаём параметры логирования
logger.add(FILE_NAME_LOG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 week",
           compression="zip")


def filtered_products_by_flag(products: list[dict]):
    """
    Получаем позиции для выбора правила проценки
    :param products: list[dict]
    В передаваемых в списке словарей обязательно наличие ключа 'select_flag'
    :return:
    """
    filtered_products = [product for product in products if product['select_flag'] == '1']
    return filtered_products


def selected_rule_for_position(products: list[dict], rules: list[dict]) -> list[dict]:
    """
    Выбираем правило для каждой позиции
    :param rules: В словарях обязательно наличие ключей 'brand', 'id_rule', 'type_rule'
    :param products: В словарях обязательно наличие ключей 'brand', 'id_rule'
    :return: Тот же список с установленным идентификатором выбранного правила в ключе словаря 'id_rule'
    """
    for product in products:
        filtered_rule = [rule for rule in rules if rule['brand'].upper() == product['brand'].upper()]
        if not filtered_rule:
            filtered_rule = [rule for rule in rules if rule['type_rule'] == 'общее']
        if filtered_rule and len(filtered_rule) == 1:
            product['id_rule'] = filtered_rule[0]['id_rule']
        elif len(filtered_rule) > 1:
            logger.error(f"Не определено уникальное правило для позиции {product}")
            logger.error(f"Получены правила: {filtered_rule}")
        else:
            logger.error(f"Не определено ни одного правила для позиции {product}")
    return products


def main():
    """
    Основной процесс программы
    :return:
    """
    logger.info(f"... Запуск программы")
    # Получаем данные из Google таблицы
    wk_g = WorkGoogle()
    products = wk_g.get_products()
    count_row = len(products)
    logger.info(f"Всего позиций на листе: {count_row}")
    products = filtered_products_by_flag(products)
    logger.info(f"Позиций для выбора правил: {len(products)}")

    # Получаем правила для проценки
    rules = wk_g.get_price_filter_rules()

    # Подставляем ID правила для отфильтрованных позиций
    products = selected_rule_for_position(products, rules)

    # Записываем в Google таблице данные по выбранным позициям
    wk_g.set_selected_products(products, count_row, 'K')
    logger.info(f"... Окончание работы программы")


if __name__ == "__main__":
    main()
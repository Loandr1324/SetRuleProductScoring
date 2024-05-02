import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import AUTH_GOOGLE
from loguru import logger
from datetime import datetime as dt


class RWGoogle:
    """
    Класс для чтения и запись данных из(в) Google таблицы(у)
    """
    def __init__(self):
        self.client_id = AUTH_GOOGLE['GOOGLE_CLIENT_ID']
        self.client_secret = AUTH_GOOGLE['GOOGLE_CLIENT_SECRET']
        self._scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self._credentials = ServiceAccountCredentials.from_json_keyfile_name(
            'google_table/credentials.json', self._scope
            # 'credentials.json', self._scope
        )
        self._credentials._client_id = self.client_id
        self._credentials._client_secret = self.client_secret
        self._gc = gspread.authorize(self._credentials)
        self.key_wb = AUTH_GOOGLE['KEY_WORKBOOK']

    def read_sheets(self) -> list[str]:
        """
        Получает данные по всем страницам Google таблицы и возвращает список страниц в виде списка строк
        self.key_wb: id google таблицы.
            Идентификатор таблицы можно найти в URL-адресе таблицы.
            Обычно идентификатор представляет собой набор символов и цифр
            после `/d/` и перед `/edit` в URL-адресе таблицы.
        :return: list[str].
            [
            'Имя 1-ой страницы',
            'Имя 2-ой страницы',
            ...
            'Имя последней страницы'
            ]
        """
        result = []
        try:
            worksheets = self._gc.open_by_key(self.key_wb).worksheets()
            result = [worksheet.title for worksheet in worksheets]
        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")
        except Exception as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")
        return result

    def read_sheet(self, worksheet_id: int) -> list[list[str]]:
        """
        Получает данные из страницы Google таблицы по её идентификатору и возвращает значения в виде списка списков
        self.key_wb: id google таблицы.
            Идентификатор таблицы можно найти в URL-адресе таблицы.
            Обычно идентификатор представляет собой набор символов и цифр
            после `/d/` и перед `/edit` в URL-адресе таблицы.
        :return: List[List[str].
        """
        sheet = []
        try:
            sheet = self._gc.open_by_key(self.key_wb).get_worksheet(worksheet_id)
        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка при получении списка настроек: {e}")
        except Exception as e:
            logger.error(f"Ошибка при получении списка имён страниц: {e}")
        return sheet.get_all_values()

    def save_cell(self, worksheet_id: int, row: int, col: int, value: str):
        """Записываем данные в ячейку"""
        try:
            sheet = self._gc.open_by_key(self.key_wb).get_worksheet(worksheet_id)
            return sheet.update_cell(row, col, value)

        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка записи в ячейку: {e}")

        except Exception as e:
            logger.error(f"ООшибка записи в ячейку: {e}")

    def save_batch(self, worksheet_id: int, values: list[dict]):
        """
        Записываем данные в разные ячейки
        :param worksheet_id: Номер вкладки
        :param values: [
            {'range': 'A1', 'values': [['Значение 1']]},
            {'range': 'B1', 'values': [['Значение 2']]},
            {'range': 'C1', 'values': [['Значение 3']]}
        ]
        :return:
        """
        try:
            sheet = self._gc.open_by_key(self.key_wb).get_worksheet(worksheet_id)
            return sheet.batch_update(values)

        except gspread.exceptions.APIError as e:
            logger.error(f"Ошибка записи в ячейки: {e}")

        except Exception as e:
            logger.error(f"Ошибка записи в ячейки: {e}")


class WorkGoogle:
    def __init__(self):
        self._rw_google = RWGoogle()

    def get_products(self) -> list[dict]:
        """
        Получаем вторую строку с первой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: list[dict
            ключи словаря {
            'number' - Код детали,
            'brand' - Имя производителя
            'description' - Описание детали
            'stock' - Наличие
            'price' - Цена
            'updated_date' - Дата последнего получения цены
            'turn_ratio' - Коэффициент оборачиваемости
            'norm_stock' - Норма наличия
            'product_group' - Товарная группа
            'rule' - Правило выбора цены
            'select_flag' - Отбор для получения цены
            'id_rule' - ID правила для получения цены
            },...]
        """
        sheet_products = self._rw_google.read_sheet(0)
        params_head = ['number', 'brand', 'description', 'stock', 'price', 'updated_date', 'turn_ratio',
                       'norm_stock', 'product_group','rule', 'select_flag', 'id_rule']
        products = []
        for i, val in enumerate(sheet_products[1:], start=2):
            product = dict(zip(params_head, val))
            product['updated_date'] = self.convert_date(str(product['updated_date']))
            product['row_product_on_sheet'] = i
            products.append(product)
        return products

    def get_rule_for_selected_products(self) -> dict:
        """
        Получаем правила выбора позиций продукта со второй страницы Google таблицы
        :return: ключи словаря {
            'count_products' - Количество продуктов для отбора,
            'days_interval' - Количество дней от текущей даты, чтобы считать цену устаревшей
            }
        """
        sheet_rule = self._rw_google.read_sheet(1)
        return {'count_products': int(sheet_rule[1][2]), 'days_interval': int(sheet_rule[2][2])}

    def get_price_filter_rules(self) -> list[dict]:
        """
        Получаем вторую строку с первой страницы и возвращаем их в словаре с предварительно заданными ключами
        :return: list[dict
            ключи словаря {
            'id_rule' - ID правила,
            'type_rule' - Тип правила,
            'rule_value' - Значение, которое используется при выборе правила,
            'type_select_supplier' - Тип отбора поставщиков,
            'id_suppliers' - Идентификаторы поставщиков,
            'type_select_routes' - Тип отбора маршрута,
            'name_routes' - Наименование маршрутов,
            'type_select_supplier_storage' - Тип отбора складов,
            'supplier_storage' - Склады,
            'supplier_storage_min_stock' - Минимальный запас товаров у поставщика,
            'delivery_probability' - Вероятность поставки,
            'max_delivery_period' - Максимальный период поставки,
            'type_selection_rule' - Тип правила отбора позиций,
            'selection_rule' - Правила отбора позиций
            },...]
        `ID правила`, `тип правила`, `значение правила`, `тип отбора поставщиков`, `поставщики`, `тип отбора маршрута`,
        `маршруты`, `тип отбора складов`, `склады`, `минимальный остаток`, `вероятность`,
        `Допустимый срок, дней`, `Цена или срок`, `отклонение цены, %`
        """
        sheet_price_filter_rules = self._rw_google.read_sheet(1)

        params_head = ['id_rule', 'type_rule', 'rule_value', 'type_select_supplier', 'id_suppliers',
                       'type_select_routes', 'name_routes', 'type_select_supplier_storage', 'supplier_storage',
                       'supplier_storage_min_stock', 'delivery_probability', 'max_delivery_period',
                       'type_selection_rule', 'selection_rule']
        price_filter_rules = []
        for i, val in enumerate(sheet_price_filter_rules[5:], start=6):
            price_filter_rule = dict(zip(params_head, val))
            price_filter_rule = self.convert_value_rule(price_filter_rule)
            price_filter_rule['row_price_filter_on_sheet'] = i
            price_filter_rules.append(price_filter_rule)
        return price_filter_rules

    def set_selected_products(self, filtered_products: list[dict], count_row: int or str, name_column: str) -> None:
        """
        Записываем информацию о выборе позиции для последующего получения цены
        :param filtered_products: Список словарей.
        Обязательный ключ [{'row_product_on_sheet': номер строки int or str}]
        :param count_row: Общее количество строк с данными таблицы без заголовка
        :param name_column: Сочетание Букв колонки excel 'A' или 'B' или 'AA' или 'BB' и т.п.
        :return:
        """

        product_dict = {str(product['row_product_on_sheet']): ", ".join(product['id_rule'])
                        for product in filtered_products}
        values = [{'range': f"{name_column}{i}", 'values': [[product_dict.get(str(i), "")]]} for i in
                  range(2, count_row + 2)]

        self._rw_google.save_batch(0, values)

    @staticmethod
    def convert_date(date: str) -> dt:
        """
        Преобразуем дату полученной из Google таблицы в необходимый формат
        :param date: Строка с датой в формате '%d.%m.%Y'
        :return: Дата в формате datetime.datetime(2024, 01, 01, 0, 0)
        """
        return dt.strptime(date, '%d.%m.%Y') if date \
            else dt.strptime('01.01.2024', '%d.%m.%Y')

    def convert_value_rule(self, dict_rule: dict) -> dict:
        """
        Преобразуем формат ключей правил для дальнейшей работы программы
        Преобразовывает значения словаря полученных правил 'type_select_supplier', 'type_select_routes' и
        'type_select_supplier_storage' в нужный формат
            'type_select_supplier' -> bool,
            'type_select_routes' -> bool,
            'type_select_supplier_storage' -> bool,
        :param dict_rule: Словарь с ключами 'date_start', 'last_start', 'time_start', 'time_finish' и 'repeat'
        :return: Преобразованный словарь
        """
        dict_rule['type_select_supplier'] = self.convert_black_white_to_bool(dict_rule['type_select_supplier'])
        dict_rule['type_select_routes'] = self.convert_black_white_to_bool(dict_rule['type_select_routes'])
        dict_rule['type_select_supplier_storage'] = self.convert_black_white_to_bool(
            dict_rule['type_select_supplier_storage']
        )
        return dict_rule

    @staticmethod
    def convert_black_white_to_bool(value: str) -> bool:
        """
        Преобразуем значения
        Если черный список, то устанавливаем False.
        Если белый список, то устанавливаем True
        :param value: Строка со значением повтора уведомления. Допустимые значения: "да" или "нет"
        :return: bool
        """
        value = value.lower()
        return True if value == "белый список" else False if value == "черный список" else None

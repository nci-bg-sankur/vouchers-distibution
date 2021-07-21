import datetime
import math
import sys
import os
import requests
import json
import pandas as pd

try:
    import pika
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic
except ModuleNotFoundError:
    pass
from urllib.parse import urljoin

from pandas.core.groupby.generic import DataFrameGroupBy
from typing import NoReturn, List, Union, Optional, Hashable, Dict
from enum import IntEnum, unique


@unique
class Direction(IntEnum):
    """Направление"""
    RESERVED = 1  # Резерв
    SANATORIUM = 2  # Санаторий
    EXCHANGE = 3  # Обмен
    MEDICAL_UNIT = 4  # МСЧ


class VoucherStatus(IntEnum):
    """Статусы при распределении"""
    TO_SANATORIUM = 1  # Распределение в санаторий
    TO_RESERVE = 2  # В резерв УМО
    TO_EXCHANGE = 3  # На обмен
    TO_MEDICAL_UNIT = 4  # В резерв МСЧ


class Settings:
    """Настройки распределения"""
    sanatorium_id: Optional[Hashable]
    to_sanatorium: int
    to_reserve: int
    to_exchange: int
    to_medical_unit: int


class Distribution(object):
    try:
        channel: BlockingChannel
    except NameError:
        pass
    vouchers: list

    # Настройки
    _df: pd.DataFrame
    _settings: List[Settings]
    _vouchers_exists: pd.DataFrame

    # списки путёвок после распределения
    to_sanatorium_vouchers = []
    to_reserve_vouchers = []
    to_exchange_vouchers = []
    to_medical_unit_vouchers = []

    # для отладки
    dump_vouchers_per_months: List[Dict[str, List]] = []
    dump_vouchers_per_days = []
    dump_arrivals_per_months = []
    dump_total_vouchers_by_months = []

    def __init__(self, **kwargs):
        self.vouchers = kwargs.get('vouchers', [])

        if not self.vouchers:
            self.ampq_url = kwargs.get('ampq_url')
            self.request_queue = kwargs.get('request_queue')
            self.vouchers_url = kwargs.get('vouchers_url')
            self.vouchers_status_code = kwargs.get('vouchers_status_code')
            self.vouchers_page_limit = kwargs.get('vouchers_page_limit')

            assert self.ampq_url, 'Необходимо указать адрес подключения к серверу RabbitMQ.'
            assert self.request_queue, ('Необходимо указать имя очереди, '
                                        'в которой будут передаваться настройки распределения.')
            assert self.vouchers_url, 'Необходимо указать URL адрес по которому необходимо получать список путёвок.'
            assert self.vouchers_status_code, 'Необходимо указать код статуса путёвок для фильтрации списка путёвок.'
            assert self.vouchers_page_limit, 'Необходимо указать кол-во элементов на 1 странице в списке путёвок.'
        else:
            self._df = pd.DataFrame(self.vouchers)

    @property
    def settings(self) -> List[Settings]:
        return self._settings

    @settings.setter
    def settings(self, value: List[Settings]):
        self._settings = value

    @property
    def dataframe(self):
        vouchers = self.get_distribute()
        rows = []
        for voucher in vouchers:
            if voucher['status'] == VoucherStatus.TO_SANATORIUM:
                status = 'В санаторий'
            else:
                status = 'Куда-то'
            row = [
                voucher['id'],
                voucher['sanatorium_id'],
                voucher['organization_id'],
                voucher['number'],
                voucher['date_begin'],
                voucher['date_end'],
                voucher['duration'],
                voucher['arrival_number'],
                status
            ]
            rows.append(row)

        df = pd.DataFrame(
            columns=[
                'ID',
                'Санаторий ID',
                'Организация',
                'Номер путёвки',
                'Дата заезда',
                'Дата выезда',
                'Длительность',
                'Заезд №',
                'Статус',
            ],
            data=rows
        )
        df.index += 1
        return df

    @property
    def df_exists(self):
        rows = []
        for idx, item in self._vouchers_exists.iterrows():
            row = [
                item['id'],
                item['sanatorium_id'],
                item['organization_id'],
                item['number'],
                item['date_begin'],
                item['date_end'],
                item['duration'],
                item['arrival_number'],
                ''
            ]
            rows.append(row)
        df = pd.DataFrame(
            columns=[
                'ID',
                'Санаторий ID',
                'Организация',
                'Номер путёвки',
                'Дата заезда',
                'Дата выезда',
                'Длительность',
                'Заезд №',
                'Статус',
            ],
            data=rows
        )
        df.index += 1
        return df

    @property
    def contol_df(self):
        rows = []
        for sanatorium_idx, dump_vouchers_per_months in enumerate(self.dump_vouchers_per_months):
            for month, month_stat in dump_vouchers_per_months.items():
                month_str = datetime.datetime.strptime(month, '%Y-%m').strftime('%B')
                rows.append([
                    month_str,
                    '%d%%' % month_stat[1],
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',

                ])
                totals_percents = 0
                totals_5 = 0
                totals_6 = 0
                totals_7 = 0
                totals_8 = 0
                for date, day_stat in self.dump_vouchers_per_days[sanatorium_idx].items():
                    if date[:7] == month:
                        total_vouchers_in_day_correct = day_stat[5] if len(day_stat) == 6 else day_stat[4]
                        rows.append([
                            date,
                            day_stat[0],
                            '',
                            '',
                            day_stat[1],
                            day_stat[2],
                            day_stat[3],
                            day_stat[3],
                            day_stat[4],
                            day_stat[4],
                            total_vouchers_in_day_correct,
                            total_vouchers_in_day_correct - day_stat[0],
                        ])
                        totals_percents += day_stat[1]
                        totals_5 += day_stat[2]
                        totals_6 += day_stat[3]
                        totals_7 += day_stat[4]
                        totals_8 += total_vouchers_in_day_correct
                rows.append([
                    'ИТОГО %s' % month_str,
                    month_stat[0],
                    month_stat[2],
                    month_stat[3],
                    totals_percents,
                    totals_5,
                    totals_6,
                    totals_6,
                    totals_7,
                    totals_7,
                    totals_8,
                    '',
                ])

        df = pd.DataFrame(
            columns=[
                'День заезда',
                '% мес/кол-во путёвок в заезде',
                'кол-во путевок к распред помесячно',
                'Округлить гр 3',
                '% пут. по заездам',
                'кол-во пут. По заездам',
                'отбросили десятки',
                'округлили',
                'получили четное',
                'Итого',
                'Итого с корректировкой',
                'Если > 1 то ОШИБКА',
            ],
            data=rows
        )
        return df

    @property
    def get_sanatoriums(self) -> pd.Series:
        return self._df['sanatorium_id'].value_counts()

    def get_distribute(self) -> list:
        """
        Функция формирует унифицированный список путёвок по распределению
        """
        self.dump_arrivals_per_months = []
        self.dump_vouchers_per_months = []
        self.dump_vouchers_per_days = []
        self.dump_total_vouchers_by_months = []

        df = self._df
        self.to_sanatorium_vouchers = []
        for sanatorium_id, total_vouchers in self.get_sanatoriums.items():
            # выделим срез данных только по текущему санаторию
            is_sanatorium = df['sanatorium_id'] == sanatorium_id
            df_sanatorium = df[is_sanatorium].sort_values(by=['date_begin', 'number'])

            # сгруппируем путёвки по дате заезда (заездным дням)
            begin_dates_df = df_sanatorium.groupby('date_begin')

            # получим настройки распределения
            settings = self.get_sanatorium_setting(sanatorium_id)

            # получим данные для распределения по месяцам
            vouchers_per_months = self.get_vouchers_per_months(begin_dates_df, total_vouchers, settings)
            # print(f'vouchers_per_months = {vouchers_per_months}')
            self.dump_vouchers_per_months.append(vouchers_per_months)

            # получим данные для распределения по дням
            vouchers_per_days = self.get_vouchers_per_days(begin_dates_df, vouchers_per_months)
            # print(f'vouchers_per_days = {vouchers_per_days}')
            self.dump_vouchers_per_days.append(vouchers_per_days)

            self.get_sanatorium_vouchers(df_sanatorium, vouchers_per_days)
        result = []
        if self.to_sanatorium_vouchers:
            result.extend(self.to_sanatorium_vouchers)
        return result

    @staticmethod
    def get_vouchers_per_months(df: DataFrameGroupBy, total_vouchers: int, settings: Settings) -> Dict[str, List]:
        """
        Функция получает данные для распределения по месяцам.

        :param df: DataFrame сгруппированный по дате заезда.
        :param total_vouchers: Общее кол-во путёвок к распределению.
        :param settings: Настройки распределения.
        :return: Словарь содержащий год-месяц в виде индекса и массив из 4-х элементов, где
                 1-ый элемент — кол-во путёвок в месяце,
                 2-ой элемент — процент путёвок в месяце от общего числа путёвок к распределению,
                 3-ий элемент — кол-во путёвок к распределению помесячно,
                 4-ый элемент — кол-во путёвок к распределению помесячно (после округления),
        """
        vouchers_per_months = {}
        for begin_date, indexes in df.groups.items():
            month = begin_date[:7]
            vouchers_in_month = vouchers_per_months.get(month, [])
            vouchers_per_months[month] = vouchers_in_month + list(indexes)

        total_vouchers_to_sanatorium_by_months = 0

        for date, indexes in vouchers_per_months.items():
            total_vouchers_in_month = len(indexes)
            vouchers_in_month = total_vouchers_in_month / total_vouchers
            vouchers_to_sanatorium_in_month = settings.to_sanatorium * vouchers_in_month
            vouchers_to_sanatorium_in_month_round = round(vouchers_to_sanatorium_in_month)
            total_vouchers_to_sanatorium_by_months += vouchers_to_sanatorium_in_month_round

            vouchers_per_months[date] = [
                # всего путёвок в месяце
                total_vouchers_in_month,

                # процент путёвок в месяце
                round(vouchers_in_month * 100),

                # кол-во путёвок к распределению помесячно
                vouchers_to_sanatorium_in_month,
                vouchers_to_sanatorium_in_month_round
            ]

        # проверим насколько получилось правильно посчитать заявок в заездные день за все месяцы,
        # если общее число не равно указанному кол-во путёвок на распределение — добавим/вычтем
        # недостающие в последний месяц
        if total_vouchers_to_sanatorium_by_months != settings.to_sanatorium:
            fault_vouchers_count = settings.to_sanatorium - total_vouchers_to_sanatorium_by_months
            vouchers_per_months[list(vouchers_per_months.keys())[-1]][-1] += fault_vouchers_count

        return vouchers_per_months

    def get_vouchers_per_days(self, df: DataFrameGroupBy, vouchers_per_months: Dict[str, List]) -> Dict[str, List]:
        """
        Функция получает данные для распределения по заездным дням.

        :param df: DataFrame сгруппированный по дате заезда.
        :param vouchers_per_months: данные распределения по месяцам.
        :return: Словарь, в виде индекса (день заезда) и массив значений из 4-х элементов, где:
                 1-ый элемент — процент путёвок по заездам,
                 2-ой элемент — кол-во путёвок по заездам,
                 3-ий элемент — кол-во путёвок по заездам целочисленное (без дроби),
                 4-ий элемент — кол-во путёвок по заездам округлённое до ближайшего чётного числа,
                 5-ый элемент — (опциональный) скорректированное кол-во путёвок за заезд.
        """
        # посчитаем кол-во путёвок в день
        vouchers_per_days = {}
        arrivals_per_months = {}
        for date, indexes in df.groups.items():
            arrivals_per_months[date[:7]] = arrivals_per_months.get(date[:7], 0) + 1
            vouchers_per_days[date] = len(indexes)

        # print(f'arrivals_per_months = {arrivals_per_months}')
        self.dump_arrivals_per_months.append(arrivals_per_months)

        # сформируем массив данных для распределения по дням заезда
        for date, total_vouchers_per_day in vouchers_per_days.items():
            data_only_month = date[:7]
            vouchers_per_arrivals = total_vouchers_per_day / vouchers_per_months[data_only_month][0]
            cnt_vouchers_per_arrival = vouchers_per_months[data_only_month][2] * vouchers_per_arrivals
            cnt_vouchers_per_arrival_int = int(cnt_vouchers_per_arrival)
            vouchers_per_days[date] = [
                # кол-во путёвок в день
                total_vouchers_per_day,
                # процент путёвок по заездам
                vouchers_per_arrivals * 100,
                # кол-во путёвок по заездам
                cnt_vouchers_per_arrival,
                # кол-во путёвок по заездам целочисленное (без дроби)
                cnt_vouchers_per_arrival_int,
                # округляем до ближайшего чётного числа
                cnt_vouchers_per_arrival_int if self.is_even(
                    cnt_vouchers_per_arrival_int) else cnt_vouchers_per_arrival_int + 1,

            ]

        # посчитаем кол-во путёвок к распределению по месяцам после расчёта распределения по дням
        total_vouchers_by_months = {}
        for date, stat in vouchers_per_days.items():
            month = date[:7]
            total_vouchers_by_months[month] = total_vouchers_by_months.get(month, 0) + stat[-1]

        # print(f'total_vouchers_by_months = {total_vouchers_by_months}')
        self.dump_total_vouchers_by_months.append(total_vouchers_by_months)

        # скорректируем кол-во путёвок за заезд исходя из расчётного кол-ва путёвок в месяц.
        for month, total_vouchers_in_month in total_vouchers_by_months.items():
            overload_ratio = vouchers_per_months[month][-1] - total_vouchers_in_month
            print(f'overload_ratio = {overload_ratio}')
            abs_overload_ration = abs(overload_ratio)
            arrivals_in_month = arrivals_per_months[month]
            overload_ration_in_day = overload_ratio / arrivals_in_month
            if overload_ration_in_day > 0:
                overload_ration_in_day = math.ceil(overload_ration_in_day)
            else:
                overload_ration_in_day = math.floor(overload_ration_in_day)

            for date, stat in sorted(list(vouchers_per_days.items()), reverse=True):
                if date[:7] == month and abs_overload_ration > 0:
                    vouchers_per_days[date].append(vouchers_per_days[date][-1] + overload_ration_in_day)
                    abs_overload_ration -= abs(overload_ration_in_day)

        return vouchers_per_days

    @staticmethod
    def is_even(number) -> bool:
        """
        Функция проверки на чётность числа.
        """
        return number % 2 == 0

    def get_sanatorium_vouchers(self, vouchers: pd.DataFrame, vouchers_per_days: Dict[str, List]) -> NoReturn:
        """
        Функция получает унифицированный список путёвок по расчётному плану распределения.

        :param vouchers: Список путёвок.
        :param vouchers_per_days: Расчётные данные распределения путёвок по заездным дням.
        """
        cnt_vouchers_to_distribute = {}
        for date, stat in vouchers_per_days.items():
            cnt_vouchers_to_distribute[date] = stat[-1]

        for index, row in vouchers.iterrows():
            if cnt_vouchers_to_distribute[row['date_begin']] > 0:
                voucher_to_distribute = row.copy()
                voucher_to_distribute['status'] = VoucherStatus.TO_SANATORIUM
                voucher_to_distribute['organization_id'] = voucher_to_distribute['sanatorium_id']
                self.to_sanatorium_vouchers.append(voucher_to_distribute)
                cnt_vouchers_to_distribute[row['date_begin']] -= 1
                vouchers.drop(index=index)
        self._vouchers_exists = vouchers

    def get_sanatorium_setting(self, sanatorium_id: Optional[Hashable]) -> Union[Settings, None]:
        """
        Функция находит и возвращает параметры распределения для конкретного санатория.
        """
        for setting in self.settings:
            if setting.sanatorium_id == sanatorium_id:
                return setting
        return None

    def start(self) -> NoReturn:
        """
        Функция выполняет постоянное соединение с RabbitMQ и создаёт очередь в которой ожидает получение сообщений.
        """
        parameters = pika.URLParameters(self.ampq_url)
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        self.channel.queue_declare(queue=self.request_queue)
        self.channel.basic_consume(queue=self.request_queue, on_message_callback=self.receiver, auto_ack=True)
        self.channel.start_consuming()

    def stop(self) -> NoReturn:
        self.channel.stop_consuming()

    def receiver(self, ch, method, props, body: bytes):
        """Функция обработки входящих сообщений от брокера RabbitMQ"""
        _ch: BlockingChannel = ch
        _method: Basic.Deliver = method
        _props: pika.BasicProperties = props
        # TODO: Написать адаптер для перевода настроек в правильный формат.
        self.settings = json.loads(body)
        print(' [x] Received %r' % self.settings)

    def get_vouchers(self, limit: int, offset: int) -> NoReturn:
        """
        Метод заполняет массив путёвок GET запросами к API списка путёвок.
        :param limit: Максимальное кол-во элементов в одном запросе.
        :param offset: Отступ по списку найденных элементов.
        """
        filters = {
            'status__code': self.vouchers_status_code,
            'limit': limit,
            'offset': offset,
            'sanatorium_id__in': self.get_sanatorium_ids,
            'date_begin__gte': self.distribution_date[0],
            'date_begin__lte': self.distribution_date[1],
        }
        url = urljoin(self.vouchers_url, '/api/v1.0/voucher/')
        r = requests.get(url, params=filters)
        if r.status_code == requests.codes.ok:
            data = r.json()
            self.vouchers.extend(data['rows'])
            if data['total'] - len(self.vouchers) > 0:
                self.get_vouchers(limit=limit, offset=len(self.vouchers) + 1)

    @property
    def get_sanatorium_ids(self):
        ids = []
        for setting in self.settings:
            ids.append(str(setting.sanatorium_id))
        return ','.join(ids)

    @property
    def df(self):
        return self._df


if __name__ == '__main__':
    ampq_url = os.environ.get('AMQP_URL', 'amqp://localhost?connection_attempts=5&retry_delay=5')
    request_queue = os.environ.get('QUEUE_NAME_REQUEST', 'request_queue')
    vouchers_url = os.environ.get('VOUCHERS_URL', 'https://11b16e85-25b8-4ff2-9980-f2c136ddc8b7.mock.pstmn.io')
    status_code = os.environ.get('VOUCHERS_STATUS_CODE', 2)
    page_limit = os.environ.get('VOUCHERS_PAGE_ITEMS', 500)

    d = Distribution(
        ampq_url=ampq_url,
        request_queue=request_queue,
        vouchers_url=vouchers_url,
        vouchers_status_code=status_code,
        vouchers_page_limit=page_limit,
    )

    try:
        d.start()
    except KeyboardInterrupt:
        d.stop()
        sys.exit(0)

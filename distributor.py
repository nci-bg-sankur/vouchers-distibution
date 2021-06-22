import math
import sys
import os
import requests
import json
import pandas as pd

if os.environ.get('AMQP_URL'):
    import pika
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic
    from urllib.parse import urljoin

from typing import NoReturn, List, Union
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
    sanatorium_id: int
    to_sanatorium: int
    to_reserve: int
    to_exchange: int
    to_medical_unit: int


class Distribution(object):
    # channel: BlockingChannel
    vouchers: list

    # Настройки
    _df: pd.DataFrame
    _settings: List[Settings]

    # списки путёвок после распределения
    to_sanatorium_vouchers = []
    to_reserve_vouchers = []
    to_exchange_vouchers = []
    to_medical_unit_vouchers = []

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
        return df

    @property
    def get_sanatoriums(self) -> pd.Series:
        return self._df['sanatorium_id'].value_counts()

    def get_distribute(self) -> list:
        """
        Функция формирует унифицированный список путёвок по распределению
        """
        df = self._df
        arrivals = df['arrival_number'].max()
        self.to_sanatorium_vouchers = []
        for sanatorium_id, total_vouchers in self.get_sanatoriums.items():
            # выделим срез данных только по текущему санаторию
            is_sanatorium = df['sanatorium_id'] == sanatorium_id
            df_sanatorium = df[is_sanatorium].sort_values(by=['date_begin', 'number'])

            settings = self.get_sanatorium_setting(sanatorium_id)
            vouchers_per_arrival = self.get_vouchers_per_arrival(settings.to_sanatorium, arrivals)
            self.get_sanatorium_vouchers(df_sanatorium, settings.to_sanatorium, vouchers_per_arrival)
        result = []
        if self.to_sanatorium_vouchers:
            result.extend(self.to_sanatorium_vouchers)
        return result

    @staticmethod
    def is_even(number) -> bool:
        """
        Функция проверки на чётность числа.
        """
        return number % 2 == 0

    def get_vouchers_per_arrival(self, to_distribute: int, arrivals: int) -> int:
        """
        Функция вычисляет кол-во путёвок на 1 заезд. При этом возвращает всегда чётное число,
        т.к. путёвки должны распределяться по парам.
        """
        result = to_distribute / arrivals
        if not result.is_integer():
            _result = math.ceil(result)
            if self.is_even(_result):
                return _result
            _result = math.floor(result)
            if self.is_even(_result):
                return _result
        else:
            if self.is_even(result):
                return int(result)
            else:
                return int(result + 1)

    def get_sanatorium_vouchers(self,
                                vouchers: pd.DataFrame,
                                to_sanatorium: int,
                                vouchers_per_arrival: int,
                                arrival_number: int = 1,
                                row: int = 0) -> NoReturn:
        """
        Функция получает унифицированный список путёвок в санаторий.

        :param vouchers: Список путёвок.
        :param to_sanatorium: Кол-во путёвок к распределению.
        :param vouchers_per_arrival: Кол-во путёвок в 1 заезде.
        :param arrival_number: Текущий заезд.
        :param row: Текущий индекс списка путёвок.
        """
        _to_sanatorium = to_sanatorium
        last_row = row
        next_arrival = False
        if _to_sanatorium >= 2:
            for x in range(row, row + vouchers_per_arrival, 2):
                last_row = x
                try:
                    one = vouchers.loc[x]
                    two = vouchers.loc[x + 1]
                    if (one['date_begin'] == two['date_begin'] and
                            one['arrival_number'] == two['arrival_number'] == arrival_number):
                        if arrival_number == 2:
                            print(arrival_number)
                            print(one)
                            print(two)
                            print('---')
                        one['status'] = two['status'] = VoucherStatus.TO_SANATORIUM
                        one['organization_id'] = two['organization_id'] = one['sanatorium_id']
                        self.to_sanatorium_vouchers.append(one)
                        self.to_sanatorium_vouchers.append(two)
                        _to_sanatorium = _to_sanatorium - 2
                        next_arrival = True
                except KeyError:
                    _to_sanatorium = 0
                    break
            if next_arrival:
                arrival_number = arrival_number + 1

        if _to_sanatorium > 0:
            self.get_sanatorium_vouchers(vouchers, _to_sanatorium, vouchers_per_arrival, arrival_number, last_row + 1)

    def get_sanatorium_setting(self, sanatorium_id: int) -> Union[Settings, None]:
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

    # def receiver(self, ch: BlockingChannel, method: Basic.Deliver, props: pika.BasicProperties, body: bytes):
    def receiver(self, ch, method, props, body: bytes):
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
            'sanatorium_id__in': self.sanatorium,
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

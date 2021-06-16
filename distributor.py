import sys
import os
import pika
import requests
import json
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from typing import NoReturn, List
from enum import IntEnum, unique
from datetime import date
from urllib.parse import urljoin


@unique
class Direction(IntEnum):
    """Направление"""
    RESERVED = 1      # Резерв
    SANATORIUM = 2    # Санаторий
    EXCHANGE = 3      # Обмен
    MEDICAL_UNIT = 4  # МСЧ


class Distribution(object):
    channel: BlockingChannel
    vouchers: list

    # Настройки
    settings: dict
    sanatorium: List[int]
    directions: List[Direction]
    statement: int
    distribution_date: List[date, date]

    def __init__(self, **kwargs):
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

    def receiver(self, ch: BlockingChannel, method: Basic.Deliver, props: pika.BasicProperties, body: bytes):
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
            'order_by': ['-date_begin', 'number'],
        }
        url = urljoin(self.vouchers_url, '/api/v1.0/voucher/')
        r = requests.get(url, params=filters)
        if r.status_code == requests.codes.ok:
            data = r.json()
            self.vouchers.extend(data['rows'])
            if data['total'] - len(self.vouchers) > 0:
                self.get_vouchers(limit=limit, offset=len(self.vouchers) + 1)


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

import json
import math

import numpy as np
import pandas as pd
import streamlit as st

from distributor import Distribution, Settings


class Options:
    def __init__(self, total_vouchers: int):
        self.total_vouchers = total_vouchers

        self._to_sanatorium = 0
        self._to_sanatorium_percent = float(0)

        self._to_reserve = 0
        self._to_reserve_percent = float(0)

        self._to_exchange = 0
        self._to_exchange_percent = float(0)

        self._to_medical_unit = 0
        self._to_medical_unit_percent = float(0)

    @property
    def to_sanatorium(self):
        return self._to_sanatorium

    @to_sanatorium.setter
    def to_sanatorium(self, value):
        self._to_sanatorium = value

    @property
    def to_sanatorium_max_value(self):
        return self.total_vouchers - (self._to_reserve + self._to_exchange + self._to_medical_unit)

    @property
    def to_sanatorium_value(self):
        return math.floor(self._to_sanatorium_percent / 100 * self.total_vouchers)

    @property
    def to_sanatorium_percent(self):
        return self._to_sanatorium_percent

    @to_sanatorium_percent.setter
    def to_sanatorium_percent(self, value):
        self._to_sanatorium_percent = value

    @property
    def to_sanatorium_percent_value(self):
        return round((self._to_sanatorium / self.total_vouchers * 100), 2)

    @property
    def to_reserve(self):
        return self._to_reserve

    @to_reserve.setter
    def to_reserve(self, value):
        self._to_reserve = value

    @property
    def to_reserve_max_value(self):
        return self.total_vouchers - (self._to_sanatorium + self._to_exchange + self._to_medical_unit)

    @property
    def to_reserve_percent(self):
        return self._to_reserve_percent

    @to_reserve_percent.setter
    def to_reserve_percent(self, value):
        self._to_reserve_percent = value

    @property
    def to_reverse_percent_value(self):
        return round((self._to_reserve / self.total_vouchers * 100), 2)

    @property
    def to_exchange(self):
        return self._to_exchange

    @to_exchange.setter
    def to_exchange(self, value):
        self._to_exchange = value

    @property
    def to_exchange_max_value(self):
        return self.total_vouchers - (self._to_sanatorium + self._to_reserve + self._to_medical_unit)

    @property
    def to_exchange_percent(self):
        return self._to_exchange_percent

    @to_exchange_percent.setter
    def to_exchange_percent(self, value):
        self._to_exchange_percent = value

    @property
    def to_exchange_percent_value(self):
        return round((self._to_exchange / self.total_vouchers * 100), 2)

    @property
    def to_medical_unit(self):
        return self._to_medical_unit

    @to_medical_unit.setter
    def to_medical_unit(self, value):
        self._to_medical_unit = value

    @property
    def to_medical_unit_value(self):
        return self.to_medical_unit_max_value

    @property
    def to_medical_unit_max_value(self):
        return self.total_vouchers - (self._to_sanatorium + self._to_reserve + self._to_exchange)

    @property
    def to_medical_unit_percent(self):
        return self._to_medical_unit_percent

    @to_medical_unit_percent.setter
    def to_medical_unit_percent(self, value):
        self._to_medical_unit_percent = value

    @property
    def to_medical_unit_percent_value(self):
        return round((self._to_medical_unit / self.total_vouchers * 100), 2)


st.set_page_config('Алгоритм распределения путёвок', layout='wide')

st.sidebar.header('Распределение путёвок')
st.sidebar.header('Шаг 1')
json_file_vouchers = st.sidebar.file_uploader(
    label='Выберите файл со списком путёвок',
    help='Файл должен быть в формате JSON.',
    type=['json', 'txt']
)
if json_file_vouchers is not None:
    vouchers_data = json.loads(json_file_vouchers.getvalue())
    dist = Distribution(vouchers=vouchers_data.get('rows', []))

    st.sidebar.header('Шаг 2')
    st.sidebar.subheader('Настройка распределения:')

    sanatoriums = dist.get_sanatoriums
    settings = []
    for sanatorium_id, total_vouchers in sanatoriums.items():
        # параметры настройки
        sanatorium_settings = Settings()
        sanatorium_settings.sanatorium_id = sanatorium_id

        # для каждого санатория инициализирует свой класс с настройками
        temp_options = Options(total_vouchers=total_vouchers)

        # выделим срез данных только по текущему санаторию
        is_sanatorium = dist.df['sanatorium_id'] == sanatorium_id
        df_sanatorium = dist.df[is_sanatorium].sort_values(by=['date_begin', 'number'])

        # выводим данные по санатория
        st.sidebar.success('ID санатория: %d' % sanatorium_id)
        st.sidebar.warning('Доступно путёвок к распределению: %d' % total_vouchers)

        # выведем пользователю настройки
        temp_options.to_sanatorium = st.sidebar.number_input(
            label='В санаторий',
            min_value=0,
            max_value=temp_options.to_sanatorium_max_value,
            value=temp_options.to_sanatorium_value,
            key='to_sanatorium_%s' % sanatorium_id
        )
        sanatorium_settings.to_sanatorium = temp_options.to_sanatorium
        temp_options.to_sanatorium_percent = st.sidebar.info(f'{temp_options.to_sanatorium_percent_value}%')

        temp_options.to_reserve = st.sidebar.number_input(
            label='В резерв',
            min_value=0,
            max_value=temp_options.to_reserve_max_value,
            value=temp_options.to_reserve,
            key='to_reserve_%s' % sanatorium_id
        )
        sanatorium_settings.to_reserve = temp_options.to_reserve
        temp_options.to_reserve_percent = st.sidebar.info(f'{temp_options.to_reverse_percent_value}%')

        temp_options.to_exchange = st.sidebar.number_input(
            label='На обмен',
            min_value=0,
            max_value=temp_options.to_exchange_max_value,
            value=temp_options.to_exchange,
            key='to_exchange_%s' % sanatorium_id
        )
        sanatorium_settings.to_exchange = temp_options.to_exchange
        temp_options.to_exchange_percent = st.sidebar.info(f'{temp_options.to_exchange_percent_value}%')

        temp_options.to_medical_unit = st.sidebar.number_input(
            label='В МСЧ',
            min_value=0,
            max_value=temp_options.to_medical_unit_max_value,
            value=temp_options.to_medical_unit_value,
            key='to_medical_unit_%s' % sanatorium_id
        )
        sanatorium_settings.to_medical_unit = temp_options.to_medical_unit
        temp_options.to_medical_unit_percent = st.sidebar.info(f'{temp_options.to_medical_unit_percent_value}%')

        settings.append(sanatorium_settings)

        # визуально разделим настройки для разных санаториев
        st.sidebar.markdown('---')

    st.header('Результаты распределения')
    dist.settings = settings
    df = dist.dataframe
    st.write(df)

    st.subheader('Контрольная таблица')
    control_df = dist.contol_df.set_index('День заезда')

    COLUMNS_CHECK = {}

    def highlight_error(x: pd.Series, color: str):
        results = {}
        for row_name, row_value in x.items():
            results[row_name] = None

            if COLUMNS_CHECK.get(row_name, False):
                COLUMNS_CHECK[row_name].append(row_value)
            else:
                COLUMNS_CHECK[row_name] = [row_value]

            if len(COLUMNS_CHECK[row_name]) == 2:
                try:
                    if COLUMNS_CHECK[row_name][0] < abs(COLUMNS_CHECK[row_name][1]) or COLUMNS_CHECK[row_name][1] > 0:
                        results[row_name] = f'color:{color};'
                except TypeError:
                    pass
        return pd.Series(data=results, index=x.keys())


    st.dataframe(
        control_df.style.apply(
            highlight_error,
            color='red',
            subset=[
                '% мес/кол-во путёвок в заезде',
                'Если > 1 то ОШИБКА',
            ]
        )
    )

    with st.beta_expander('Исходный список'):
        st.write(dist.df_exists)

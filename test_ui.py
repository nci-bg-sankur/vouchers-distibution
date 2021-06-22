import math
import streamlit as st
import json
import pandas as pd
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
    st.header('Шаг 2')
    st.subheader('Настройка распределения:')
    vouchers_data = json.loads(json_file_vouchers.getvalue())

    dist = Distribution(vouchers=vouchers_data.get('rows', []))
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

        # выведем пользователю настройки
        st.beta_container()
        cols = st.beta_columns(6)
        with cols[0]:
            st.number_input('ID санатория:', value=sanatorium_id, key='sanatorium_%s' % sanatorium_id)
        with cols[1]:
            st.number_input('Доступно:', value=total_vouchers, key='vouchers_total_%s' % sanatorium_id)
        with cols[2]:
            temp_options.to_sanatorium = st.number_input(
                label='В санаторий',
                min_value=0,
                max_value=temp_options.to_sanatorium_max_value,
                value=temp_options.to_sanatorium_value,
                key='to_sanatorium_%s' % sanatorium_id
            )
            sanatorium_settings.to_sanatorium = temp_options.to_sanatorium
            temp_options.to_sanatorium_percent = st.number_input(
                label='%',
                min_value=float(0),
                max_value=float(100),
                value=temp_options.to_sanatorium_percent_value,
                key='to_sanatorium_percent_%s' % sanatorium_id
            )
        with cols[3]:
            temp_options.to_reserve = st.number_input(
                label='В резерв',
                min_value=0,
                max_value=temp_options.to_reserve_max_value,
                value=temp_options.to_reserve,
                key='to_reserve_%s' % sanatorium_id
            )
            sanatorium_settings.to_reserve = temp_options.to_reserve
            temp_options.to_reserve_percent = st.number_input(
                label='%',
                min_value=float(0),
                max_value=float(100),
                value=temp_options.to_reverse_percent_value,
                key='to_reserve_percent_%s' % sanatorium_id
            )
        with cols[4]:
            temp_options.to_exchange = st.number_input(
                label='На обмен',
                min_value=0,
                max_value=temp_options.to_exchange_max_value,
                value=temp_options.to_exchange,
                key='to_exchange_%s' % sanatorium_id
            )
            sanatorium_settings.to_exchange = temp_options.to_exchange
            temp_options.to_exchange_percent = st.number_input(
                label='%',
                min_value=float(0),
                max_value=float(100),
                value=temp_options.to_exchange_percent_value,
                key='to_exchange_percent_%s' % sanatorium_id
            )
        with cols[5]:
            temp_options.to_medical_unit = st.number_input(
                label='В МСЧ',
                min_value=0,
                max_value=temp_options.to_medical_unit_max_value,
                value=temp_options.to_medical_unit_value,
                key='to_medical_unit_%s' % sanatorium_id
            )
            sanatorium_settings.to_medical_unit = temp_options.to_medical_unit
            temp_options.to_medical_unit_percent = st.number_input(
                label='%',
                min_value=float(0),
                max_value=float(100),
                value=temp_options.to_medical_unit_percent_value,
                key='to_medical_unit_percent_%s' % sanatorium_id
            )

        settings.append(sanatorium_settings)

        # визуально разделим настройки для разных санаториев
        st.markdown('---')

    dist.settings = settings
    df = dist.dataframe
    st.write(df)

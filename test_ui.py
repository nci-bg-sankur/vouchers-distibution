import json

import pandas as pd
import streamlit as st

from distributor import Distribution, Settings
from test_options import TestOptions

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
    test_options = TestOptions()
    for sanatorium_id, total_vouchers in sanatoriums.items():
        # параметры настройки
        sanatorium_settings = Settings()
        sanatorium_settings.sanatorium_id = sanatorium_id
        test_options.total_vouchers = total_vouchers

        # выделим срез данных только по текущему санаторию
        is_sanatorium = dist.df['sanatorium_id'] == sanatorium_id
        df_sanatorium = dist.df[is_sanatorium].sort_values(by=['date_begin', 'number'])

        # выводим данные по санатория
        st.sidebar.success('ID санатория: %d' % sanatorium_id)
        exists_vouchers = st.sidebar.empty()

        # выведем пользователю настройки
        test_options.to_sanatorium = st.sidebar.number_input(
            label='В санаторий',
            min_value=0,
            max_value=test_options.to_sanatorium_max,
        )
        sanatorium_settings.to_sanatorium = test_options.to_sanatorium
        st.sidebar.info(f'{test_options.to_sanatorium_percent}%')

        test_options.to_reserve = st.sidebar.number_input(
            label='В резерв',
            min_value=0,
            max_value=test_options.to_reserve_max,
        )
        sanatorium_settings.to_reserve = test_options.to_reserve
        st.sidebar.info(f'{test_options.to_reverse_percent}%')

        # test_options.to_exchange = st.sidebar.number_input(
        #     label='На обмен',
        #     min_value=0,
        #     max_value=test_options.to_exchange_max,
        # )
        # sanatorium_settings.to_exchange = test_options.to_exchange
        # st.sidebar.info(f'{test_options.to_exchange_percent}%')

        test_options.medical_units = st.sidebar.number_input(
            label='Кол-во МСЧ',
            min_value=0,
        )
        if test_options.medical_units:
            st.sidebar.warning('Поля МСЧ на распределение заполняются поочередно.')
        for medical_unit in range(test_options.medical_units):
            test_options.to_medical_units[medical_unit] = st.sidebar.number_input(
                label='В МСЧ %d' % (medical_unit + 1),
                min_value=0,
                max_value=test_options.to_medical_unit_max
            )
            sanatorium_settings.to_medical_units[medical_unit] = test_options.to_medical_units[medical_unit]
            st.sidebar.info(f'{test_options.to_medical_unit_percent(medical_unit)}%')

        # дополним массив настроек конфигурацией распределения текущего санатория
        settings.append(sanatorium_settings)

        if test_options.exists_vouchers:
            exists_vouchers.error(
                'Доступно путёвок к распределению: %d из %d' % (test_options.exists_vouchers, total_vouchers)
            )
        else:
            exists_vouchers.error('Больше нет доступных путёвок к распределению.')

        # визуально разделим настройки для разных санаториев
        st.sidebar.markdown('---')

    st.header('Результаты распределения')
    dist.settings = settings
    df = dist.dataframe
    st.write(df)

    st.subheader('Контрольная таблица')
    col1, col2 = st.beta_columns([4, 1])

    _directions = ['to_sanatorium', 'to_reserve'] + ['to_medical_unit_%d' % (x + 1) for x in
                                                     range(test_options.medical_units)]
    directions = ['В санаторий', 'В резерв'] + ['МСЧ %d' % (x + 1) for x in range(test_options.medical_units)]
    direction = col2.radio('Направление распределения:', directions)

    control_table = dist.get_control_df(_directions[directions.index(direction)]).set_index('День заезда')

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


    col1.dataframe(
        control_table.style.apply(
            highlight_error,
            color='red',
            subset=[
                '% мес/кол-во путёвок в заезде',
                'Если > 1 то ОШИБКА',
            ]
        )
    )

    with st.beta_expander('Остаточный список'):
        st.write(dist.df_exists)

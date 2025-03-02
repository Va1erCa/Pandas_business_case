# main module (by Va1erCa)

import configparser, os
from pathlib import Path, PurePath
from datetime import datetime

import pandas as pd
import numpy as np


cnf = configparser.ConfigParser()
cnf.read("settings.ini", encoding='utf-8')


path_saby = cnf['Paths']['path_saby']
path_pharmacy = cnf['Paths']['path_pharmacy']
path_report = cnf['Paths']['path_report']
allowed_docs = cnf['Docs']['list_allowed_docs'].split(',')


def main() -> None:

    # Чтение данных из СБИС-выгрузки
    df_saby = pd.DataFrame()
    for f in Path(path_saby).iterdir():
        if f.suffix == '.csv' :
            print(f'Читаем файл: {f.name}')
            df_part = pd.read_csv(
                Path(path_saby).joinpath(f.name),
                thousands=' ',
                decimal=',',
                delimiter=';',
                parse_dates=[0],
                date_format='%d.%m.%y',
                encoding='cp1251'
            )
            print(f'Загружен: {Path(path_saby).joinpath(f.name)}, его форма: {df_part.shape}')
            df_saby = pd.concat([df_saby, df_part])

    df_saby.columns = [_.replace(' ','_').replace('.','_') for _ in list(df_saby)]
    print('Результирующий датафрейм:')
    print(df_saby.info())

    # Фильтруем допустимые документы из СБИС-выгрузки
    # docs = "СчФктр", "УпдДоп", "УпдСчфДоп", "ЭДОНакл"
    df_saby = df_saby.query('Тип_документа in @allowed_docs')

    df_saby = df_saby.groupby('Номер').first().reset_index()

    # Чтение/обработка данных аптечных выгрузок
    date_now = datetime.now().date().strftime('%Y-%m-%d')
    report_path = Path(path_report).joinpath(date_now)

    for f in Path(path_pharmacy).iterdir() :
        if f.suffix == '.csv' :
            df_pharmacy = pd.read_csv(
                Path(path_pharmacy).joinpath(f.name),
                thousands=' ',
                decimal='.',
                delimiter=';',
                encoding='cp1251'
            )
            df_pharmacy = pd.merge(df_pharmacy, df_saby,
                                   left_on='Номер накладной',
                                   right_on='Номер',
                                   how='left'
                                   )
            df_pharmacy['Дата'] = df_pharmacy['Дата'].dt.strftime('%d.%m.%Y')
            column_renames = (
                {
                    'Дата':'Дата счет-фактуры',
                    'Номер':'Номер счет-фактуры',
                    'Сумма':'Сумма счет-фактуры'
                }
            )
            df_pharmacy.rename(columns=column_renames, inplace=True)
            df_pharmacy['Сравнение дат'] = np.where(
                df_pharmacy['Дата накладной'] == df_pharmacy['Дата счет-фактуры'],
                '',
                'Не совпадает!'
            )
            res_columns = (
                ['№ п/п', 'Штрих-код партии', 'Наименование товара', 'Поставщик',
                 'Дата приходного документа', 'Номер приходного документа',
                 'Дата накладной', 'Номер накладной', 'Номер счет-фактуры',
                 'Сумма счет-фактуры', 'Кол-во',
                 'Сумма в закупочных ценах без НДС', 'Ставка НДС поставщика',
                 'Сумма НДС', 'Сумма в закупочных ценах с НДС', 'Дата счет-фактуры',
                 'Сравнение дат']
            )
            # оставляем нужные признаки
            df_pharmacy = df_pharmacy.loc[:, res_columns]
            df_pharmacy['Номер накладной'] += np.where(df_pharmacy['Поставщик'].str.find('ЕАПТЕКА') < 0, '', '/15')

            # готовим файловую инфраструктуру для записи результатов
            if not Path(report_path).is_dir() :
                print(f'Папки: {report_path} еще нет, создаем...')
                os.makedirs(report_path)
            else :
                print(f'Папка: {report_path} обнаружена...')

            name_report_file = f'{f.name.partition('.csv')[0]} - результат.xlsx'

            # записываем результат в xlsx-книгу
            with pd.ExcelWriter(Path(report_path).joinpath(name_report_file)) as ew :
                df_pharmacy.to_excel(ew, sheet_name='Sheet1', index=False)
            print(f'Файл: {name_report_file} успешно сохранен.')

if __name__ == '__main__':
    main()

import pandas as pd
import numpy as np
from datetime import timedelta
import json

# tem no colab também só precisa criar a pasta databases e upar os arquivos dentro
# link do colab: https://colab.research.google.com/drive/1Rza-GCMILncAiAJHlU4ZdmAu0MP1de7s#scrollTo=spFt9Ss4d2nE

# carregando os databases em dataframes e o arquivo schema
gateway_df = pd.read_pickle('./databases/gateway.pkl')
measurements_df = pd.read_pickle('./databases/measurements.pkl')
sensors_df = pd.read_pickle('./databases/sensors.pkl')

# carregsando o json com os tipo

schema = json.load(open('./databases/schema.json', 'r'))

datatypes = {}
for column in schema:
    datatype = schema[column]
    if datatype == 'string':
        datatypes[column] = '<U5'
    elif datatype == 'int':
        datatypes[column] = 'int64'
    elif datatype == 'boolean':
        datatypes[column] = 'bool'
    elif datatype == 'float':
        datatypes[column] = 'float64'
    elif datatype == 'datetime':
        datatypes[column] = 'O'

# tratando
gateway_df = gateway_df.dropna().drop_duplicates().rename(columns = {'id' : 'gateway_id', 'name': 'gateway_name'})
measurements_df = measurements_df.drop_duplicates()
sensors_df = sensors_df.rename(columns = {'gatewayId': 'gateway_id', 'id': 'sensor'})

# funções que serão usadas posteriormente

def renomeia_nome(nome):
    nome = nome.split('_')
    nome = nome[0].upper() + ' ' + nome[1].zfill(2)
    return nome

def valid_configurations(row):
    return all(row.start.notna()) and all(row.start.notna())

def percentual_valid_configurations(row):
    notna_start = row.start.notna()
    notna_frequency = row.frequency.notna()
    notna = list(zip(notna_start, notna_frequency))
    notna = list(map(lambda x: x[0] and x[1], notna))
    return len(list(filter(lambda x: x, notna))) / len(notna_start) * 100

def signal_status(row):
    if row.signal_mean_value < -100:
        row['signal_status'] = 'Ruim'
    elif row.signal_mean_value < -90:
        row['signal_status'] = 'Regular'
    else:
        row['signal_status'] = 'Bom'
    return row

def signal_issue(signals):
  return len(list(filter(lambda x: np.isnan(x) or x is None or x > 0, signals.tolist())))

def measurement_status(elapsed_time_since_last_measurement):
    if elapsed_time_since_last_measurement.days < 60:
      return 'coletado nos ultimos 60 dias'
    elif elapsed_time_since_last_measurement.days >= 60:
      return 'coletado há mais de 60 dias'
    else:
      return 'nunca coletado'

def expected_measurements(row):
    start = row.start
    start_seconds = start.hour * 60 * 60 + start.minute * 60 + start.second
    day_seconds = 24 * 60 * 60
    frequency = row.frequency
    frequency_seconds = frequency.total_seconds()
    return pd.Series({'gateway_id': row.gateway_id, 'expected_measurements': 1 + (day_seconds - start_seconds) // frequency_seconds})

def one_hour_groups(datetimes):
    pairs, hi = 0, 0
    datetimes = datetimes[datetimes.notna()]
    datetimes = sorted(datetimes.tolist())
    for i in range(len(datetimes)):
        while hi < len(datetimes) and datetimes[hi] - datetimes[i] < timedelta(hours = 1):
            hi += 1
        pairs += hi - i
    return pairs

def convert_to_datatypes(col, datatypes):
    return col.astype(datatypes[col.name])

# passos 1 e 2
gateway_df['gateway_name'] = gateway_df['gateway_name'].apply(renomeia_nome)

# passo 3
number_of_registered_sensors_df = sensors_df[['gateway_id']].value_counts().rename('number_of_registered_sensors')
gateway_df = gateway_df.merge(number_of_registered_sensors_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 4
valid_configurations_df = sensors_df[['gateway_id', 'start', 'frequency']].groupby('gateway_id')[['start', 'frequency']].apply(valid_configurations).rename('valid_configurations')
gateway_df = gateway_df.merge(valid_configurations_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 5
percentual_valid_configurations_df = sensors_df[['gateway_id', 'start', 'frequency']].groupby('gateway_id')[['start', 'frequency']].apply(percentual_valid_configurations).rename('percentual_valid_configurations')
gateway_df = gateway_df.merge(percentual_valid_configurations_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 6
expected_measurements_df = sensors_df.apply(expected_measurements, axis = 1).groupby('gateway_id').mean()
gateway_df = gateway_df.merge(expected_measurements_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 7
signal_mean_value_df = sensors_df[sensors_df.signal.notna()]
signal_mean_value_df = signal_mean_value_df[signal_mean_value_df.signal <= 0]
signal_mean_value_df = signal_mean_value_df[['gateway_id', 'signal']].groupby('gateway_id').mean()
signal_mean_value_df = signal_mean_value_df.rename(columns = {'signal': 'signal_mean_value'})
gateway_df = gateway_df.merge(signal_mean_value_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 8
signal_status_df = signal_mean_value_df.apply(signal_status, axis = 1)
signal_status_df = signal_status_df.drop(columns = ['signal_mean_value'])
gateway_df = gateway_df.merge(signal_status_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 9
signal_issue_df = sensors_df[['gateway_id', 'signal']].groupby('gateway_id')['signal'].apply(signal_issue).rename('signal_issue')
gateway_df = gateway_df.merge(signal_issue_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 10
#passo 10
tz = measurements_df['datetime'].loc[0].tz
measurements_df['elapsed_time_since_last_measurement'] = pd.Timestamp.today(tz = tz) - measurements_df['datetime']
elapsed_time_since_last_measurement_df = measurements_df[['sensor', 'elapsed_time_since_last_measurement']]
elapsed_time_since_last_measurement_df = sensors_df[['sensor', 'gateway_id']].drop_duplicates().merge(elapsed_time_since_last_measurement_df, left_on = 'sensor', right_on = 'sensor').drop(columns = ['sensor'])
elapsed_time_since_last_measurement_df = elapsed_time_since_last_measurement_df[['gateway_id', 'elapsed_time_since_last_measurement']].groupby('gateway_id').min()
gateway_df = gateway_df.merge(elapsed_time_since_last_measurement_df, left_on = 'gateway_id', right_on = 'gateway_id')

# passo 11
gateway_df['measurement_status'] = gateway_df['elapsed_time_since_last_measurement'].apply(measurement_status)

# passo 12
one_hour_groups_df = measurements_df[['sensor', 'datetime']].merge(sensors_df[['gateway_id', 'sensor']])
one_hour_groups_df = one_hour_groups_df[['gateway_id', 'datetime']].groupby('gateway_id')['datetime'].apply(one_hour_groups).rename('one_hour_groups')
gateway_df = gateway_df.merge(one_hour_groups_df, left_on = 'gateway_id', right_on = 'gateway_id')

# alterando os tipos das colunas
gateway_df = gateway_df.apply(convert_to_datatypes, datatypes = datatypes)

#########################################################################################################################################################################################################

# testes unitários

import unittest
from pandas.testing import assert_frame_equal, assert_series_equal

class Testes(unittest.TestCase):
  def test_renomeia_nome(self):
    teste_df = pd.DataFrame({'gateway_name': ['A_0', 'B_1', 'C_2', 'D_3']})
    exemplo_df = pd.DataFrame({'gateway_name': ['A 00', 'B 01', 'C 02', 'D 03']})
    teste_df['gateway_name'] = teste_df['gateway_name'].apply(renomeia_nome)
    assert_frame_equal(teste_df, exemplo_df)

  def teste_valid_configurations(self):
    ts = pd.Timestamp(year = 2023, month = 12, day = 21, hour = 11, minute = 12)

    teste_df = pd.DataFrame({
        'gateway_id': [1, 1, 2, 3, 4, 4],
        'start': [np.nan, ts, ts, np.nan, ts, ts],
        'frequency': [ts, ts, ts, np.nan, np.nan, ts]
    })

    exemplo_df = pd.Series({
        1: False,
        2: True,
        3: False,
        4: True
    }).rename_axis('gateway_id')

    teste_df = teste_df[['gateway_id', 'start', 'frequency']].groupby('gateway_id')[['start', 'frequency']].apply(valid_configurations)
    assert_series_equal(teste_df, exemplo_df)

  def teste_percentual_valid_configurations(self):
      ts = pd.Timestamp(year = 2023, month = 12, day = 21, hour = 11, minute = 12)

      teste_df = pd.DataFrame({
          'gateway_id': [1, 1, 2, 3, 4, 4],
          'start': [np.nan, ts, ts, np.nan, ts, ts],
          'frequency': [ts, ts, ts, np.nan, np.nan, ts]
      })

      exemplo_df = pd.Series({
          1: 50.0,
          2: 100.0,
          3: 0.0,
          4: 100.0
      }).rename_axis('gateway_id')

      teste_df = teste_df[['gateway_id', 'start', 'frequency']].groupby('gateway_id')[['start', 'frequency']].apply(percentual_valid_configurations)
      assert_series_equal(teste_df, exemplo_df)

  def teste_expected_measurements(self):
    start = pd.Timestamp(year = 2023, month = 12, day = 21, hour = 3, minute = 12, second = 0)
    frequency = pd.Timedelta(hours = 1, minutes = 23, seconds = 14)

    teste_df = pd.DataFrame({
        'gateway_id': [1],
        'start': [start],
        'frequency': [frequency]
    })

    exemplo_df = pd.DataFrame({
        'gateway_id': [1.0],
        'expected_measurements' :[15.0]
    }).set_index('gateway_id')

    teste_df = teste_df.apply(expected_measurements, axis = 1).groupby('gateway_id').mean()
    assert_frame_equal(teste_df, exemplo_df)

def executa_testes():
  testes = Testes()
  testes.test_renomeia_nome()
  testes.teste_valid_configurations()
  testes.teste_expected_measurements()

executa_testes()

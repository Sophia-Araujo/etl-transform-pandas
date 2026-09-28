"""Exercício 2 - Transformação de dados de sensores IoT (ETL / fase Transform)."""
import numpy as np
import pandas as pd

# EXTRACT (dados do enunciado; o PDF trunca o 4º/5º timestamp, então assumimos
# 10:01:00 para S3 e 10:02:00 para o último S2)
leituras_iot = {
    'sensor_id': ['S1', 'S2', 'S1', 'S3', 'S2'],
    'timestamp': ['2026-03-17 10:00:00', '17/03/2026 10:00', '2026-03-17 10:00:00',
                  '2026-03-17 10:01:00', '2026-03-17 10:02:00'],
    'temperatura_c': ['24.5 C', '850.0 C', '24.5 C', None, '-10.2 C'],
    'pressao_bar': ['1.01', '1.05', '1.01', '0.98', '0.00'],
}

TEMP_MIN, TEMP_MAX = -20.0, 100.0   # faixa operacional válida (°C)
PRESSAO_MIN = 0.5                   # pressão válida: estritamente > 0.5 bar


def transform_iot_data(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()

    # Regra 0: padroniza timestamps heterogêneos (ISO e dd/mm/aaaa) para datetime.
    # Feito ANTES da deduplicação: senão "17/03/2026 10:00" e "2026-03-17 10:00:00"
    # seriam strings diferentes para o mesmo instante.
    df_clean['timestamp'] = pd.to_datetime(
        df_clean['timestamp'], errors='coerce', dayfirst=True, format='mixed')
    df_clean = df_clean.dropna(subset=['timestamp', 'sensor_id'])  # chave inválida = descarte

    # Regra 1: remove duplicatas pela chave composta (sensor_id, timestamp)
    df_clean = df_clean.drop_duplicates(subset=['sensor_id', 'timestamp'], keep='first')

    # Regra 2: temperatura -> float, removendo a unidade textual " C"
    df_clean['temperatura_c'] = pd.to_numeric(
        df_clean['temperatura_c'].astype('string').str.replace('C', '', regex=False).str.strip(),
        errors='coerce')
    df_clean['pressao_bar'] = pd.to_numeric(df_clean['pressao_bar'], errors='coerce')

    # Regra 3: outliers irreais são descartados (nulos são preservados p/ a regra 4).
    # Descartar antes de imputar evita que valores absurdos contaminem a mediana.
    temp_ok = df_clean['temperatura_c'].isna() | df_clean['temperatura_c'].between(TEMP_MIN, TEMP_MAX)
    pressao_ok = df_clean['pressao_bar'] > PRESSAO_MIN   # NaN também é reprovado
    df_clean = df_clean[temp_ok & pressao_ok]

    # Regra 4: nulos de temperatura -> mediana do respectivo sensor.
    mediana = df_clean.groupby('sensor_id')['temperatura_c'].transform('median')
    df_clean['temperatura_c'] = df_clean['temperatura_c'].fillna(mediana)

    # Regra 5: sem histórico válido do sensor não há base estatística para imputar;
    # inventar valor mascararia falha de telemetria -> descarte justificado.
    df_clean = df_clean.dropna(subset=['temperatura_c'])

    return df_clean.sort_values(['sensor_id', 'timestamp']).reset_index(drop=True)


if __name__ == '__main__':
    df_raw = pd.DataFrame(leituras_iot)
    print('--- BRUTO ---');  print(df_raw.to_string(index=False))
    print('\n--- TRANSFORMADO ---'); print(transform_iot_data(df_raw).to_string(index=False))

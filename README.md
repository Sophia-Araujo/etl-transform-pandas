# A Fase de Transformação (Transform) no Pipeline ETL — Respostas

**Disciplina:** Business Intelligence e Big Data
**Stack:** Python & Pandas

Estrutura do projeto:

```
etl-transform-pandas/
├── README.md           # este documento
├── transform_iot.py    # código executável do Exercício 2
└── requirements.txt
```

---

## Exercício 1 — Teórico-Conceitual: ELT vs. ETL

### 1) Diferença fundamental no local e momento da transformação

| Aspecto | ETL | ELT |
|---|---|---|
| **Momento** | Transformação **antes** da carga | Transformação **depois** da carga |
| **Local** | Servidor/motor intermediário (staging area, Spark, Python/pandas, ferramenta ETL) | Dentro do próprio repositório analítico (Snowflake, BigQuery, Databricks), usando seu poder de processamento (SQL, dbt) |
| **O que é carregado** | Apenas dados já limpos e no schema final | Dado bruto (*raw*), que é transformado em camadas (ex.: bronze → silver → gold) |
| **Schema** | *Schema-on-write* | *Schema-on-read* / evolutivo |
| **Escalabilidade** | Limitada pelo servidor de transformação | Elástica, pois a computação e o armazenamento da nuvem são desacoplados |
| **Reprocessamento** | Exige reextrair da fonte se a regra mudar | Basta reexecutar a transformação sobre o dado bruto já armazenado |

Em resumo: no ETL, o repositório recebe só o dado tratado. No ELT, o repositório recebe o dado bruto e passa a ser também o motor de transformação.

### 2) Em qual cenário a governança e a anonimização prévia (LGPD) são mais críticas?

**No ELT.** Como o dado bruto é gravado no Data Lake/Warehouse *antes* de qualquer tratamento, CPFs, e-mails, telefones e demais PII passam a existir em claro no repositório. Isso gera riscos que o ETL evita por construção:

- **Ampliação da superfície de exposição:** qualquer usuário, job ou ferramenta com acesso à camada *raw* enxerga dados pessoais em claro.
- **Princípios da LGPD (art. 6º):** *necessidade* e *minimização* (só coletar o estritamente necessário) e *segurança* (medidas técnicas desde a concepção) são violados se o dado pessoal desnecessário pousa sem controle.
- **Direitos do titular (art. 18):** eliminação e anonimização ficam mais difíceis, porque o dado bruto se replica em várias camadas, *snapshots*, *backups* e *time travel*.
- **Auditoria e rastreabilidade:** é preciso saber quem acessou o quê, com linhagem de dados.

No ETL, a pseudonimização (por exemplo, o hash SHA-256 do CPF do estudo de caso) acontece na etapa T, e o repositório já recebe o dado protegido. Isso não elimina a responsabilidade de governança, mas o risco fica concentrado na staging area, que é temporária.

**Mitigações necessárias no ELT:** mascaramento dinâmico e criptografia em nível de coluna, controle de acesso por papel (RBAC/ABAC) com a camada *raw* restrita, tags de classificação de PII, políticas de retenção e expurgo da camada bruta, e, quando possível, pseudonimização já na ingestão (*tokenization at source*).

### 3) Dois casos em que o ETL clássico ainda é preferível

1. **Dados sensíveis sujeitos a regulação estrita (LGPD, saúde, financeiro):** quando a política exige que o dado pessoal identificável **nunca** pouse no repositório analítico (ou saia de uma jurisdição/rede), a anonimização e a remoção de PII devem ocorrer antes da carga. Exemplo: dados de prontuário que só podem chegar ao DW já pseudonimizados.
2. **Destino com poder de processamento limitado ou custo por consulta elevado, e cargas pequenas e bem definidas:** um Data Mart on-premises, um banco relacional tradicional ou um sistema legado sem capacidade elástica não comportam transformações pesadas. O mesmo vale quando o custo de computação em nuvem, cobrado por consulta/crédito, torna caro transformar repetidamente. Nesses casos, é mais eficiente transformar uma vez em um motor dedicado (Spark, SSIS, Informatica) e carregar só o resultado final, com schema fixo e qualidade de dados garantida.

Outros exemplos válidos: integração com sistemas legados de formato rígido, e redução do volume trafegado/armazenado ao carregar apenas o necessário.

---

## Exercício 2 — Prático em Python: Dados de Sensores IoT

### Código (`transform_iot.py`)

```python
import numpy as np
import pandas as pd

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
    print(transform_iot_data(df_raw).to_string(index=False))
```

### Resultado da execução

**Entrada (bruta):**

| sensor_id | timestamp | temperatura_c | pressao_bar |
|---|---|---|---|
| S1 | 2026-03-17 10:00:00 | 24.5 C | 1.01 |
| S2 | 17/03/2026 10:00 | 850.0 C | 1.05 |
| S1 | 2026-03-17 10:00:00 | 24.5 C | 1.01 |
| S3 | 2026-03-17 10:01:00 | None | 0.98 |
| S2 | 2026-03-17 10:02:00 | -10.2 C | 0.00 |

**Saída (transformada):**

| sensor_id | timestamp | temperatura_c | pressao_bar |
|---|---|---|---|
| S1 | 2026-03-17 10:00:00 | 24.5 | 1.01 |

### Rastreio linha a linha

| Linha | Destino | Motivo |
|---|---|---|
| 1 — S1 | ✅ Mantida | Válida (24.5 °C, 1.01 bar) |
| 2 — S2 | ❌ Descartada | **Outlier de temperatura:** 850 °C está fora de [-20, 100] |
| 3 — S1 | ❌ Descartada | **Duplicata** da linha 1 pela chave (sensor_id, timestamp) |
| 4 — S3 | ❌ Descartada | **Nulo sem base para imputar:** a mediana do S3 não existe, pois ele só tem essa leitura |
| 5 — S2 | ❌ Descartada | **Outlier de pressão:** 0.00 bar não é > 0.5 (a temperatura de -10.2 °C seria válida) |

### Decisões de projeto justificadas

- **Timestamps antes da deduplicação.** Formatos misturados fazem a mesma leitura parecer diferente para o `drop_duplicates`. Normalizar primeiro garante que a chave composta funcione.
- **Descartar outliers antes de imputar.** A mediana é robusta, mas só deve ser calculada sobre leituras fisicamente plausíveis.
- **Descarte em vez de imputação para o S3.** Preencher com a mediana global de outros sensores atribuiria a um equipamento a temperatura de outro. Em telemetria industrial, um valor nulo pode indicar falha do sensor, e mascará-lo é perigoso. Uma alternativa seria manter a linha com a flag `temperatura_imputada = True`, ou registrá-la em uma tabela de rejeitados (*dead-letter table*) para auditoria.
- **Limites de pressão.** O enunciado exige `> 0.5`, então o filtro é estrito (exatamente 0.5 é rejeitado).
- **Produção.** Em um pipeline real, convém gravar os registros descartados com o motivo da rejeição, em vez de simplesmente removê-los, para permitir monitoramento da qualidade dos dados.

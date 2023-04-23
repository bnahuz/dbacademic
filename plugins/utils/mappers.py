import pandas as pd
from hashlib import md5

def mapper_discentes(
        dataframe: pd.DataFrame,
        instituto: str,
        resource_id: str,
        nome_discente: str,
        matricula: str,
        sexo: str,
        ano_ingresso: str,
        periodo_ingresso: str,
        nome_curso: str
    ) -> pd.DataFrame:
    #create collum if not exists
    for col in [sexo, ano_ingresso, periodo_ingresso, nome_curso]:
        if col not in dataframe.columns:
            dataframe[col] = 0
    result = dataframe.apply(lambda df: pd.Series({
        'nome': str(df[nome_discente]).upper(),
        'id': md5((f"{df[matricula]}{instituto}{resource_id}").encode()).hexdigest(),
        'matricula': str(df[matricula]),
        'sexo': df[sexo],
        'data_ingresso': f"{str(df[ano_ingresso])}.{str(df[periodo_ingresso])}",
        'curso': df[nome_curso]
    }), axis = 1)
    return result

def mapper_docentes(
        dataframe: pd.DataFrame,
        instituto: str,
        resource_id: str,
        nome_docente: str,
        siape: str,
        sexo: str,
        ano_ingresso: str,
        periodo_ingresso: str,
        lotacao: str
    ) -> pd.DataFrame:
    for col in [sexo, ano_ingresso, periodo_ingresso, lotacao]:
        if col not in dataframe.columns:
            dataframe[col] = 0
    result = dataframe.apply(lambda df: pd.Series({
        'nome': str(df[nome_docente]).upper(),
        'id': md5((f"{df[siape]}{instituto}{resource_id}").encode()).hexdigest(),
        'siape': str(df[siape]),
        'sexo': df[sexo],
        'data_ingresso': f"{str(df[ano_ingresso])}.{str(df[periodo_ingresso])}",
        'lotacao': df[lotacao]
    }), axis = 1)
    return result

def mapper_cursos(
        dataframe: pd.DataFrame,
        instituto: str,
        resource_id: str,
        nome_curso: str,
        id_curso: str
    ) -> pd.DataFrame:
    result = dataframe.apply(lambda df: pd.Series({
        'nome': str(df[nome_curso]).upper(),
        'id': md5((f"{df[nome_curso]}{instituto}{resource_id}").encode()).hexdigest(),
        'codigo': str(df[id_curso]),
        'instituicao': instituto
    }), axis = 1)
    return result

def trunc_date(df: pd.DataFrame, coluna_datetime, type :str) -> pd.DataFrame:
    if type == 'ano_periodo':
        df[coluna_datetime] = pd.to_datetime(df[coluna_datetime], format='%Y.%m')
        df['ano_ingresso'] = df[coluna_datetime].dt.year
        df['periodo_ingresso'] = df[coluna_datetime].dt.month
        df['periodo_ingresso'] = df['periodo_ingresso'].apply(lambda x: 1 if x == 1 else 2)
    return df
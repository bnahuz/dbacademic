import pandas as pd
from hashlib import md5

def mapper_discentes(
        dataframe: pd.DataFrame,
        nome_discente: str,
        matricula: str,
        sexo: str,
        ano_ingresso: str,
        periodo_ingresso: str,
        nome_curso: str,
        instituto: str,
        resource_id: str
    ) -> pd.DataFrame:
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
        nome_docente: str,
        siape: str,
        sexo: str,
        ano_ingresso: str,
        periodo_ingresso: str,
        nome_curso: str,
        instituto: str,
        resource_id: str
    ) -> pd.DataFrame:
    result = dataframe.apply(lambda df: pd.Series({
        'nome': str(df[nome_docente]).upper(),
        'id': md5((f"{df[siape]}{instituto}{resource_id}").encode()).hexdigest(),
        'siape': str(df[siape]),
        'sexo': df[sexo],
        'data_ingresso': f"{str(df[ano_ingresso])}.{str(df[periodo_ingresso])}",
        'curso': df[nome_curso]
    }), axis = 1)
    return result

def mapper_cursos(
        dataframe: pd.DataFrame,
        nome_curso: str,
        id_curso: str,
        instituto: str,
        resource_id: str
    ) -> pd.DataFrame:
    result = dataframe.apply(lambda df: pd.Series({
        'nome': str(df[nome_curso]).upper(),
        'id': md5((f"{df[nome_curso]}{instituto}{resource_id}").encode()).hexdigest(),
        'codigo': str(df[id_curso]),
        'instituicao': instituto
    }), axis = 1)
    return result

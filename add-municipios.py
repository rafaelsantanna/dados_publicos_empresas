import sqlite3
import pandas as pd
from tqdm import tqdm

def carregar_municipios(conexao):
    consulta_municipios = "SELECT codigo, descricao FROM municipio"
    df_municipios = pd.read_sql_query(consulta_municipios, conexao)
    df_municipios['codigo'] = df_municipios['codigo'].astype(str)
    return df_municipios.set_index('codigo')['descricao'].to_dict()

def formatar_codigo_municipio(codigo):
    # Garantir que o código tenha 4 dígitos, adicionando zeros à esquerda se necessário
    return f"{codigo:0>4}"

def atualizar_municipios_no_arquivo(arquivo, municipios):
    try:
        df = pd.read_csv(arquivo, low_memory=False)
        df['municipio'] = df['municipio'].astype(str)

        # Formatar os códigos dos municípios para garantir 4 dígitos
        df['municipio_formatado'] = df['municipio'].apply(formatar_codigo_municipio)

        if 'municipio_formatado' in df.columns:
            df['nome_municipio'] = df['municipio_formatado'].map(municipios)
            df['nome_municipio'] = df['nome_municipio'].fillna('Não Encontrado')

            # Remover a coluna 'municipio_formatado' após o uso
            df.drop('municipio_formatado', axis=1, inplace=True)

        else:
            print(f"A coluna 'municipio_formatado' não foi encontrada em {arquivo}")

        df.to_csv(arquivo, index=False)
        print(f"Arquivo {arquivo} atualizado com sucesso.")
    except pd.errors.ParserError as e:
        print(f"Erro ao processar {arquivo}: {e}")

def main():
    conexao = sqlite3.connect('cnpj.db')
    municipios = carregar_municipios(conexao)
    conexao.close()

    arquivos = ['AC.csv', 'AL.csv', 'AP.csv', 'AM.csv', 'BA.csv', 'CE.csv', 'DF.csv', 
                'ES.csv', 'GO.csv', 'MA.csv', 'MT.csv', 'MS.csv', 'MG.csv', 'PA.csv', 
                'PB.csv', 'PR.csv', 'PE.csv', 'PI.csv', 'RJ.csv', 'RN.csv', 'RS.csv', 
                'RO.csv', 'RR.csv', 'SC.csv', 'SP.csv', 'SE.csv', 'TO.csv']

    for arquivo in tqdm(arquivos, desc="Processando arquivos"):
        atualizar_municipios_no_arquivo(arquivo, municipios)

if __name__ == '__main__':
    main()

import sqlite3
import pandas as pd
from tqdm import tqdm

def carregar_municipios(conexao):
    consulta_municipios = "SELECT codigo, descricao FROM municipio"
    df_municipios = pd.read_sql_query(consulta_municipios, conexao)
    return df_municipios.set_index('codigo')['descricao'].to_dict()

def atualizar_municipios_no_arquivo(arquivo, municipios):
    try:
        df = pd.read_csv(arquivo, low_memory=False)

        # Índice da coluna 'municipio'
        coluna_municipio = 27

        # Verifica se a coluna 'municipio' e 'nome_municipio' existem
        if 'municipio' in df.columns:
            nome_municipio = df['municipio'].map(municipios).fillna(df['municipio'])

            if 'nome_municipio' in df.columns:
                # Atualiza a coluna existente
                df['nome_municipio'] = nome_municipio
            else:
                # Insere a nova coluna
                df.insert(coluna_municipio + 1, 'nome_municipio', nome_municipio)
        else:
            print(f"A coluna 'municipio' não foi encontrada em {arquivo}")

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

import sqlite3
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import time

def process_chunk(chunk):
    inicio = time.time()

    # Substituindo ";" por "|" em todas as colunas do DataFrame (se necessário)
    chunk = chunk.replace(";", "|", regex=True)
    
    # Ajustar tipos de dados
    chunk['cnpj_basico'] = chunk['cnpj_basico'].astype(str)
    chunk['natureza_juridica'] = chunk['natureza_juridica'].astype(int)
    chunk['qualificacao_responsavel'] = chunk['qualificacao_responsavel'].astype(int)

    # Operações vetorizadas para manipulação de strings
    cnae_cols = chunk['cnae_fiscal_secundaria'].str.split(',', expand=True).iloc[:, :5]
    cnae_cols.rename(columns=lambda x: f'cnae_fiscal_secundaria_{x+1}', inplace=True)
    chunk = pd.concat([chunk, cnae_cols], axis=1)

    fim = time.time()
    print(f"Chunk processado em {fim - inicio:.2f} segundos.")
    return chunk

def parallel_processing(consulta, conexao, chunksize, n_cores):
    chunks = pd.read_sql_query(consulta, conexao, chunksize=chunksize)
    with ProcessPoolExecutor(max_workers=n_cores) as executor:
        result = executor.map(process_chunk, chunks)
    return pd.concat(result)

def main():
    conexao = sqlite3.connect('cnpj.db')
    
    estados = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 
               'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 
               'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 
               'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

    for estado in estados:
        consulta = f"""
        SELECT 
          e.cnpj_basico,
          e.razao_social,
          e.natureza_juridica,
          e.qualificacao_responsavel,
          e.porte_empresa,
          e.ente_federativo_responsavel, 
          e.capital_social,
          es.*,
          c.descricao AS cnae_descricao,
          m.descricao AS nome_municipio
        FROM 
          empresas e
        INNER JOIN 
          estabelecimento es ON e.cnpj_basico = es.cnpj_basico
        LEFT JOIN 
          cnae c ON es.cnae_fiscal = c.codigo
        LEFT JOIN 
          municipio m ON es.municipio = m.codigo
        WHERE
          es.uf = '{estado}'"""

        n_cores = 4

        print(f"Iniciando processamento para o estado {estado}.")
        processed_data = parallel_processing(consulta, conexao, 10000, n_cores)

        if processed_data.empty:
            print(f"Não há mais dados para o estado {estado}. Passando para o próximo.")
            continue  # Passa para o próximo estado se não houver mais dados

        file_name = f'{estado}.csv'
        processed_data.to_csv(file_name, index=False, header=True)
        print(f"Processamento para o estado {estado} concluído. Dados salvos em {file_name}")

    conexao.close()

if __name__ == '__main__':
    main()

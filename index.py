import sqlite3
import pandas as pd
from multiprocessing import Pool
import time

def process_chunk(chunk):
    inicio = time.time()

    # Operações vetorizadas para manipulação de strings
    cnae_cols = chunk['cnae_fiscal_secundaria'].str.split(',', expand=True).iloc[:, :5]
    cnae_cols.rename(columns=lambda x: f'cnae_fiscal_secundaria_{x+1}', inplace=True)
    chunk = pd.concat([chunk, cnae_cols], axis=1)

    fim = time.time()
    print(f"Chunk processado em {fim - inicio:.2f} segundos.")
    return chunk

def parallel_processing(consulta, conexao, chunksize, n_cores):
    with Pool(n_cores) as pool:
        chunks = pd.read_sql_query(consulta, conexao, chunksize=chunksize)
        result = pool.map(process_chunk, chunks)
        return pd.concat(result)

def main():
    conexao = sqlite3.connect('cnpj.db')
    estados = ['AC.csv', 'AL.csv', 'AP.csv', 'AM.csv', 'BA.csv', 'CE.csv', 'DF.csv', 
                'ES.csv', 'GO.csv', 'MA.csv', 'MT.csv', 'MS.csv', 'MG.csv', 'PA.csv', 
                'PB.csv', 'PR.csv', 'PE.csv', 'PI.csv', 'RJ.csv', 'RN.csv', 'RS.csv', 
                'RO.csv', 'RR.csv', 'SC.csv', 'SP.csv', 'SE.csv', 'TO.csv']
    max_lines = 6400000  # Define o número máximo de linhas por arquivo

    for estado in estados:
        file_counter = 0
        offset = 0  # Inicia o offset em 0
        while True:
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
              es.uf = '{estado}'
            LIMIT {max_lines} OFFSET {offset}"""

            n_cores = 4

            print(f"Iniciando processamento para o estado {estado}, arquivo {file_counter}.")
            processed_data = parallel_processing(consulta, conexao, 10000, n_cores)

            if processed_data.empty:
                break  # Se não houver mais dados, encerra o loop

            file_name = f'{estado}-{file_counter}.csv'
            processed_data.to_csv(file_name, index=False, header=True)
            print(f"Processamento para o estado {estado}, arquivo {file_counter} concluído. Dados salvos em {file_name}")

            file_counter += 1
            offset += max_lines  # Aumenta o offset para a próxima iteração

    conexao.close()

if __name__ == '__main__':
    main()

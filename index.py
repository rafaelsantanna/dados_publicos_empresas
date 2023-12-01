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
    # estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
    estados = ['SP', 'SE', 'TO']

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

        n_cores = 4  # Ajuste isso de acordo com a sua máquina

        print(f"Iniciando processamento para o estado {estado}.")
        processed_data = parallel_processing(consulta, conexao, 10000, n_cores)

        processed_data.to_csv(f'{estado}.csv', mode='a', index=False, header=True)
        print(f"Processamento para o estado {estado} concluído. Dados salvos em {estado}.csv")

    conexao.close()

if __name__ == '__main__':
    main()

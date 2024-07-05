import sqlite3
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import time
import logging

# Configuração do logging
logging.basicConfig(filename='processamento.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

def process_chunk(chunk):
    try:
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
        logging.info(f"Chunk processado em {fim - inicio:.2f} segundos.")
        return chunk
    except Exception as e:
        logging.error(f"Erro ao processar chunk: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

def parallel_processing(consulta, conexao, chunksize, n_cores):
    try:
        chunks = []
        for chunk in pd.read_sql_query(consulta, conexao, chunksize=chunksize):
            chunks.append(chunk)
        
        with ProcessPoolExecutor(max_workers=n_cores) as executor:
            result = executor.map(process_chunk, chunks)
        return pd.concat(result)
    except Exception as e:
        logging.error(f"Erro no processamento paralelo: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

def main():
    try:
        conexao = sqlite3.connect('cnpj.db')
        
        # Melhorias de configuração do SQLite
        conexao.execute('PRAGMA journal_mode=WAL;')
        conexao.execute('PRAGMA cache_size = 10000;')
        conexao.execute('PRAGMA synchronous = OFF;')
        conexao.execute('ANALYZE;')
        conexao.commit()
        
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
            chunksize = 100000  # Define o tamanho do chunk

            logging.info(f"Iniciando processamento para o estado {estado}.")
            processed_data = parallel_processing(consulta, conexao, chunksize, n_cores)

            if processed_data.empty:
                logging.info(f"Não há mais dados para o estado {estado}. Passando para o próximo.")
                continue  # Passa para o próximo estado se não houver mais dados

            file_name = f'{estado}.csv'
            processed_data.to_csv(file_name, index=False, header=True)
            logging.info(f"Processamento para o estado {estado} concluído. Dados salvos em {file_name}")

        conexao.close()
    except Exception as e:
        logging.error(f"Erro no processamento principal: {e}")

if __name__ == '__main__':
    main()

import sqlite3
import pandas as pd
from tqdm import tqdm
import time

# Conectar ao banco de dados SQLite
conexao = sqlite3.connect('cnpj.db')

# Lista dos estados brasileiros
# estados = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']
estados = ['DF']

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
      c.descricao AS cnae_descricao
    FROM 
      empresas e
    INNER JOIN 
      estabelecimento es ON e.cnpj_basico = es.cnpj_basico
    LEFT JOIN 
      cnae c ON es.cnae_fiscal = c.codigo
    WHERE
      es.uf = '{estado}'"""

    # Contar quantos registros temos para esse estado
    total_registros = pd.read_sql_query(f"SELECT COUNT(*) FROM ({consulta})", conexao).iloc[0,0]
    pbar = tqdm(total=total_registros, desc=f"Processando {estado}")

    for chunk in pd.read_sql_query(consulta, conexao, chunksize=50000):  
        inicio_chunk = time.time()

        # Dividir o campo 'cnae_fiscal_secundaria' e criar novas colunas
        cnae_cols = chunk['cnae_fiscal_secundaria'].str.split(',', expand=True)
        cnae_cols.rename(columns=lambda x: f'cnae_fiscal_secundaria_{x+1}', inplace=True)
        
        # Concatenar as novas colunas com o chunk original
        chunk = pd.concat([chunk, cnae_cols], axis=1)

        chunk.to_csv(f'{estado}.csv', mode='a', index=False, header=not bool(chunk.index.start))
        pbar.update(len(chunk))
        fim_chunk = time.time()
        print(f"Chunk processado em {fim_chunk - inicio_chunk:.2f} segundos.")

    pbar.close()

conexao.close()

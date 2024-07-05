import os
import pandas as pd
from tqdm import tqdm

# Defina o diretório onde estão os arquivos CSV
directory = "./"

# Lista de todos os arquivos CSV no diretório
csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]

# DataFrame vazio para armazenar os resultados
result = pd.DataFrame(columns=['CNAE', 'Estado', 'Quantidade'])

# Colunas que queremos ler e seus tipos de dados esperados
usecols = [
    'uf', 'cnae_fiscal', 'situacao_cadastral'
]

dtypes = {
    'uf': 'object',
    'cnae_fiscal': 'object',
    'situacao_cadastral': 'int64'
}

# Loop para ler cada arquivo CSV, filtrar os dados e adicionar ao DataFrame
for file in tqdm(csv_files, desc="Processando arquivos CSV"):
    file_path = os.path.join(directory, file)
    
    try:
        # Ler o arquivo CSV com pandas especificando colunas e tipos de dados
        data = pd.read_csv(file_path, usecols=usecols, dtype=dtypes, encoding='utf-8')
        
        # Filtra os dados onde situacao_cadastral == 2
        filtered_data = data[data['situacao_cadastral'] == 2]
        
        # Contar a quantidade de CNAEs por estado
        count_data = filtered_data.groupby(['cnae_fiscal', 'uf']).size().reset_index(name='Quantidade')
        
        # Renomear as colunas para corresponder ao formato desejado
        count_data.columns = ['CNAE', 'Estado', 'Quantidade']
        
        # Concatenar com o DataFrame resultante
        result = pd.concat([result, count_data], ignore_index=True)
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path}: {e}")

# Salvar o resultado consolidado em um novo arquivo CSV
output_path = os.path.join(directory, 'filtered_data_consolidated.csv')
result.to_csv(output_path, index=False, encoding='utf-8')
print(f"Dados filtrados e consolidados salvos em: {output_path}")

# Processador de Dados de CNPJ

Este projeto é um pipeline de processamento de dados projetado para automatizar a recuperação, normalização e segregação dos dados públicos de CNPJ da Receita Federal do Brasil. Ele baixa os dados brutos, processa-os em um banco de dados SQLite estruturado e, subsequentemente, gera arquivos CSV separados e limpos para cada estado brasileiro, além de enriquecer o conjunto de dados com informações de município.

## Funcionalidades

-   **Aquisição de Dados**: Automatiza o download dos arquivos de dados de CNPJ das fontes oficiais do governo.
-   **Integração com Banco de Dados**: Utiliza um robusto banco de dados SQLite para a ingestão inicial e manipulação dos dados.
-   **Segregação por Estado**: Divide eficientemente o conjunto de dados nacional consolidado em arquivos CSV individuais para cada estado.
-   **Enriquecimento de Dados**: Aumenta os dados de saída com códigos e nomes de municípios usando o script `add-municipio.py`.
-   **Saída Padrão e Consistente**: Garante cabeçalhos de CSV consistentes em todos os arquivos de estado, seguindo o padrão do arquivo `SP.csv`.

## Fluxo do Projeto

O projeto foi projetado para ser executado de forma sequencial:

1.  **Download**: Baixe os arquivos de dados brutos necessários de `dadosabertos.rfb.gov.br/CNPJ/`.
2.  **Ingestão**: Execute o script `dados_cnpj_para_sqlite.py` para processar os arquivos brutos e criar o banco de dados consolidado `CNPJs.db`.
3.  **Segregação**: Execute o script de segregação por estado para gerar arquivos CSV individuais para cada estado (ex: `RJ.csv`, `SP.csv`). O sistema lida com arquivos grandes como os de SP sem problemas.
4.  **Enriquecimento**: Execute o script `add-municipio.py` para adicionar os detalhes dos municípios aos arquivos de estado gerados.

## Pré-requisitos

-   Python 3.x
-   SQLite 3.x
-   `DB Browser for SQLite` (para inspeção manual, opcional): [https://sqlitebrowser.org/](https://sqlitebrowser.org/)

## Configuração e Uso

bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/cnpj_data_processor.git
cd cnpj_data_processor

# 2. (Recomendado) Crie e ative um ambiente virtual Python
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate

# 3. Instale as dependências necessárias (se houver)
pip install -r requirements.txt

# 4. Execute os scripts de processamento de dados em ordem
python dados_cnpj_para_sqlite.py
# ... execute o script de segregação por estado ...
python add-municipio.py


## Estrutura do Repositório


cnpj_data_processor/
│
├── dados_cnpj_para_sqlite.py   # Script principal para converter dados brutos para SQLite
├── add-municipio.py            # Script para adicionar dados de município
├── CNPJs.db                    # Banco de dados SQLite gerado (após o passo 2)
├── *.csv                       # Arquivos CSV gerados por estado (após o passo 3)
├── README.md                   # Documentação do projeto (Inglês)
└── README.pt.md                # Documentação do projeto (Português)

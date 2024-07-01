https://github.com/rictom/cnpj-sqlite
https://dadosabertos.rfb.gov.br/CNPJ/
DB Browser for SQLite: https://sqlitebrowser.org/

-- Separar os arquivos por estados: RJ.csv, SP.csv, MG.csv;
-- Gerar CSV ou XLSX com colunas especificas(as colunas estão no arquivo XLSX);
-- Começar por um estado pequeno, pode ser AMAPA;
-- Separar informações da coluna CNAE em mais colunas(Olhar workana)


TODO: 
- Acessar o link htpp://dadosabertos.rfb.gov.br/CNPJ/ e baixar os arquivos de CNPJs;
- Rodar o script dados_cnpj_para_sqlite.py do link https://github.com/rictom/cnpj-sqlite para gerar o arquivo CNPJs.db;
- Depois que gerar o arquivo CNPJs.db, rode o script para gerar os arquivos por cada estado;
- Se atentar para os cabeçalhos serem iguais em todos os arquivos, pode usar o padrão do arquivo SP.csv;
- Depois de ter os arquivos por cada estado, então rode o script para adicionar municipos: add-municipio.py;
- Colocar o Estado SP em um unico arquivo, não precisa mais separar o arquivo caso ele seja grande;
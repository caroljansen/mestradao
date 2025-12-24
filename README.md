# Sobre

Este repositório de código contém bases de dados, artefatos de código de processamento de dados, e também dá origem ao [site interativo](https://caroljansen.github.io/mestradao/apps/mestrado_carol.html) para exploração de dados da pesquisa de mestrado de Carolina Jansen Gandara Mendes.

A base de dados quantitativos foi elaborada por Carolina Jansen Gandara Mendes, [Caio Elmôr Lang](https://www.linkedin.com/in/caiolang/) e [Ana Kellen Nogueira Campelo](https://www.linkedin.com/in/anaknog/).

## Artefatos de código

### Sobre os artefatos de código
Ambos os artefatos de código usaram a linguagem Python[^python] com uma série de bibliotecas, sendo que a reatividade das visualizações se deve à bibioteca Marimo[^marimo].

- `apps/data_prep.py` : Contém o processamento de dados desde as bases brutas até as bases utilizadas na pesquisa, aplicando premissas de análise para seleção de famílias e respostas, fazendo padronização de respostas, etc.

- `apps/data_viz.py` : Contém o código que gera visualizações de dados interativas, consumindo as bases geradas em `data_prep.py`.

[^python]:
    Python Software Foundation, https://www.python.org/

[^marimo]:
    Marimo, https://github.com/marimo-team/marimo

### Rodando os artefatos de código
Recomenda-se instalar o [`uv`](https://docs.astral.sh/uv/#installation) gerenciador de projetos e bibliotecas Python.

Com `uv` instalado, os seguintes comandos devem funcionar:

#### Para abrir a interface de edição Marimo
Para a etapa de limpeza de dados:

    uv run marimo edit apps/data_prep.py

ou, para a visualização de dados:

    uv run marimo edit apps/data_viz.py

#### Para rodar a etapa de limpeza de dados como script python simples
    uv run apps/data_prep.py

## Sobre as bases

### Usadas para outras análises da pesquisa
- `base_wide.csv` : Base em formato wide, com a chave única `id_family_datalake` como identificadora da família,e cada pergunta em colunas `<pergunta>_FIRST` e `<pergunta>_LAST` indicando a primeira e última resposta da família para a pergunta em questão. A coluna `FavelaID` indica a favela.

- `base_log.csv` : Base auxiliar que indica qual o tempo (T0, ..., T3) usado para cada resposta X família incluída na `base_wide.csv`.

### Usadas para visualização de dados
Para minimizar a carga computacional do site com as visualizações, algumas transformações da base são feitas em `data_prep.py`, e as bases resultantes são armazenadas como um *cache*, para serem consumidas em `data_viz.py`.

- `base_long.csv` : Base em formato long (uma row por resposta de cada família, ao invés de uma row por família / uma coluna por pergunta). Usada em `data_viz.py`.

- `base_exploded.csv` : Base em formato long, porém cada resposta de perguntas multi-asserções aparece em uma linha separada.  Usada em `data_viz.py`.

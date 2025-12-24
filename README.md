# Sobre

Este repositório reúne os dados e os artefatos de código da pesquisa de mestrado de Carolina Jansen Gandara Mendes, além de gerar o [site interativo para exploração dos resultados](https://caroljansen.github.io/mestradao/apps/data_viz.html).

As bases de dados quantitativos foram preparadas por Carolina Jansen Gandara Mendes, Caio Elmôr Lang e Ana Kellen Nogueira Campelo.

**Conteúdo principal**

- Código de preparação de dados: `apps/data_prep.py`
- Código de visualização interativa: `apps/data_viz.py`

## Requisitos

- Instalar o gerenciador [`uv`](https://docs.astral.sh/uv/#installation), usado para administrar os ambientes virtuais, instalação de bibliotecas e versões de Python, além de executar os fluxos de trabalho.

## Como usar

1) Abrir a interface Marimo (edição interativa)

- Para editar o fluxo de limpeza de dados:

```
uv run marimo edit apps/data_prep.py
```

- Para editar/visualizar as visualizações interativas:

```
uv run marimo edit apps/data_viz.py
```

2) Executar os scripts como scripts Python

- Executar a etapa de preparação de dados (gera `base_long.csv`, `base_exploded.csv` e caches usados pelas visualizações):

```
uv run apps/data_prep.py
```

3) Gerar uma versão estática do site localmente (em `_site/`)

```
uv run build.py \
       --output_dir '_site' \
       --template 'templates/tailwind.html.j2'
```

## Estrutura das bases

As saídas geradas por `apps/data_prep.py` são usadas por `apps/data_viz.py`.

- `base_wide.csv`: formato wide — uma linha por família; chave única `id_family_datalake`; perguntas multi-temporais aparecem como `<pergunta>_FIRST` e `<pergunta>_LAST`. A coluna `FavelaID` indica a favela.
- `base_log.csv`: base auxiliar que mapeia qual tempo (T0, ..., T3) foi usado para cada resposta da família incluída em `base_wide.csv`.
- `base_long.csv`: formato long — uma linha por resposta/família; usada nas visualizações.
- `base_exploded.csv`: formato long com respostas multi-asserções explodidas em linhas separadas; usada nas visualizações.

## Notas técnicas

- O código foi desenvolvido usando o ambiente de *notebooks* interativos Marimo[^marimo].

- O processamento de dados usou a biblioteca Polars[^polars].

[^python]:
    Python Software Foundation, https://www.python.org/

[^marimo]:
    Marimo, https://github.com/marimo-team/marimo

[^polars]:
    Polars, https://pola.rs/
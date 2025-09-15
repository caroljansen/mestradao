import marimo

__generated_with = "0.15.2"
app = marimo.App(width="columns", app_title="Mestrado Carolina Jansen")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Mestrado""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Estrutura

    Nessa página, estão os dados quantitativos da pesquisa de mestrado que foram produzidos e organizados pela Carolina Jansen e pelo Caio Lang.

    Há informações sobre a base de dados, sobre o Índice Gerando Falcões das famílias e as respostas delas às perguntas do programa Favela 3D.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Base""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Distribuição de tempo inicial e final

    Famílias cuja diferença entre o tempo inicial e final era de apenas uma unidade (0 ➜ 1, 1 ➜ 2, 2 ➜ 3) totalizavam apenas 57 famílias na base inicial, e foram removidas da análise.
    """
    )
    return


@app.cell(hide_code=True)
def _(GT, df_long_periods, loc, md, pl, style):
    # Create the dataframe with the time distributions
    time_distribution = (
        df_long_periods.select("id_family_datalake", "time_first", "time_last")
        .group_by("*")
        .len()
        .select("time_first", "time_last")
        .group_by("*")
        .len("count")
        .sort("count", descending=True)
        .with_columns(
            percentage=(pl.col("count") / pl.col("count").sum() * 100)
            .round(2)
            .cast(pl.Utf8)
            + pl.lit(" %")
        )
    )

    # Create a GreatTables table
    (
        GT(time_distribution)
        .tab_header(
            title=md("Distribuição de Tempo Inicial e Final"),
            subtitle=md(
                "Contagem de famílias por combinação de períodos de coleta"
            ),
        )
        .cols_label(
            time_first=md("Tempo Inicial"),
            time_last=md("Tempo Final"),
            count=md("Contagem"),
            percentage=md("Percentual"),
        )
        .tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(columns=["percentage"]),
        )
        .tab_style(
            style=style.text(align="right"),
            locations=loc.body(columns=["count", "percentage"]),
        )
        .tab_style(
            style=style.text(weight="bold", size="large"),
            locations=loc.column_labels(),
        )
        .opt_stylize(style=1, color="blue")
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Número de respondentes por favela""")
    return


@app.cell(hide_code=True)
def _(GT, df_plot_variables, loc, md, style):
    # Create the dataframe with the time distributions
    # Não precisa segregar por tempo, então removemos a coluna "time"
    respondentes_por_favela = (
        df_plot_variables.select("FavelaID", "id_family_datalake")
        .unique()
        .group_by("FavelaID")
        .len()
        .rename({"len": "n_familias"})
        .sort("FavelaID")
    )
    # Agrupa por FavelaID e tempo, conta o número de famílias respondentes em cada tempo por favela
    # respondentes_por_favela = (
    #     df_plot_variables
    #     .select("FavelaID", "id_family_datalake", "time")
    #     .unique()
    #     .group_by("FavelaID", "time")
    #     .len()
    #     .rename({"len": "n_familias"})
    #     .sort(["FavelaID", "time"])
    # )

    # Cria a tabela com GreatTables
    (
        GT(respondentes_por_favela)
        .tab_header(
            title=md("Número de famílias respondentes por favela"),
            # subtitle=md("Contagem de famílias por favela e por tempo de coleta"),
        )
        .cols_label(
            FavelaID=md("Favela"),
            # time=md("Tempo"),
            n_familias=md("Nº de Famílias"),
        )
        .tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(columns=["n_familias"]),
        )
        .tab_style(
            style=style.text(align="right"),
            locations=loc.body(columns=["n_familias"]),
        )
        .tab_style(
            style=style.text(weight="bold", size="large"),
            locations=loc.column_labels(),
        )
        .opt_stylize(style=1, color="blue")
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Dados faltantes


    Diante do caráter experimental do Favela 3D, há uma quantidade considerável de perguntas que não foram respondidas por algumas famílias. Na tabela, é apresentado o percentual de NAs para cada pergunta no tempo inicial e final.
    """
    )
    return


@app.cell(hide_code=True)
def _(name_dict):
    THRESH_NA = 50


    def na_table(
        df_long,
        question_names: list,
        percentage=False,
        color_col="time",
        filter_by_order=False,
    ):
        from great_tables import GT, md, style, loc
        import polars as pl

        # Aggregate by question, time, and answer, and count occurrences
        df_plot = (
            df_long.filter(pl.col("question").is_in(question_names))
            .group_by(["question", "time", "answer"])
            .len()
            .with_columns(
                total_per_question_time=pl.col("len")
                .sum()
                .over(["question", "time"])
            )
            .with_columns(
                percentage=(
                    pl.col("len") / pl.col("total_per_question_time") * 100
                )
            )
            .select(["question", "time", "answer", "len", "percentage"])
        )

        #
        first_df = df_plot.filter(pl.col("time") == "FIRST")
        last_df = df_plot.filter(pl.col("time") == "LAST")
        _df = (
            first_df.join(
                last_df,
                on=["question", "answer"],
                how="full",
                # suffixes=("_FIRST", "_LAST"),
            )
            .fill_null(0)
            .filter(pl.col("answer") == "NA")
            .select(
                pl.col.question,
                # pl.col.answer,
                pl.col.len.alias("count_first"),
                pl.col.percentage.alias("pct_first"),
                pl.col.len_right.alias("count_last"),
                pl.col.percentage_right.alias("pct_last"),
            )
        )

        # Handle questions without NAs
        all_questions = set(question_names)
        questions_in_df = set(_df["question"].to_list())
        missing_questions = list(all_questions - questions_in_df)

        missing_questions
        if missing_questions:
            missing_data = pl.DataFrame(
                {
                    "question": missing_questions,
                    # "answer": ["NA"] * len(missing_questions),
                    "count_first": [0] * len(missing_questions),
                    "pct_first": [0.0] * len(missing_questions),
                    "count_last": [0] * len(missing_questions),
                    "pct_last": [0.0] * len(missing_questions),
                }
            ).with_columns(
                count_first=pl.col.count_first.cast(pl.UInt32),
                count_last=pl.col.count_last.cast(pl.UInt32),
            )
            _df = pl.concat([_df, missing_data])

        _df = _df.sort("count_first", descending=True)
        question_name_df = pl.DataFrame(
            dict(key=name_dict.keys(), value=name_dict.values())
        )
        _df = (
            _df.join(
                question_name_df, left_on="question", right_on="key", how="left"
            )
            .with_columns(
                question_id=pl.col("question"),
                question=pl.col("value"),
            )
            .drop("value")
            # .rename({"value": "question"})
        )
        # Create the GreatTable
        gt_table = (
            GT(_df)
            .cols_move_to_start(columns=["question_id", "question"])
            .tab_header(
                title=md("Percentual de NAs por Pergunta"),
                subtitle=md("Percentual de respostas NA por pergunta e tempo"),
            )
            .cols_label(
                question=md("Pergunta"),
                question_id=md("ID da Pergunta"),
                count_first=md("Núm. de NAs"),
                count_last=md("Núm. de NAs"),
                pct_first=md("% de NAs"),
                pct_last=md("% de NAs"),
                # **{
                #     col: md(col.replace("@", " @ "))
                #     for col in _df.columns
                #     if col != "question"
                # },
            )
            .tab_spanner(
                label=md("**Tempo INICIAL**"),
                columns=[
                    "count_first",
                    "pct_first",
                ],
            )
            .tab_spanner(
                label=md("**Tempo FINAL**"),
                columns=[
                    "count_last",
                    "pct_last",
                ],
            )
            .fmt_number(
                columns=["pct_first", "pct_last"],
                decimals=2,
                pattern="{x}%",
            )
            .opt_stylize(style=1, color="blue")
            .tab_style(
                style=style.text(size="14px"),
                locations=loc.body(columns=["question_id", "question"]),
            )
            .tab_style(
                style=style.text(weight="bold"),
                locations=loc.body(columns=["question"]),
            )
            .tab_style(
                style=style.text(align="right"),
                locations=loc.body(
                    columns=[
                        "count_first",
                        "count_last",
                        "pct_first",
                        "pct_last",
                    ]
                ),
            )
            # .tab_style(
            #     style.fill("#bde3c0"),
            #     loc.body(
            #         # columns=cs.starts_with("cha"),
            #         rows=pl.col("pct_first")<THRESH_NA,
            #     ),
            # )
            .tab_style(
                style.fill("#F5C8B4"),
                loc.body(
                    # columns=cs.starts_with("cha"),
                    rows=(pl.col("pct_first") > THRESH_NA)
                    | (pl.col("pct_last") > THRESH_NA),
                ),
            )
        )

        return gt_table
    return (na_table,)


@app.cell
def _(df_plot_variables, mo, na_table, questions):
    mo.accordion({"Tabela de NAs": na_table(df_plot_variables, questions)})
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Análises

    ### Análise 1 - Mobilidade entre categorias do IGF

    O **Índice Gerando Falcões (IGF)** é uma ferramenta para medir pobreza e dignidade de famílias e territórios de forma multidimensional. A régua do IGF, das categorias de maior pobreza e vulnerabilidade à melhor categoria, vai de extrema pobreza 1 (`E1`), extrema pobreza 2 (`E2`), pobreza 1 (`P1`), pobreza 2 (`P2`) e dignidade (`D`). 

    `E1` e `E2` representam situações em que a família vive **risco diverso e crônico**, `P1` e `P2` são situações de **pobreza**, com **menor risco mas indignas** e `D` são famílias com o **mínimo existencial**, que permite uma vida **sem riscos iminentes**.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo, name_dict, petals_cats):
    _options = {v: k for k, v in name_dict.items() if k in petals_cats}

    petal_to_plot = mo.ui.dropdown(
        options=_options,
        value=list(_options.keys())[0],
        label="Escolha uma categoria do IGF: ",
        searchable=True,
    )
    petal_to_plot
    return (petal_to_plot,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""#### Transição entre categorias do IGF""")
    return


@app.cell(hide_code=True)
def _(df_long_first, df_long_last, name_dict, petal_to_plot, pl, px):
    igf_color_dict = dict(
        value=[
            "E1",
            "E2",
            "P1",
            "P2",
            "D",
            "NA",
        ],
        color=[
            "#EB5133",
            "#EB7E69",
            "#F7F7A1",
            "#72B7B2",
            "#3366CC",
            "#6E899C",
            # "#EB5133",
            # "#EB7E69",
            # "#F7F7A1",
            # "#F7CFA1",
            # "#3CF6D7",
            # "#B8B8B8",
        ],
    )

    df_colors = pl.DataFrame(igf_color_dict)

    question_name = petal_to_plot.value
    df_plot = (
        df_long_first.filter(pl.col("question") == question_name)
        .with_columns(first_answer="answer")
        .select("id_family_datalake", "question", "first_answer")
        .join(df_long_last, on=["id_family_datalake", "question"], how="left")
        .with_columns(last_answer="answer")
        .select("id_family_datalake", "question", "first_answer", "last_answer")
    ).join(df_colors, left_on="last_answer", right_on="value")

    fig = px.parallel_categories(
        df_plot.select("first_answer", "last_answer", "color").sort(
            "first_answer", "last_answer"
        ),
        dimensions=["first_answer", "last_answer"],
        title=f"<b>{name_dict.get(question_name)}</b>",
        subtitle="Cores de acordo com a última resposta",
        color="color",
        labels={
            "first_answer": "Primeira resposta da família",
            "last_answer": "Última resposta da família",
        },
    )
    fig.update_layout(margin=dict(t=100, l=100, r=100, b=100))
    fig
    # fig.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Análise 2 - Categorias do IGF no tempo
    Quantas famílias em número absoluto e porcentual estão em cada categoria da régua do IGF (E1, E2, P1, P2, D) em cada tempo?
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    pct_2 = mo.ui.switch(value=True, label=f"Percentual")
    pct_2
    return (pct_2,)


@app.cell
def _(df_plot_variables, pct_2, petal_to_plot, plot_variables, px):
    plot_variables(
        df_plot_variables,
        [petal_to_plot.value],
        seggregate_favela=False,
        orientation="v",
        palette=px.colors.qualitative.Pastel,
        percentage=pct_2.value,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Análise 3 - Nota média do IGF
    Qual a nota que representa a média das dimensões do IGF no tempo inicial e final de cada família?
    """
    )
    return


@app.cell(hide_code=True)
def _(df_long_first, df_long_last, pl, px):
    def parallel_plot(petal="AverageIGF"):
        cols = ["answer_first", "answer_last"]

        legends = [
            f"Média de <b>{petal}</b> (T {time})" for time in ["inicial", "final"]
        ]

        df_plot = (
            df_long_first.join(
                df_long_last, how="left", on=["id_family_datalake", "question"]
            )
            .filter((pl.col("question") == petal) & (pl.col("answer") != "NA"))
            .select(
                pl.col("answer").alias("answer_first"),
                pl.col("answer_right").alias("answer_last"),
            )
        )
        df_plot = df_plot.select(
            [pl.col(col_name).cast(pl.Float32) for col_name in cols]
        )

        fig = px.parallel_coordinates(
            df_plot,
            color=cols[-1],
            title="Média da nota do IGF de cada família no tempo inicial e final",
            labels={k: v for k, v in zip(cols, legends)},
            color_continuous_scale=[
                "rgb(178,24,43)",
                "rgb(215, 157, 20)",
                "rgb(245, 237, 141)",
                "rgb(18, 152, 103)",
                "rgb(0, 136, 16)",
            ],
            height=400,
        )

        fig.update_layout(
            margin=dict(l=160, r=60, t=100, b=40),
            # paper_bgcolor="LightSteelBlue",
        )
        return fig


    parallel_plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Análise 4 - Respostas às perguntas

    Quais as respostas dadas pelas famílias às perguntas do Programa em cada tempo?
    """
    )
    return


@app.cell
def _(mo):
    pct_4 = mo.ui.switch(value=True, label=f"Percentual")
    pct_4
    return (pct_4,)


@app.cell(hide_code=True)
def _(mo):
    mo.accordion(
        {
            "Pendências": """
    - Contagem de número de respostas positivas/negativas (doenças, problemas na casa, acesso à internet, etc) precisam de uma re-normalização para ter uma row por família
    - O `BathroomQualit` também precisa juntar as respostas do T0 que estão em várias colunas
    """
        }
    )
    return


@app.cell(hide_code=True)
def _(mo, name_dict, questions, wip_multiple_assertions, wip_other):
    _question_names = sorted(
        list((set(questions) - set(wip_other)) | set(wip_multiple_assertions))
    )
    _options = {v: k for k, v in name_dict.items() if k in _question_names}

    question_to_plot = mo.ui.dropdown(
        options=_options,
        value=list(_options.keys())[0],
        label="Escolha uma pergunta: ",
        searchable=True,
    )
    question_to_plot
    return (question_to_plot,)


@app.cell
def _(df_plot_variables, pct_4, plot_variables, px, question_to_plot):
    plot_variables(
        df_plot_variables,
        # question_names=sorted(
        #     set(wip_multiple_assertions)
        # ),
        question_names=[question_to_plot.value],
        seggregate_favela=False,
        orientation="v",
        # orientation="h",
        palette=px.colors.qualitative.Pastel,
        percentage=pct_4.value,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Análise 5 - Drogadição, alcoolismo e violências

    No Programa, eu me reuni com cada assistente social de cada território e fomos checando em todas as famílias participantes aquelas com casos entre os familiares de drogadição, alcoolismo, violência contra mulher e violência contra crianças.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    #### Associação

    Usamos abaixo o V de Cramér para computar a força de associação entre pares de variáveis dentre drogadição, alcoolismo e violências contra mulheres e crianças.

    Observa-se uma associação relativamente forte entre as violências e entre alcoolismo e drogadição, moderada entre alcoolismo e drogadição e entre alcoolismo e as violências, e associação mais fraca entre violência contra crianças e drogadição.
    """
    )
    return


@app.cell(hide_code=True)
def _(cramers_v, df_long_periods, np, pl, px):
    def cramer_plot():
        columns = [
            "drug_addiction",
            "alcoholism",
            "violence_women",
            "violence_children",
        ]

        df_subset = (
            df_long_periods.select(["id_family_datalake"] + columns)
            # Mantém apenas uma linha por família
            .unique()
            # Talvez -> Sim
            .with_columns(
                [
                    pl.col(col).replace({"Talvez": "Sim"}).alias(col)
                    for col in columns
                ]
            )
        )

        # Cria matriz de associação, inicialmente zerada
        n_vars = len(columns)
        association_matrix = np.zeros((n_vars, n_vars))

        # Calcula a associação de cada par usando V de Cramér
        for i in range(n_vars):
            for j in range(n_vars):
                if i == j:
                    association_matrix[i, j] = 1.0
                else:
                    association_matrix[i, j] = cramers_v(
                        df=df_subset,
                        col1=columns[i],
                        col2=columns[j],
                        verbose=False,
                    )

        # Cria o heatmap
        fig = px.imshow(
            association_matrix,
            x=columns,
            y=columns,
            # color_continuous_scale="Purples",
            color_continuous_scale="OrRd",
            title="Heatmap de associação (Cramér's V)",
            labels=dict(color="Cramér's V"),
        )

        dims = 500
        fig.update_layout(
            width=dims,
            height=dims,
            xaxis_title="Variáveis",
            yaxis_title="Variáveis",
        )
        return fig


    cramer_plot()
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    #### Renda

    Há três outliers de renda, prováveis erros de digitação. Todos os outros registros informam rendas de menos de dez mil reais, portanto usamos esse limite (10k) aqui para visualização em histograma.
    """
    )
    return


@app.cell(hide_code=True)
def _(pl, px):
    def plot_answer_histogram(
        df,
        question_name,
        bin_count=10,
        title=None,
        upper_limit=100_000,
        show_values=True,
        cumulative=False,
        subtitle=None,
        # group_by=None,
        color_palette=None,
        y_max=None,
        percentage=False,
    ):
        """
        Create a histogram visualization of the 'answer' column data.

        Parameters:
        - df: Polars DataFrame containing the data
        - question_name: Optional filter to show only specific question
        - title: Optional custom title for the plot
        - color_palette: Optional color palette for the bars
        - percentage: If True, show percentages instead of counts

        Returns:
        - Plotly figure object
        """

        conditions = (pl.col("question") == question_name) & (
            pl.col("answer") != "NA"
        )

        select_cols = [pl.col("answer").cast(pl.Float32)]

        # Filtra por question
        df_filtered = df.filter(conditions)

        df_filtered = df_filtered.select(select_cols)

        # Aplica o limite superior (upper_limit) para eliminar outliers, caso informado
        if upper_limit:
            df_filtered = df_filtered.filter(pl.col("answer") <= upper_limit)

        # Gera os dados de histograma usando .hist()
        hist_data = pl.Series(df_filtered).hist(bin_count=10)
        # [TODO] Fazer uma função de bin própria que receba o módulo que cada bin deve ter (ex.: cada bin é R$500 ou R$1000 a mais)
        # hist_data = pl.Series(df_filtered).hist()

        # Calcula percentuais
        total_count = hist_data.select("count").sum().item()

        hist_data = hist_data.with_columns(
            (pl.col("count") / total_count * 100).alias("percentage")
        )

        hist_data = hist_data.with_columns(
            cum_count=pl.col("count").cum_sum(),
            cum_pct=pl.col("percentage").cum_sum(),
            # cum_pct=pl.col("percentage").sort("breakpoint").cum_sum(),
        )

        labels_dict = {
            "breakpoint": "Limite superior",
            "percentage": "Percentual de famílias",
            "count": "Núm de famílias",
            "cum_count": "Núm de famílias cumulativo",
            "cum_pct": "% de famílias cumulativo",
        }

        # Configura colunas
        if percentage:
            if cumulative:
                y_column = "cum_pct"
            else:
                y_column = "percentage"
            # y_label = "Percentual de famílias"
            # y_title = "Percentual de famílias no bin"
        else:
            if cumulative:
                y_column = "cum_count"
            else:
                y_column = "count"
            # y_label = "Núm de famílias"
            # y_title = "Número de famílias no bin"

        y_label = labels_dict.get(y_column)
        y_title = y_label + " no bin"
        # print(hist_data.head())

        # Gera a imagem do histograma

        if cumulative:
            title += " (Cumulative)"

        fig = px.bar(
            hist_data,
            # x="category",
            x="breakpoint",
            y=y_column,
            title=title
            or f"Distribution of Answers{' for ' + question_name if question_name else ''}",
            subtitle=subtitle or "",
            color_discrete_sequence=color_palette or px.colors.qualitative.Set3,
            labels=labels_dict,
            hover_data=[
                "count",
                "percentage",
                "cum_count",
                "cum_pct",
            ],
        )

        x_kwargs = dict(
            tickformat=",.2f",
            tickprefix="R$ ",
            tickangle=-45,
            title="Limit superior do bin",
            range=[0, upper_limit],
        )

        y_kwargs = dict(
            title=y_title,
            range=[0, y_max] if y_max else None,
        )

        # Update layout
        fig.update_layout(
            xaxis=x_kwargs,
            yaxis=y_kwargs,
        )

        if show_values:
            if percentage:
                fig.update_traces(texttemplate="%{y:.1f}%", textposition="outside")
            else:
                fig.update_traces(texttemplate="%{y}", textposition="outside")

        # fig.show()

        return fig
    return (plot_answer_histogram,)


@app.cell(hide_code=True)
def _(df_plot_variables, pl, plot_answer_histogram, px):
    _group_by_vals = ["Sim", "Não"]
    title_dict = {
        "alcoholism": "Alcoolismo",
        "drug_addiction": "Drogadição",
        "violence_women": "Violência contra mulheres",
        "violence_children": "Violência contra crianças",
    }

    _palette_dict = {
        "Não": [px.colors.qualitative.Pastel[4]],
        "Sim": [px.colors.qualitative.Pastel[2]],
    }


    def plot_histograms_income(col, percentage=True, cum=False):
        figs_list = []
        # for cum in [False, True]:
        for val in _group_by_vals:
            df_plot = df_plot_variables.filter(pl.col(col) == val)

            fig = plot_answer_histogram(
                df_plot,
                question_name="Income",
                title="Distribuição de renda (Income)",
                subtitle=f"{title_dict.get(col)}: {val}",
                color_palette=_palette_dict.get(val, px.colors.qualitative.Pastel),
                bin_count=10,
                y_max=120 if percentage else None,
                upper_limit=10_000,
                percentage=percentage,
                cumulative=cum,
            )
            figs_list.append(fig)

        return figs_list
    return


@app.cell
def _(mo):
    bin_size_slider = mo.ui.slider(
        start=200, stop=1000, step=100, label="Tamanho de cada bin (R$)", value=500
    )
    max_y_slider = mo.ui.slider(
        start=50, stop=1000, step=50, label="Valor máximo do eixo Y", value=450
    )
    max_x_slider = mo.ui.slider(
        start=1_000,
        stop=10_000,
        step=1_000,
        label="Valor máximo do eixo X",
        value=10_000,
    )
    income_group_by_cols = mo.ui.dropdown(
        options={
            "Drogadição": "drug_addiction",
            "Violência contra mulheres": "violence_women",
            "Violência contra crianças": "violence_children",
            "Violência contra crianças e mulheres": "violences",
            "Alcoolismo": "alcoholism",
            "Drogadição + Alcoolismo": "drugs",
        },
        label="Escolha a variável de agrupamento: ",
        # value="Drogadição",
        allow_select_none=True,
    )

    cb_first_time = mo.ui.checkbox(label="Tempo inicial", value=True)
    cb_last_time = mo.ui.checkbox(label="Tempo final", value=True)

    # income / per capita
    income_col_to_use = mo.ui.dropdown(
        label="Escolha a variável principal:",
        options={"Renda": "Income", "Renda per Capita": "IncomePerCapita"},
        value="Renda per Capita",
    )
    # [NICE TO HAVE] percentage e cumulative
    return (
        bin_size_slider,
        cb_first_time,
        cb_last_time,
        income_col_to_use,
        income_group_by_cols,
        max_x_slider,
        max_y_slider,
    )


@app.cell
def _():
    import matplotlib.colors as mcolors


    def lighten_color(hex_color, amount=0.5):
        # amount: 0 = original, 1 = white
        c = mcolors.to_rgb(hex_color)
        return mcolors.to_hex([1 - (1 - x) * (1 - amount) for x in c])
    return (lighten_color,)


@app.cell
def _(
    GT,
    bin_size_slider,
    cb_first_time,
    cb_last_time,
    df_long,
    df_plot_variables,
    go,
    income_col_to_use,
    income_group_by_cols,
    lighten_color,
    max_x_slider,
    max_y_slider,
    md,
    pl,
):
    def get_income_plot():
        _col_to_use = income_col_to_use.value
        # _col_to_use = "IncomePerCapita"

        _palette = dict(
            NA="#7f7f7f",  # (gray)
            Não="#1f77b4",  # (blue)
            Sim="#ff7f0e",  # (orange)
            # green="#2ca02c",  # (green)
            # red="#d62728",  # (red)
            # purple="#9467bd",  # (purple)
            # brown="#8c564b",  # (brown)
            # pink="#e377c2",  # (pink)
            # olive="#bcbd22",  # (olive)
            # cyan="#17becf",  # (cyan)
        )

        # ['', '/', '\\', 'x', '-', '|', '+', '.']
        _palette_pattern = dict(
            Não="\\",
            NA="+",
            Sim="x",
        )
        _color_first = "purple"
        _color_last = "green"

        _shape_first = "-"
        _shape_last = "x"

        # [TODO] FILTER CONDITIONS
        # [TODO] PERCENTAGE AND CUMULATIVE

        _subtitle_l = []
        _fig_info = {
            "Income": dict(title="Renda", max_y=400),
            "IncomePerCapita": dict(title="Renda per Capita", max_y=400),
        }

        # [TODO] add essa coluna no processamento da base?
        if _col_to_use == "IncomePerCapita":
            _data = (
                # 583 famílias tem dados de HowManyPHHH, e 577 tem Income marcado no seu primeiro tempo. Pegamos a intersecção entre esses grupos (537 famílias)
                df_long.filter(  # [NOTE] Aqui pegamos do df_long porque a pergunta só foi feita no tempo 0
                    (pl.col("question") == "HowManyPHHH")
                    & (pl.col("answer") != "NA")
                )
                .select(
                    "id_family_datalake",
                    pl.col("answer").cast(pl.Int32).alias("num_family_members"),
                )
                .join(
                    df_plot_variables.filter(
                        # [NOTE] Pegamos as respostas válidas de renda no (do primeiro ou último tempo) da família (não necessariamente o tempo 0), apesar de que HowManyPHHH é só no tempo 0 (ex.: para time=="FIRST", usamos os dados de Income de 524 famílias no tempo 0, 13 no tempo 1)
                        (pl.col("question") == "Income")
                        & (pl.col("answer") != "NA")
                    ).select(
                        "id_family_datalake",
                        pl.col("answer").cast(pl.Float64).alias("Income"),
                        "time",
                        "drug_addiction",
                        "alcoholism",
                        "violence_women",
                        "violence_children",
                    ),
                    on="id_family_datalake",
                    how="inner",
                )
                .with_columns(
                    IncomePerCapita=pl.col("Income") / pl.col("num_family_members")
                )
            )
        elif _col_to_use == "Income":
            _data = df_plot_variables.filter(
                (pl.col("question") == "Income") & (pl.col("answer") != "NA")
            ).with_columns(pl.col("answer").cast(pl.Float64).alias("Income"))

        _data = _data.with_columns(
            violences=(pl.col("violence_women") == "Sim")
            & (pl.col("violence_children") == "Sim"),
            drugs=(pl.col("drug_addiction") == "Sim")
            & (pl.col("alcoholism") == "Sim"),
        ).with_columns(
            violences=pl.when(pl.col.violences == True)
            .then(pl.lit("Sim"))
            .when(pl.col.violences == False)
            .then(pl.lit("Não")),
            drugs=pl.when(pl.col.drugs == True)
            .then(pl.lit("Sim"))
            .when(pl.col.drugs == False)
            .then(pl.lit("Não")),
        )

        _first_data = _data.filter(pl.col.time == "FIRST")
        _last_data = _data.filter(pl.col.time == "LAST")

        _fig = go.Figure()

        _group_by_col = income_group_by_cols.value

        if cb_first_time.value:
            if _group_by_col:
                for group_name, group_df in _first_data.group_by(_group_by_col):
                    _fig.add_trace(
                        go.Histogram(
                            name=f"Tempo <b>inicial</b>, <i>{income_group_by_cols.selected_key}</i>: <b>{group_name[0]}</b>",
                            marker=dict(
                                pattern=dict(
                                    # shape=_palette_pattern.get(
                                    #     group_name[0]
                                    # ),
                                    shape=_shape_first,
                                    fillmode="overlay",  # "overlay" or "replace"
                                    fgcolor=lighten_color(
                                        _palette.get(group_name[0]), 0.1
                                    ),  # Foreground color of pattern
                                    bgcolor=lighten_color(
                                        _palette.get(group_name[0])
                                    ),  # Background color of pattern
                                    size=15,  # Size of pattern elements
                                    solidity=0.1,  # Opacity of pattern
                                    fgopacity=1,
                                ),
                                line=dict(
                                    color=_palette.get(
                                        group_name[0]
                                    ),  # Contour color
                                    width=1,  # Contour width in pixels
                                ),
                            ),
                            x=pl.Series(group_df[_col_to_use]).to_numpy(),
                            xbins=dict(
                                start=0,
                                end=10_000,
                                size=bin_size_slider.value
                                if bin_size_slider.value
                                else 500,
                            ),
                            hovertemplate="Bin: R$ %{x}<br>Count: %{y}<extra></extra>",
                        ),
                    )
            else:
                _fig.add_trace(
                    go.Histogram(
                        name="Tempo <b>inicial</b>",
                        marker=dict(
                            pattern=dict(
                                # shape="-",
                                shape=_shape_first,
                                fgcolor=lighten_color(_color_first, 0.1),
                                bgcolor=lighten_color(_color_first),
                                size=15,  # Size of pattern elements
                                solidity=0.1,  # Opacity of pattern
                                fgopacity=1,
                            ),
                            line=dict(
                                color=_color_first,  # Contour color
                                width=1,  # Contour width in pixels
                            ),
                        ),
                        x=pl.Series(_first_data[_col_to_use]).to_numpy(),
                        xbins=dict(
                            start=0,
                            end=10_000,
                            size=bin_size_slider.value
                            if bin_size_slider.value
                            else 500,
                        ),
                        hovertemplate="Bin: R$ %{x}<br>Count: %{y}<extra></extra>",
                    ),
                )
            _subtitle_l.append("<b>Tempo inicial</b>")

        if cb_last_time.value:
            if _group_by_col:
                for group_name, group_df in _last_data.group_by(_group_by_col):
                    _fig.add_trace(
                        go.Histogram(
                            name=f"Tempo <b>final</b>, <i>{income_group_by_cols.selected_key}</i>: <b>{group_name[0]}</b>",
                            marker=dict(
                                pattern=dict(
                                    shape=_shape_last,
                                    # shape=_palette_pattern.get(
                                    #     group_name[0]
                                    # ),
                                    fillmode="overlay",  # "overlay" or "replace"
                                    fgcolor=lighten_color(
                                        _palette.get(group_name[0]), 0.1
                                    ),  # Foreground color of pattern
                                    bgcolor=lighten_color(
                                        _palette.get(group_name[0])
                                    ),  # Background color of pattern
                                    size=10,  # Size of pattern elements
                                    solidity=0.1,  # Opacity of pattern
                                    fgopacity=0.5,
                                ),
                                line=dict(
                                    color=_palette.get(
                                        group_name[0]
                                    ),  # Contour color
                                    width=1,  # Contour width in pixels
                                ),
                            ),
                            x=pl.Series(group_df[_col_to_use]).to_numpy(),
                            xbins=dict(
                                start=0,
                                end=10_000,
                                size=bin_size_slider.value
                                if bin_size_slider.value
                                else 500,
                            ),
                            hovertemplate="Bin: R$ %{x}<br>Count: %{y}<extra></extra>",
                        ),
                    )
            else:
                _fig.add_trace(
                    go.Histogram(
                        name="Tempo <b>final</b>",
                        marker=dict(
                            pattern=dict(
                                # shape="-",
                                shape=_shape_last,
                                fgcolor=lighten_color(_color_last, 0.1),
                                bgcolor=lighten_color(_color_last),
                                size=15,  # Size of pattern elements
                                solidity=0.1,  # Opacity of pattern
                                fgopacity=1,
                            ),
                            line=dict(
                                color=_color_last,  # Contour color
                                width=1,  # Contour width in pixels
                            ),
                        ),
                        x=pl.Series(_last_data[_col_to_use]).to_numpy(),
                        xbins=dict(
                            start=0,
                            end=10_000,
                            size=bin_size_slider.value
                            if bin_size_slider.value
                            else 500,
                        ),
                        hovertemplate="Bin: R$ %{x}<br>Count: %{y}<extra></extra>",
                    ),
                )
            _subtitle_l.append("<b>Tempo final</b>")

        _subtitle = (" e ").join(_subtitle_l)
        if _group_by_col:
            _subtitle += (
                f" agrupado por <b>{income_group_by_cols.selected_key}</b>"
            )

        _fig.update_layout(
            title={
                "text": f"{_fig_info.get(_col_to_use).get('title', '')}<br><sup>{_subtitle}</sup>",
                "x": 0.5,  # Center the title
            },
            xaxis_title=_fig_info.get(_col_to_use).get("title", "")
            + f" (bins de R${bin_size_slider.value})",
            yaxis_title="Número de respostas no bin",
            yaxis=dict(
                range=[0, max_y_slider.value if max_y_slider.value else 300]
            ),
            xaxis=dict(
                range=[0, max_x_slider.value if max_x_slider.value else 300]
            ),
            bargap=0.1,
        )
        _fig.update_layout(
            legend=dict(
                orientation="v",
                y=0.7,  # Slightly above the plot area
                x=0.8,  # Centered horizontally
                xanchor="center",
                yanchor="bottom",
            )
        )
        _fig.update_layout(width=1000)  # Set width in pixels

        # ----

        # return _fig, None
        if not _group_by_col:
            return _fig, None

        _cols_gt = _col_to_use
        # if _col_to_use = "

        _df_gt_first = pl.concat(
            [
                (
                    _first_data.filter(True & (pl.col(_group_by_col) == "Sim"))
                    .select(_cols_gt)
                    .describe()
                    .with_columns(answer=pl.lit("Sim"))
                ),
                (
                    _first_data.filter(True & (pl.col(_group_by_col) == "Não"))
                    .select(_cols_gt)
                    .describe()
                    .with_columns(answer=pl.lit("Não"))
                ),
            ]
        )

        _df_gt_last = pl.concat(
            [
                (
                    _last_data.filter(True & (pl.col(_group_by_col) == "Sim"))
                    .select(_cols_gt)
                    .describe()
                    .with_columns(answer=pl.lit("Sim"))
                ),
                (
                    _last_data.filter(True & (pl.col(_group_by_col) == "Não"))
                    .select(_cols_gt)
                    .describe()
                    .with_columns(answer=pl.lit("Não"))
                ),
            ]
        )

        _gt = [(
            GT(
                _df.filter(pl.col.statistic != "null_count")
            )
            .fmt_number(
                columns=[_col_to_use],
                # force_sign=True,
                decimals=2,
                # scale_by=100,
                pattern="R$ {x}",
                rows=list(range(1, 8)) + list(range(9, 16)),
            )
            # .fmt_number(
            #     columns=["num_family_members"],
            #     # force_sign=True,
            #     decimals=2,
            #     # scale_by=100,
            #     pattern="{x}",
            #     rows=list(range(1, 8)) + list(range(9, 16)),
            # )
            .fmt_number(
                columns=[_cols_gt],
                decimals=0,
                pattern="{x}",
                rows=[0, 8],
            )
            .tab_header(
                title=_title,
                subtitle=md(
                    f"Descrição estatística <b>{income_group_by_cols.selected_key}</b>"
                ),
            )
            .cols_label(
                **{_cols_gt:income_col_to_use.selected_key}
                # num_family_members=md("Núm. de membros da família"),
            )
            .tab_stub(rowname_col="statistic", groupname_col="answer")
        ) for (_df, _title) in zip([_df_gt_first, _df_gt_last], ["Tempo inicial", "Tempo final"])]

        return _fig, _gt
    return (get_income_plot,)


@app.cell
def _(
    bin_size_slider,
    cb_first_time,
    cb_last_time,
    get_income_plot,
    income_col_to_use,
    income_group_by_cols,
    max_x_slider,
    max_y_slider,
    mo,
):
    _fig, _gt = get_income_plot()

    mo.vstack(
        [
            mo.hstack(
                [
                    bin_size_slider,
                    max_y_slider,
                    max_x_slider,
                ],
                justify="start",
            ),
            mo.hstack(
                [
                    cb_first_time,
                    cb_last_time,
                ],
                justify="start",
            ),
            mo.hstack(
                [
                    income_col_to_use,
                    income_group_by_cols,
                ],
                justify="start",
            ),
            _fig,
            mo.accordion({"Descrição estatística do agrupamento": mo.hstack(_gt, justify="space-around")}) if _gt else "",
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""#### Tipo de trabalho (IncomeWorkS3)""")
    return


@app.cell
def _(mo):
    pct_5 = mo.ui.switch(value=True, label=f"Percentual")
    mo.hstack([pct_5], justify="start")
    return (pct_5,)


@app.cell
def _(df_plot_variables, mo, pct_5, pl, plot_variables, px):
    _columns = [
        "drug_addiction",
        "alcoholism",
        "violence_women",
        "violence_children",
    ]

    _figs = []
    for _col in _columns:
        _group_by_vals = [
            val
            for val in pl.Series(df_plot_variables.select(_col).unique()).to_list()
            if val != "NA"
        ]

        for _val in _group_by_vals:
            _df_plot = df_plot_variables.filter(pl.col(_col) == _val)

            _fig = plot_variables(
                _df_plot,
                ["IncomeWorkS3"],
                subtitle=f"{_col}: {_val}",
                seggregate_favela=False,
                orientation="v",
                # orientation="h",
                max_y=100 if pct_5.value else 150,
                percentage=pct_5.value,
                palette=px.colors.qualitative.Pastel,
            )
            _figs.append(_fig)

    mo.hstack(_figs)
    return


@app.cell
def _(df_plot_variables, mo, pct_5, pl, plot_variables, px):
    _figs = []

    # for val in group_by_vals:
    _df_plot = df_plot_variables.filter(
        (pl.col("drug_addiction") == "Sim") & (pl.col("alcoholism") == "Sim")
    )

    _df_plot_baseline = df_plot_variables.filter(
        (pl.col("alcoholism") == "Não") & (pl.col("drug_addiction") == "Não")
    )

    for _df, _subtitle in [
        (_df_plot, "SIM para Drogadição e Alcoolismo"),
        (_df_plot_baseline, "NÃO para Drogadição e Alcoolismo"),
    ]:
        _fig = plot_variables(
            _df,
            ["IncomeWorkS3"],
            subtitle=_subtitle,
            seggregate_favela=False,
            orientation="v",
            max_y=100 if pct_5.value else 150,
            palette=px.colors.qualitative.Pastel,
            percentage=pct_5.value,
        )
        _figs.append(_fig)

    mo.hstack(_figs)
    return


@app.cell
def _(df_plot_variables, mo, pct_5, pl, plot_variables, px):
    _figs = []

    # for val in group_by_vals:
    _df_plot = df_plot_variables.filter(
        (pl.col("violence_women") == "Sim")
        & (pl.col("violence_children") == "Sim")
    )

    _df_plot_baseline = df_plot_variables.filter(
        (pl.col("violence_women") == "Não")
        & (pl.col("violence_children") == "Não")
    )

    for _df, _subtitle in [
        (_df_plot, "SIM para Violências (mulher e criança)"),
        (_df_plot_baseline, "NÃO para Violências (mulher e criança)"),
    ]:
        _fig = plot_variables(
            _df,
            ["IncomeWorkS3"],
            subtitle=_subtitle,
            seggregate_favela=False,
            orientation="v",
            max_y=100 if pct_5.value else 150,
            palette=px.colors.qualitative.Pastel,
            percentage=pct_5.value,
        )
        _figs.append(_fig)

    mo.hstack(_figs)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Análise 6 - Diferença entre grupo raciais e de gênero

    A diferenças entre grupos (race e gender) diminuiu ao longo dos tempos?
    """
    )
    return


@app.cell
def _(
    df_long_first,
    df_long_last,
    dimension_6,
    get_answer_maps_per_question,
    get_answer_orders_per_question,
    name_dict,
    pl,
    px,
    question_6,
):
    # [TODO] cores de acordo com a resposta, dependendo da _question_name
    def get_parallel_cats():
        _dimension = dimension_6.value
        _question_name = question_6.value

        answer_map = get_answer_maps_per_question(_question_name)[0]
        answer_order = get_answer_orders_per_question(_question_name)[0]
        colors_dic = {
            6: ["#6E899C", "#D7263D", "#EB7E69", "#F7F7A1", "#72B7B2", "#3366CC"],
            5: ["#6E899C", "#EB7E69", "#F7F7A1", "#72B7B2", "#3366CC"],
            4: ["#6E899C", "#F7F7A1", "#72B7B2", "#3366CC"],
        }

        _igf_color_dict = dict(
            value=answer_order,
            color=colors_dic.get(len(answer_order)),
        )

        _df_colors = pl.DataFrame(_igf_color_dict)

        _df_plot = (
            df_long_first.filter(pl.col("question") == _question_name)
            .with_columns(first_answer="answer")
            .select("id_family_datalake", "question", "first_answer", _dimension)
            .join(
                df_long_last.with_columns(last_answer="answer"),
                on=["id_family_datalake", "question"],
                how="left",
            )
            # .with_columns(last_answer="answer")
            .select(
                "id_family_datalake",
                "question",
                pl.col("first_answer").replace(answer_map).alias("first_answer"),
                pl.col("last_answer").replace(answer_map).alias("last_answer"),
                _dimension,
            )
        ).join(_df_colors, left_on="last_answer", right_on="value")

        if _dimension == "race":
            _df_plot = _df_plot.with_columns(
                race=pl.when(
                    (pl.col("race") == "Parda")
                    | (pl.col("race") == "Preta")
                    | (pl.col("race") == "Indígena")
                )
                .then(pl.lit("Negros e indígenas"))
                .otherwise(pl.lit("Brancos e amarelos"))
            )
        if _dimension == "gender":
            _df_plot = _df_plot.filter(
                pl.col.gender.is_in([
                    "Mulher trans", # 2 pessoa
                    "Homem trans", # 1 pessoa
                    "Não binário",# 1 pessoa
                ]).not_()
            )

        _fig = px.parallel_categories(
            _df_plot.select(
                "first_answer",
                "last_answer",
                "color",
                pl.col(_dimension).alias(name_dict.get(_dimension)),
            ).sort(name_dict.get(_dimension), "first_answer", "last_answer"),
            dimensions=[name_dict.get(_dimension), "first_answer", "last_answer"],
            title=f"<b>{name_dict.get(_question_name)}</b>",
            subtitle=f"[{_question_name}]",
            color="color",
            labels={
                "first_answer": "Primeira resposta da família",
                "last_answer": "Última resposta da família",
            },
        )

        return _fig
    return (get_parallel_cats,)


@app.cell(hide_code=True)
def _(mo, name_dict):
    _questions = [
        "FoodManytimes",
        "SchoolLiteracy",
        "IncomeWorkS3",
    ]
    _options = {v: k for k, v in name_dict.items() if k in _questions}

    _dimensions = {v: k for k, v in name_dict.items() if k in ["gender", "race"]}

    question_6 = mo.ui.dropdown(
        options=_options,
        value=list(_options.keys())[0],
        label="Escolha uma pergunta:",
    )
    dimension_6 = mo.ui.dropdown(
        options=_dimensions,
        value=list(_dimensions.keys())[0],
        label="e uma dimensão de análise:",
    )

    mo.hstack([question_6, dimension_6], justify="start")
    return dimension_6, question_6


@app.cell
def _(get_parallel_cats):
    get_parallel_cats()
    return


@app.cell
def _(enrich_first_and_last_time, get_vars_IGF, mo, pl):
    # Leitura do df original em CSV
    df_long_path = str(mo.notebook_location() / "public" / "df_long.csv")
    # Passa o dataframe para o formato long (uma row por resposta, ao invés de uma row por família)
    df_long = pl.read_csv(df_long_path)

    # Enriquece o df_long com as datas da primeira e da última coleta de cada família
    df_long_periods = enrich_first_and_last_time(df_long)

    # Filtra só as respostas do tempo inicial e tempo final de cada família
    df_long_first = df_long_periods.filter(
        (pl.col("time") == (pl.col("time_first")))
    )

    df_long_last = df_long_periods.filter(
        (pl.col("time") == (pl.col("time_last")))
    )

    df_plot_variables = pl.concat(
        [
            df_long_first.with_columns(time=pl.lit("FIRST")),
            df_long_last.with_columns(time=pl.lit("LAST")),
        ]
    )

    questions = get_vars_IGF()

    wip_multiple_assertions = [
        "Garbage",  # OK
        "HousingProblems",  # OK
        "CommFacilities",  # OK
        "Internet",  # versão contagem OK, versão binária teria que re-normalizar para família (ao invés de multi assercao). Talvez gerar uma nova coluna?
        "BathroomQualit",  # [TODO] Tem que fundir as colunas de T0 que estão separadas
        "HealthGenKidsNames",  # versão contagem OK, NAs OK, número de doenças tem que normalizar para família
        "HealthGenNames",  # versão contagem OK, NAs OK, número de doenças tem que normalizar para família
        "Documents",  #
        #
        # São de outra pergunta
        # "IncomeDesc",
        # "JobSatisfaction"
    ]

    wip_other = ["Income"]

    petals_cats = [
        "CategoriaIGF",
        "CategoriaIncome",
        "CategoriaCitizenship",
        "CategoriaCulture",
        "CategoriaFirstInfancy",
        "CategoriaHealth",
        "CategoriaHousing",
        "CategoriaSchooling",
        "CategoriaWomanAutonomy",
        "CategoriaEnvironment",
    ]

    petals_avg = [
        "AverageIGF",
        "AverageIncome",
        "AverageCitizenship",
        "AverageCulture",
        "AverageFirstInfancy",
        "AverageHealth",
        "AverageHousing",
        "AverageSchooling",
        "AverageWomanAutonomy",
        "AverageEnvironment",
    ]

    questions_except_wip = list(
        set(questions) - set(wip_multiple_assertions) - set(wip_other)
    )
    return (
        df_long,
        df_long_first,
        df_long_last,
        df_long_periods,
        df_plot_variables,
        petals_cats,
        questions,
        wip_multiple_assertions,
        wip_other,
    )


@app.cell
def _():
    name_dict = {
        "race": "Raça",
        "gender": "Gênero",
        "Access": "Existe algum integrante da família que não tem acesso a atividades de lazer, recreação e convívio social?",
        "BankAccount": "A família tem conta em banco?",
        "Bathroom": "Sobre o banheiro da sua casa...",
        "BathroomQualit": "Sobre o banheiro da sua casa, o que possui?",
        "CEP": "O endereço da sua casa tem CEP?",
        "CommFacilities": "Sobre a favela onde mora, assinale os itens que você possui acesso",
        "CulturalEvent": "Com qual frequência vai a eventos culturais?",
        "Documents": "Quais documentos vocês têm?",
        "Eletricity": "Quanto à energia elétrica...",
        "Floor": "O chão ou piso da sua casa é, na maioria dos cômodos, feito de qual material?",
        "FoodManytimes": "Quantas vezes a família faz refeições ao dia?",
        "Garbage": "O que é feito com o lixo da casa?   ",
        "HealthGenKidsNames": "Quantos adultos maiores de 18 anos da família apresentam as seguintes condições?  ",
        "HealthGenNames": "Quantas crianças e adolescentes de 0 a 17 anos da família apresentaram as seguintes condições?",
        "HousingProblems": "Quais são as principais dificuldades e ou risco que sua família enfrenta morando nessa casa?",
        "IncomeWorkS3": "Qual é a situação de trabalho principal?",
        "Internet": "Para que você usa a internet? ",
        "Roof": "De qual material é feita a cobertura da sua casa?",
        "SchoolCurrent": "Está estudando?",
        "SchoolLast": "Qual a escolaridade?  ",
        "SchoolLiteracy": "Quão bem sabe ler e escrever?",
        "SchoolMathLit": "Quão bem sabe fazer contas matemáticas?",
        "Sewer": "Sobre o esgoto da sua casa...",
        "Walls": "As paredes da sua casa são, na maioria dos cômodos, feitas de qual material?",
        "Water": "Sobre o fornecimento de água...",
        "WaterFrequency": "Com qual frequência você tem água na sua casa?",
        "CategoriaIGF": "Média das dimensões do IGF",
        "CategoriaIncome": "Geração de renda",
        "CategoriaCitizenship": "Cidadania",
        "CategoriaCulture": "Cultura",
        "CategoriaFirstInfancy": "Primeira infância",
        "CategoriaHealth": "Saúde",
        "CategoriaHousing": "Moradia e urbanismo",
        "CategoriaSchooling": "Educação",
        "CategoriaWomanAutonomy": "Autonomia das mulheres",
        "CategoriaEnvironment": "Meio ambiente",
    }
    return (name_dict,)


@app.cell
def _():
    ASSERTION_MAP = {
        "Access": {
            "map": {
                "Existe algum membro que não tem acesso": [
                    "Sim, existe algum membro que não tem acesso"
                ],
                "Todos os membros tem acesso": [
                    "Não, todos os membros tem acesso",
                ],
            },
            "order": [
                "NA",
                "Existe algum membro que não tem acesso",
                "Todos os membros tem acesso",
            ],
        },
        "SchoolCurrent": {
            "map": {
                "Não": [
                    "Nao",
                    "Não sabe",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Sim",
            ],
        },
        "FoodManytimes": {
            "map": {
                "Mais que 3 vezes ao dia": [
                    "mais que 3 vezes ao dia",
                ],
                "1 ou 2 vezes ao dia": [
                    "1 vez ao dia",
                    "2 vezes ao dia",
                ],
            },
            "order": [
                "NA",
                "1 ou 2 vezes ao dia",
                "3 vezes ao dia",
                "Mais que 3 vezes ao dia",
            ],
        },
        "Eletricity": {
            "map": {
                "Não": [
                    "Nao",
                    "Não sabe",
                    "Outro",
                ],
                "Possui sem padrão próprio": [
                    "Possui sem padrao proprio",
                ],
                "Possui com padrão próprio": [
                    "Possui com padrao proprio",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Possui sem padrão próprio",
                "Possui com padrão próprio",
            ],
        },
        "Floor": {
            "map": {
                "Cerâmica, lajota, pedra, material sustentável e/ou madeira trabalhada": [
                    "Ceramica Lajota ou Pedra",
                    "Madeira Trabalhada",
                    "Material sustentável",
                ],
                "Terra batida": [
                    "Terra Batida",
                ],
                "Madeira aproveitada": [
                    "Madeira Aproveitada",
                ],
                "Cimento e/ou contrapiso": [
                    "Cimento / Contrapiso",
                    "Outro Material",
                ],
            },
            "order": [
                "NA",
                "Terra batida",
                "Madeira aproveitada",
                "Cimento e/ou contrapiso",
                "Cerâmica, lajota, pedra, material sustentável e/ou madeira trabalhada",
            ],
        },
        "CEP": {
            "map": {
                "Tenho CEP": [
                    "Sim, tenho CEP (Código de Endereçamento Postal)",
                ],
                "Não tenho CEP": [
                    "Não, não tenho CEP (Código de Endereçamento Postal)",
                ],
            },
            "order": [
                "NA",
                "Não tenho CEP",
                "Tenho CEP",
            ],
        },
        "StayFavela": {
            "order": [
                "NA",
                "Não",
                "Não sei",
                "Sim",
            ]
        },
        "DreamsKids": {
            "map": {
                "Não": [
                    "(espontâneo) Não sei",
                    "Nao Sei",
                    "Nao",
                ],
                "Sim": [
                    "Sim, o que?",
                    "Sim o que?",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Sim",
            ],
        },
        "FamilyRelations": {
            "order": [
                "NA",
                "Conflituosas, com violência",
                "Eventualmente há violência",
                "Conflituosas, sem violência",
                "Sem conflitos relevantes",
                "Harmônicas",
            ],
        },
        "EventsFrequency": {
            "order": [
                "NA",
                "Nunca",
                "Às vezes",
                "Com frequência",
                "Sempre",
            ]
        },
        "Satisfaction": {
            "map": {},
            "order": [
                "NA",
                "Muito insatisfeito",
                "Insatisfeito",
                "Mais ou menos",
                "Satisfeito",
                "Muito satisfeito",
            ],
        },
        "Water": {
            "map": {
                "Encanada de poço ou nascente": [
                    "Encanada de Poco / Nascente",
                    "Cisterna",
                ],
                "Busco com balde": [
                    "Busco com Balde",
                ],
                "Carro-pipa": [
                    "Carro Pipa",
                ],
                "Encanada fora da rede oficial": [
                    "Encanada Clandestina",
                ],
                "Encanada da rede pública": [
                    "Rede Publica",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Busco com balde",
                "Carro-pipa",
                "Encanada fora da rede oficial",
                "Encanada de poço ou nascente",
                "Encanada da rede pública",
            ],
        },
        "Walls": {
            "map": {
                "Outro": [
                    "Não sabe",
                ],
                "Madeira aproveitada": [
                    "Madeira Aproveitada",
                ],
                "Taipa ou alvenaria sem revestimento": [
                    "Alvenaria / Tijolo SemRevestimento",
                    "Taipa",
                    "Taipa/Alvenaria e Tijolo Sem Revestimento",
                ],
                "Materiais adequados": [
                    "Paineis estruturados",
                    "Alvenaria / Tijolo Com Revestimento",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Lona",
                "Palha",
                "Madeira aproveitada",
                "Taipa ou alvenaria sem revestimento",
                "Materiais adequados",
            ],
        },
        "BankAccount": {
            "map": {
                "Sim": [
                    "Sim",
                    "Sim em meu nome",
                ],
                "Não": [
                    "Nao",
                    "Nao sei",
                    "Nao Sei",
                ],
            },
            "order": [
                "NA",
                "Não",
                "Sim mas não em meu nome",
                "Sim",
            ],
        },
        "CulturalEvent": {
            "map": {},
            "order": [
                "NA",
                "Nunca",
                "Às vezes",
                "Com frequência",
                "Sempre",
            ],
        },
        "SchoolLiteracy": {
            "map": {
                "Não sei ler": [
                    "Não sei informar",
                ],
            },
            "order": [
                "NA",
                "Não sei ler",
                "Não muito bem",
                "Bem",
                "Muito bem",
            ],
        },
        "IncomeWorkS3": {
            "map": {
                "Trabalho informal": [
                    "Trabalho Informal",
                ],
                "Autônomo": [
                    "Autonomo",
                ],
                "Não estou trabalhando": [
                    "Eu não estou trabalhando",
                    "Não sei",
                ],
                "CLT, servidor público, estágio ou jovem aprendiz": [
                    "CLT",
                    "Funcionário Público Concursado",
                    "Servidor público",
                    "Estágio/jovem aprendiz",
                ],
            },
            "order": [
                "NA",
                "Não estou trabalhando",
                "Trabalho informal",
                "Autônomo",
                "CLT, servidor público, estágio ou jovem aprendiz",
                "Aposentado",
            ],
        },
        "WhyNotWork": {
            "map": {
                "Estou aposentado": [
                    "Estou aposentado/a",
                ],
                "Estou cuidando de alguém da família ou não tenho com quem deixar meus filhos": [
                    "Está cuidando de alguém da família e por isso não consigue trabalhar",
                    "Não tenho com quem deixar meus filhos",
                ],
                "Estou com um problema de saúde": [
                    "Estou com um problema de saúde que me impossibilita/dificulta trabalhar"
                ],
                "Sou dona de casa": [
                    "Sou dona/o de casa",
                ],
                "Estou buscando trabalho mas não encontro": [
                    "Estou buscando emprego e trabalho ativamente mas não encontro"
                ],
                "Outro": [
                    "Não sei",
                    "Outro. O que?",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Estou em situação de drogadição",
                "Estou com um problema de saúde",
                "Estou cuidando de alguém da família ou não tenho com quem deixar meus filhos",
                "Sou dona de casa",
                "Estou buscando trabalho mas não encontro",
                "Estou estudando",
                "Estou aposentado",
            ],
        },
        "SchoolLast": {
            "map": {
                "Nunca estudei": [
                    "Não sei",
                ],
                "EJA": [
                    "Alfabetização para adultos",
                    "Alfabetização para adutlos",
                    "Educação de Jovens e Adultos",
                    "EJA",
                ],
                "Técnico profissionalizante": [
                    "Técnico/profissionalizante",
                    "Técnico / Profissionalizante",
                ],
            },
            "order": [
                "NA",
                "Nunca estudei",
                "Pré-escola",
                "Ensino fundamental",
                "EJA",
                "Ensino médio",
                "Técnico profissionalizante",
                "Superior",
            ],
        },
        "Sewer": {
            "map": {
                "Outro": [
                    "Não sabe",
                ],
                "Céu aberto": [
                    "Ceu Aberto",
                ],
                "Ligado à rede não oficial": [
                    "Ligado Rede Não Oficial",
                    "Ligado Rede Nao Oficial",
                ],
                "Ligado à rede oficial": [
                    "Ligado Rede Oficial",
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Céu aberto",
                "Ligado à rede não oficial",
                "Fossa",
                "Fossa séptica",
                "Ligado à rede oficial",
            ],
        },
        "SchoolMathLit": {
            "map": {
                "Não sei fazer contas matemáticas": [
                    "Não sei informar",
                ],
            },
            "order": [
                "NA",
                "Não sei fazer contas matemáticas",
                "Não muito bem",
                "Bem",
                "Muito bem",
            ],
        },
        "WaterFrequency": {
            "map": {
                "Não tenho": [
                    "Nao Tenho",
                ],
            },
            "order": [
                "NA",
                "Não tenho",
                "Menos de dois dias por semana",
                "Entre dois e quatro dias por semana",
                "Entre quatro e seis dias por semana",
                "Todos os dias da semana",
            ],
        },
        "Bathroom": {
            "map": {
                "Não tem": [
                    "Nao Tem",
                ],
                "Um banheiro compartilhado": [
                    "Um Banheiro Compartilhado",
                ],
                "Um banheiro exclusivo da família": [
                    "Um Banheiro Exclusivo",
                ],
                "Mais de um banheiro exclusivo da família": [
                    "Mais de um Banheiro Exclusivo"
                ],
            },
            "order": [
                "NA",
                "Outro",
                "Não tem",
                "Um banheiro compartilhado",
                "Um banheiro exclusivo da família",
                "Mais de um banheiro exclusivo da família",
            ],
        },
        "Roof": {
            "map": {
                "Materiais adequados": [
                    "Telhas de Fibrocimento Com Manta",
                    "Telha Metalica",
                    "Laje",
                    "Lajes com Impermeabilizacao",
                    "Painéis estruturados",
                    "Telha de Barro  / Ceramica",
                ],
                "Telhas sem manta": [
                    "Telhas de Fibrocimento Sem Manta",
                ],
                "Outro material": [
                    "Outro Material",
                    "Não sabe",
                ],
                "Madeira aproveitada": [
                    "Madeira Aproveitada",
                ],
            },
            "order": [
                "NA",
                "Outro material",
                "Lona",
                "Palha",
                "Madeira aproveitada",
                "Telhas sem manta",
                "Materiais adequados",
            ],
        },
        "Documents": [
            {
                "map": {
                    "Registro Nacional de Estrangeiro (RNE)": [
                        "Permisso de entrada, Autorização de residência, Protocolo de situação de Refugio, RNE, RME, Refugiado",
                        "RNE",
                    ],
                    "Registro indígena": [
                        "Registro Administrativo de Nascimento Indígena",
                        "Registro indígena",
                    ],
                    "RG": [
                        "Carteira de Identidade",
                        "RG",
                    ],
                    "Cartão SUS": ["Cartão SUS", "SUS"],
                    "Certidão de nascimento": ["Certidão de Nascimento"],
                    "Não tenho nenhum documento": [
                        "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                    ],
                },
                "filter_by_order": True,  # [TODO] Ainda não é usado
                "order": [
                    # "0", # 13 respostas (só tempo incial)
                    # "1", # 45 respostas (só tempo incial)
                    "Não tenho nenhum documento",
                    "CPF",
                    "Carteira de trabalho",
                    "Certidão de nascimento",
                    "Certidão de casamento",
                    "Cartão SUS",
                    "RG",
                    "Certificado de reservista",
                    "NIS/NIT",
                    "Título de eleitor",
                    "Registro indígena",
                    "Registro Nacional de Estrangeiro (RNE)",
                ],
            }
        ],
        "HealthGenKidsNames": {
            "map": {"Diarréia crônica": ["Diarréia Crônica"]},
            "order": [
                "NA",
                "Problemas de saúde bucal",
                "Doenças causadas por insetos",
                "Doenças respiratórias",
                "Diarréia crônica",
                "Desnutrição",
            ],
        },
        "HealthGenNames": {
            "map": {"Diarréia crônica": ["Diarréia Crônica"]},
            "order": [
                "NA",
                "Problemas de saúde bucal",
                "Doenças causadas por insetos",
                "Doenças respiratórias",
                "Diarréia crônica",
                "Desnutrição",
            ],
        },
        "Internet": [
            {
                "subtitle": "Todas as respostas",
                "map": {
                    "Não tenho acesso à internet": [
                        "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                    ]
                },
                "order": [
                    "Não tenho acesso à internet",
                    "Para acessar banco e serviços financeiros",
                    "Para acessar serviços públicos (poupa tempo, gov.br, INSS, etc...)",
                    "Para comprar coisas",
                    "Para estudar",
                    "Para falar com amigos e família",
                    "Para passar o tempo (redes sociais, jogos, ouvir música, ver filme...)",
                    "Para trabalhar",
                ],
            },
            # {
            #     "subtitle": "Binário: acesso à internet",  # [TODO] aqui precisaria re-obter uma linha por família, senão cada família vai ter várias asserções somando em "Tenho acesso à internet" por exemplo :ooo
            #     "map": {
            #         "Tenho acesso à internet": [
            #             "Para acessar banco e serviços financeiros",
            #             "Para acessar serviços públicos (poupa tempo, gov.br, INSS, etc...)",
            #             "Para comprar coisas",
            #             "Para estudar",
            #             "Para falar com amigos e família",
            #             "Para passar o tempo (redes sociais, jogos, ouvir música, ver filme...)",
            #             "Para trabalhar",
            #         ]
            #     },
            #     "order": ["Não tenho acesso à internet", "Tenho acesso à internet"],
            # },
        ],
        "CommFacilities": {
            "map": {
                "Iluminação pública": [
                    "Iluminacao Publica",
                    "Iluminação Pública",
                ],
                "Hospital público": [
                    "Hospital Publico",
                    "Hospital Público",
                ],
                "Creche ou escola pública": [
                    "Coleta de Lixo",
                    "Creche Publica",
                    "Creche Pública",
                    "Escola Publica",
                    "Escola Pública",
                ],
                "Opções de lazer": [
                    "Opcoes de Lazer",
                    "Opções de lazer",
                ],
                "Transporte Público": [
                    "Transporte Publico",
                    "Transporte Público",
                ],
                "Esgoto e água encanada": [
                    "Acesso à rede de esgoto",
                    "Agua Encanada",
                    "Esgoto",
                    "Água encanada",
                ],
                "Espaços comunitários": [
                    "Espaços para reuniões comunitárias",
                ],
                "Ruas e vielas": [
                    "Boas condições das ruas, vielas ou escadas que dão acesso à comunidade",
                    "Pavimentação das ruas e vielas da comunidade",
                ],
                "Posto de Saúde": [
                    "Posto de Saude",
                    "Posto de Saúde",
                ],
            },
            "order": [
                "Nenhuma opção",
                "Não sabe",
                "Iluminação pública",
                "Hospital público",
                "Creche ou escola pública",
                "Opções de lazer",
                "Transporte Público",
                "Esgoto e água encanada",
                "Espaços comunitários",
                "Ruas e vielas",
                "Posto de Saúde",
            ],
        },
        "BathroomQualit": {
            "map": {},
            "order": [
                "Box ou cortina que fecha o chuveiro",
                "Chuveiro com água quente",
                "Parede de azulejo",
                "Piso de azulejo",
                "Porta externa que fecha o banheiro",
                "Privada com tampa",
            ],
        },
        "HousingProblems": {
            "map": {
                "Infiltração, alagamento, inundação, umidade, chuva, goteiras e mofo": [
                    "Infiltração",
                    "Infiltração e humildade",
                    "Chuva Goteiras",
                    "Goteira",
                    "Umidade Mofo",
                    "Alagamento Inundacao",
                ],
                "Deslizamento, desmoronamento, solapamento ou casa caindo": [
                    "Deslizamento",
                    "Desmoronamento",
                    "Solapamento",
                    "Casa caindo",
                ],
                "Nenhum problema": [
                    "Minha casa não tem nenhum problema",
                    "Nenhum risco",
                    "Não",
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ],
                "Animais indesejados": [
                    "Escorpião, Embuá",
                    "Ratos Baratas Animais Indesejados",
                    "Gambá",
                    "Mosquito",
                ],
                # "Rachaduras e vazamentos": [
                # ],
                # "Saneamento básico": [
                # ],
                "Risco de incêndio": [
                    "Incendio",
                ],
                "Outros": [
                    "1",  # ???
                    "A escada de entrada.",
                    "Cozinha Com Lenha",
                    "Tiroteio",
                    "Espaço pequeno",
                    "Poste de iluminação pública com risco de cair",
                    "Espaço pequeno",
                    "Outro",
                    #
                    "Esgoto entupido",
                    "Falta de água constante",
                    "Saneamento básico",
                    #
                    "Banheiro com vazamento",
                    "Vazamentos hidráulicos",
                    "Rachadura",
                    "rachaduras",
                ],
            },
            "order": [
                "Outros",
                "Deslizamento, desmoronamento, solapamento ou casa caindo",
                "Infiltração, alagamento, inundação, umidade, chuva, goteiras e mofo",
                "Animais indesejados",
                "Risco de incêndio",
                "Cupim",
                "Nenhum problema",
            ],
        },
        "Garbage": {
            "map": {
                "Joga na rua, vala ou quintal": [
                    "Joga na rua / vala quintal",
                    "Joga na rua/vala/quintal",
                ],
                "Queimado ou enterrado": [
                    "Queimado / Enterrado",
                    "Queimado/enterrado",
                ],
                "Recolhido pela prefeitura": [
                    "Recolhido pela Prefeitura",
                ],
                "Lixeira": [
                    "Cacamba",
                    "Caçamba mais proxima",
                    "Contêiner",
                    "Contenier",
                    "Leva a lixeira lá em baixo.",
                    "Leva até a lixeira",
                    "Leva até a lixeira.",
                    "Leva na lixeira",
                    "Leva na lixeira.",
                    "Leva para a lixeira",
                    "Leva para a lixeira.",
                    "Leva pra lixeira",
                    "Levam na lixeira.",
                    "Levo até lixeira.",
                    "Lixeira",
                    "contenier",
                ],
                "Coleta seletiva": [
                    "Coleta Seletiva",
                ],
                "Outro": [
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ],
            },
            "order": [
                "Outro",
                "Queimado ou enterrado",
                "É jogado no rio ou córrego",
                "Joga na rua, vala ou quintal",
                "Lixeira",
                "Recolhido pela prefeitura",
                "Coleta seletiva",
            ],
        },
    }
    return (ASSERTION_MAP,)


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import random

    import altair as alt
    from great_tables import GT, md, style, loc
    import polars as pl
    import plotly.express as px
    import plotly.graph_objects as go
    from rich import print
    import numpy as np
    return GT, go, loc, md, np, pl, print, px, style


@app.cell
def _(ASSERTION_MAP, name_dict, np, pl, print, px):
    def get_assertion_map():
        """Returns the assertion map used to map answers to their respective categories."""
        from const import ASSERTION_MAP as assertion_map

        return assertion_map


    def get_descriptive_table(
        df_long_first,
        df_long_last,
        question_name,
        describe_by="race",
        mix_pp=True,
    ):
        """Get a table with the count and percentage of answers

        Args:
            df_long_first (_type_): _description_
            df_long_last (_type_): _description_
            question_name (str, optional): _description_. Defaults to "FoodManytimes".
            mix_pp (bool, optional): _description_. Defaults to True.

        Returns:
            _type_: _description_
        """
        from great_tables import GT, md, style, loc

        if mix_pp and describe_by == "race":
            df_long_first = df_long_first.with_columns(
                race=pl.when(
                    (pl.col("race") == "Parda")
                    | (pl.col("race") == "Preta")
                    | (pl.col("race") == "Indígena")
                )
                .then(pl.lit("Negros e indígenas"))
                .otherwise(pl.lit("Brancos e amarelos"))
            )
            df_long_last = df_long_last.with_columns(
                race=pl.when(
                    (pl.col("race") == "Parda")
                    | (pl.col("race") == "Preta")
                    | (pl.col("race") == "Indígena")
                )
                .then(pl.lit("Negros e indígenas"))
                .otherwise(pl.lit("Brancos e amarelos"))
            )

        conditions = (
            (pl.col("question") == question_name)
            & (pl.col(describe_by) != "NA")
            & (pl.col("answer") != "NA")
        )

        first_group_sum = (
            df_long_first.filter(conditions)
            .select(describe_by, "answer")
            .group_by(describe_by)
            .len(name="group_total_first")
        )

        last_group_sum = (
            df_long_last.filter(conditions)
            .select(describe_by, "answer")
            .group_by(describe_by)
            .len(name="group_total_last")
        )

        df_print = (
            (
                df_long_first.filter(conditions)
                .select(describe_by, "answer")
                .group_by("*")
                .len(name="count_first")
                .join(first_group_sum, on=describe_by, how="left")
                .with_columns(
                    pct_first=(
                        pl.col("count_first") / pl.col("group_total_first")
                    ),
                )
            )
            .join(
                df_long_last.filter(conditions)
                .select(describe_by, "answer")
                .group_by("*")
                .len(name="count_last")
                .join(last_group_sum, on=describe_by, how="left")
                .with_columns(
                    pct_last=(pl.col("count_last") / pl.col("group_total_last")),
                ),
                on=[describe_by, "answer"],
                how="full",
                coalesce=True,
            )
            .with_columns(
                count_first=pl.coalesce(
                    pl.col("count_first").cast(pl.Int64), pl.lit(0).cast(pl.Int64)
                ),
                count_last=pl.coalesce(
                    pl.col("count_last").cast(pl.Int64), pl.lit(0).cast(pl.Int64)
                ),
                group_total_first=pl.coalesce(
                    pl.col("group_total_first").cast(pl.Int64),
                    pl.lit(0).cast(pl.Int64),
                ),
                group_total_last=pl.coalesce(
                    pl.col("group_total_last").cast(pl.Int64),
                    pl.lit(0).cast(pl.Int64),
                ),
                pct_first=pl.coalesce(pl.col("pct_first"), pl.lit(0)),
                pct_last=pl.coalesce(pl.col("pct_last"), pl.lit(0)),
            )
            .with_columns(
                delta_count=pl.col("count_last").cast(pl.Int64)
                - pl.col("count_first").cast(pl.Int64),
                delta_pct=pl.col("pct_last") - pl.col("pct_first"),
            )
            .sort(
                "answer", describe_by
            )  # [TODO] Ver se é necessário ordenar por answer
        )

        answer_maps = get_answer_maps_per_question(question_name)
        answer_orders = get_answer_orders_per_question(question_name)

        assert len(answer_maps) == 1, (
            "This function only supports one answer map per question_name"
        )
        assert len(answer_orders) == 1, (
            "This function only supports one answer order per question_name"
        )

        answer_map, answer_order = answer_maps[0], answer_orders[0]

        # Correct answer names with the mapping
        df_print = df_print.with_columns(
            answer=pl.col("answer").replace(answer_map)
        )

        gt_table = (
            GT(
                df_print,
                rowname_col=describe_by,
                groupname_col="answer",
            )
            .row_group_order([i for i in answer_order if i != "NA"])
            .tab_header(
                title=md(f"Respostas à pergunta {question_name}"),
                subtitle=md(
                    f"Distribuição de respostas por `{describe_by}` no tempo inicial e final"
                ),
            )
            .cols_label(
                answer=md("Resposta"),
                count_first=md("Contagem no tempo inicial"),
                count_last=md("Contagem no tempo final"),
                pct_first=md("Porcentagem no tempo inicial"),
                pct_last=md("Porcentagem no tempo final"),
                delta_count=md("Variação de contagem"),
                delta_pct=md("Variação de porcentagem"),
                group_total_first=md("Total do grupo no tempo inicial"),
                group_total_last=md("Total do grupo no tempo final"),
            )
            .fmt_number(
                columns=["count_first", "count_last", "delta_count"],
                decimals=0,
            )
            .fmt_number(
                columns=["pct_first", "pct_last"],
                decimals=2,
                scale_by=100,
                pattern="{x}%",
            )
            .fmt_number(
                columns=[
                    "delta_count",
                ],
                force_sign=True,
            )
            .fmt_number(
                columns=["delta_pct"],
                force_sign=True,
                decimals=2,
                scale_by=100,
                pattern="{x}%",
            )
            .tab_spanner(
                label=md("Tempo inicial"),
                columns=[
                    "count_first",
                    "pct_first",
                    "group_total_first",
                ],
            )
            .tab_spanner(
                label=md("Tempo final"),
                columns=[
                    "count_last",
                    "pct_last",
                    "group_total_last",
                ],
            )
            .tab_spanner(
                label=md("Variação (Final - Inicial)"),
                columns=[
                    "delta_count",
                    "delta_pct",
                ],
            )
            .tab_style(
                style=style.text(weight="bold"),
                locations=loc.body(
                    columns=[
                        "answer",
                        "pct_first",
                        "pct_last",
                        "delta_pct",
                    ]
                ),
            )
            .opt_stylize(style=1, color="blue")
            .opt_all_caps()
        )
        return gt_table


    def cramers_v(df, col1, col2, verbose=False):
        """
        Calculates Cramér's V statistic for association between two categorical columns in a Polars DataFrame.
        Cramér's V is a measure of association between two nominal variables, giving a value between 0 and +1
        (inclusive). A value of 0 indicates no association, and a value of 1 indicates perfect association.
        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame containing the data.
        col1 : str
            The name of the first categorical column.
        col2 : str
            The name of the second categorical column.
        verbose : bool, optional (default=False)
            If True, prints the number of valid rows used in the calculation.
        Returns
        -------
        float
            The Cramér's V statistic for the association between the two columns.
            Returns 0.0 if there is not enough data to compute the statistic.
        Notes
        -----
        - Rows with null values or "NA" in either column are excluded from the calculation.
        - The calculation is based on the chi-square statistic from the contingency table of the two columns.
        """

        # Clean data only for this pair of columns
        df_clean = df.filter(
            ~pl.any_horizontal(
                pl.col([col1, col2]).is_null() | (pl.col([col1, col2]) == "NA")
            )
        ).select(col1, col2)
        if verbose:
            print(f"{col1} X {col2} : {df_clean.height} rows")

        if df_clean.height < 2:  # Not enough data
            return 0.0

        # Create contingency table
        contingency = (
            df_clean.group_by([col1, col2])
            .len()
            .pivot(index=col1, on=col2, values="len")
            .fill_null(0)
        )

        # Convert to numpy array for calculation
        ct = contingency.select(pl.exclude(col1)).to_numpy()

        # Calculate chi-square
        chi2 = 0
        n = ct.sum()
        row_sums = ct.sum(axis=1)
        col_sums = ct.sum(axis=0)

        for i in range(ct.shape[0]):
            for j in range(ct.shape[1]):
                expected = (row_sums[i] * col_sums[j]) / n
                if expected > 0:
                    chi2 += (ct[i, j] - expected) ** 2 / expected

        # Calculate Cramér's V
        n_rows, n_cols = ct.shape
        cramers = np.sqrt(chi2 / (n * min(n_rows - 1, n_cols - 1)))
        return cramers


    def cramers_v_scipy(df, col1, col2, verbose=False):
        """
        Calculates Cramér's V statistic for association between two categorical columns in a Polars DataFrame using scipy.stats.contingency.association.
        Parameters
        ----------
        df : pl.DataFrame
            The Polars DataFrame containing the data.
        col1 : str
            The name of the first categorical column.
        col2 : str
            The name of the second categorical column.
        verbose : bool, optional (default=False)
            If True, prints the number of valid rows used in the calculation.
        Returns
        -------
        float
            The Cramér's V statistic for the association between the two columns.
            Returns 0.0 if there is not enough data to compute the statistic.
        """
        from scipy.stats.contingency import association

        df_clean = df.filter(
            ~pl.any_horizontal(
                pl.col([col1, col2]).is_null() | (pl.col([col1, col2]) == "NA")
            )
        ).select(col1, col2)
        if verbose:
            print(f"{col1} X {col2} : {df_clean.height} rows")

        if df_clean.height < 2:
            return 0.0

        contingency = (
            df_clean.group_by([col1, col2])
            .len()
            .pivot(index=col1, columns=col2, values="len")
            .fill_null(0)
        )
        ct = contingency.select(pl.exclude(col1)).to_numpy()

        return association(ct, method="cramer")


    def get_vars_IGF():
        return [
            "Access",
            "BankAccount",
            "Bathroom",
            "CEP",
            "CommFacilities",
            "CulturalEvent",
            "Documents",
            "Eletricity",
            "Floor",
            "FoodManytimes",
            "Garbage",
            "HealthGenKidsNames",
            "HealthGenNames",
            "HousingProblems",
            "Income",
            "IncomeWorkS3",
            "Internet",
            "Roof",
            "SchoolCurrent",
            "SchoolLast",
            "SchoolLiteracy",
            "SchoolMathLit",
            "Sewer",
            "Walls",
            "Water",
            "WaterFrequency",
        ]


    def plot_variables(
        df_long,
        question_names: list,
        seggregate_favela=False,
        orientation="v",
        palette=px.colors.qualitative.Pastel,
        percentage=False,
        color_col="time",
        compare_by_col=None,
        unique_per_family=False,
        filter_by_order=False,
        title=None,
        subtitle=None,
        verbose=False,
        max_y=None,
    ):
        """Plots the variables from a list of question names, each in a bar chart.

        Args:
            df_long (pl.DataFrame): The polars DataFrame in long format (one row questionnaire entry, having columns 'question', 'answer', 'time', 'FavelaID').
            question_names (list): The list of variable names to plot.
            seggregate_favela (bool, optional): If the bar plot should segment the results by favela. Defaults to False.
            orientation (str, optional): Orientation of the bars in the bar plot, "h" for horizontal, "v" for vertical. Defaults to "v".
            palette (list(str), optional): Color palette as defined in the plotly express lib. Defaults to px.colors.qualitative.Pastel.
            percentage (bool, optional): Wether the plots should show percentages (of the 'time') or absolute numbers of answers. Defaults to False.
        """
        # color_col = "time"

        for question_name in question_names:
            agg_cols = [color_col, "answer"]
            if seggregate_favela:
                agg_cols.append("FavelaID")
            if compare_by_col is not None:
                agg_cols.append(compare_by_col)

            # Initializing columns for the hover data and subtitle variable
            hover_cols = ["len", "percentage", "percentage_wo_na"]
            subtitle = subtitle if subtitle else ""

            # Formatting the hover data
            hover_dict = {k: True for k in agg_cols + hover_cols}
            hover_dict["percentage"] = ":.2f"
            hover_dict["percentage_wo_na"] = ":.2f"

            # Getting answer maps and orders for the question
            answer_maps = get_answer_maps_per_question(question_name)
            answer_orders = get_answer_orders_per_question(question_name)

            assert len(answer_maps) == len(answer_orders), (
                f"Number of answer maps ({len(answer_maps)}) does not match number of answer orders ({len(answer_orders)}) for question '{question_name}'"
            )

            for answer_map, answer_order in zip(answer_maps, answer_orders):
                # Correct answer names with the mapping
                df_plot = df_long.with_columns(
                    answer=pl.when(pl.col("question") == question_name)
                    .then(pl.col("answer").replace(answer_map))
                    .otherwise(pl.col("answer"))
                )

                conditions = pl.col("question") == question_name
                if answer_order != ["NA"] and filter_by_order:
                    if verbose:
                        print("Removing from dataframe:")
                        print(
                            df_plot.filter(
                                conditions
                                & pl.col("answer").is_in(answer_order).not_()
                            )
                            .select(agg_cols)
                            .group_by(agg_cols)
                            .len()
                        )

                    conditions &= pl.col("answer").is_in(answer_order)

                df_plot = (
                    df_plot.filter(conditions)
                    .select(agg_cols)
                    .group_by(agg_cols)
                    .len()
                )

                # [TODO] Solução WIP para agrupar a uma resposta por família.
                # [TODO] Tem que ter assertions pra garantir que não estamos agrupando quando não faz sentido (família com respostas diferentes para a mesma pergunta). Add mensagems claras desse erro.
                if unique_per_family:
                    # If unique_per_family is True, we need to filter the dataframe to have only one row per family
                    df_plot = (
                        df_plot.group_by("id_family_datalake", "time").agg(
                            pl.col("answer").first()
                        )
                        # .rename({"answer": "answer"})
                    )

                df_pct = df_plot.with_columns(
                    percentage=(
                        pl.col("len") / pl.col("len").sum().over("time") * 100
                    )
                )

                df_pct_wo_na = df_plot.filter(
                    pl.col("answer") != "NA"
                ).with_columns(
                    percentage_wo_na=(
                        pl.col("len") / pl.col("len").sum().over("time") * 100
                    )
                )

                df_plot = (
                    df_plot.join(
                        df_pct_wo_na, on=agg_cols, how="left", suffix="_wo_na"
                    )
                    .with_columns(
                        percentage_wo_na=pl.when(
                            pl.col("percentage_wo_na").is_null()
                        )
                        .then(pl.lit("NÃO INCLUÍDO"))
                        .otherwise(pl.col("percentage_wo_na"))
                    )
                    .join(df_pct, on=agg_cols, how="left", suffix="_na")
                )

                amount_col = "len"

                if percentage:
                    amount_col = "percentage"

                if orientation == "v":
                    kwargs = dict(
                        x="answer",
                        y=amount_col,
                        orientation="v",
                        facet_row=compare_by_col,
                    )
                else:
                    kwargs = dict(
                        x=amount_col,
                        y="answer",
                        orientation="h",
                        facet_row=compare_by_col,
                    )
                if compare_by_col is not None:
                    kwargs["height"] = 1000
                    # kwargs["facet_row_spacing"] = 0.05
                    # kwargs["facet_col_spacing"] = 0.05
                    kwargs["facet_row"] = compare_by_col

                if max_y is not None:
                    kwargs["range_y"] = (
                        [0, max_y] if orientation == "v" else [0, None]
                    )
                    kwargs["range_x"] = (
                        [0, max_y] if orientation == "h" else [0, None]
                    )

                fig = px.bar(
                    df_plot,
                    **kwargs,
                    color=color_col,
                    # height=1000,
                    color_discrete_sequence=palette,  # [NOTE] Ver https://plotly.com/python/discrete-color/
                    barmode="group",
                    title=title
                    if title
                    # else f"Respostas à pergunta {question_name}",
                    else name_dict.get(question_name),
                    # title=f"{questions_dict.get(question_name)}", # [TODO] Add function to get the full question from the question name
                    subtitle=f"[{question_name}] " + subtitle,
                    hover_data=hover_dict,
                    hover_name="answer",
                    labels=dict(
                        answer="Resposta",
                        len="Núm. de respostas"
                        if question_name  # Questões com múltiplas asserções
                        in [
                            "BathroomQualit",
                            "HousingProblems",
                            "CommFacilities",
                            "Garbage",
                            "HealthGenKidsNames",
                            "HealthGenNames",
                            "Internet",
                            "Documents",
                            "IncomeDesc",
                            "JobSatisfaction",
                        ]
                        else "Núm. de famílias",
                        time="Período",
                        percentage="% no período (com NAs)",
                        percentage_wo_na="% no período (excluindo NAs)",
                        FavelaID="Favela",
                    ),
                    category_orders={
                        "time": ["FIRST", "LAST"],
                        "drug_addiction": ["Não", "Talvez", "Sim", "NA"],
                        # "time": ["0", "1", "2", "3", "FIRST", "LAST"],
                        "answer": answer_order,
                    },
                )
                fig.update_layout(margin=dict(t=100, l=100, r=100, b=100))
                # fig.show()

                if verbose:
                    print("\nPlotting question:", question_name)
                    print("\nAnswer order:", answer_order)
                    print("\nAnswer map:", answer_map)
                    print(
                        "\nOptions on plot:",
                        sorted(
                            df_plot.select("answer").unique().to_series().to_list()
                        ),
                    )
        return fig


    def get_answer_map_per_question(question_name):
        assertion_map = ASSERTION_MAP
        new_to_list_of_old = assertion_map.get(question_name, {}).get("map", {})
        old_to_new = {
            value: key
            for key, values in new_to_list_of_old.items()
            for value in values
        }
        return old_to_new


    def get_answer_maps_per_question(question_name):
        # Handles assertion_map entries that are a dict or a list of dicts
        assertion_map = ASSERTION_MAP
        entry = assertion_map.get(question_name, {})
        answer_maps = []

        if not isinstance(entry, list):
            entry = [entry]

        for e in entry:
            if isinstance(e, dict):
                if "map" in e:
                    new_to_list_of_old = e["map"]
                    old_to_new = {
                        v: k
                        for k, vals in new_to_list_of_old.items()
                        for v in vals
                    }
                    answer_maps.append(old_to_new)
                else:
                    # If no map is defined, return an empty map
                    answer_maps.append({})

        return answer_maps


    def get_answer_order_per_question(question_name):
        assertion_map = ASSERTION_MAP
        if question_name.startswith("Categoria"):
            return [
                "NA",
                "E1",
                "E2",
                "P1",
                "P2",
                "D",
            ]

        return assertion_map.get(question_name, {}).get("order", ["NA"])


    def get_answer_orders_per_question(question_name):
        assertion_map = ASSERTION_MAP
        ORDER_KEY = "order"

        if question_name.startswith("Categoria"):
            return [
                [
                    "NA",
                    "E1",
                    "E2",
                    "P1",
                    "P2",
                    "D",
                ]
            ]

        entry = assertion_map.get(question_name, {})
        answer_orders = []

        if not isinstance(entry, list):
            entry = [entry]

        for e in entry:
            if isinstance(e, dict):
                if ORDER_KEY in e:
                    answer_orders.append(e[ORDER_KEY])
                else:
                    answer_orders.append(["NA"])

        return answer_orders


    def enrich_first_and_last_time(df_long):
        """Enriches the dataframe with the first and last time for each family.

        Args:
            df_long (pl.DataFrame): The polars DataFrame in long format (one row questionnaire entry, having columns 'question', 'answer', 'time', 'FavelaID').

        Returns:
            pl.DataFrame: The dataframe enriched with columns 'time_first' and 'time_last', with the first and last collection times for each family.
        """
        # Gets the first time for each family
        df_first = (
            df_long.filter(
                (pl.col("question") == "CategoriaIGF") & (pl.col("answer") != "NA")
            )
            .select("id_family_datalake", "column", "time")
            .sort("id_family_datalake", "time")
            .group_by("id_family_datalake")
            .first()
            .select("id_family_datalake", "time")
        )

        # Gets the last time for each family
        df_last = (
            df_long.filter(
                (pl.col("question") == "CategoriaIGF") & (pl.col("answer") != "NA")
            )
            .select("id_family_datalake", "column", "time")
            .sort("id_family_datalake", "time")
            .group_by("id_family_datalake")
            .last()
            .select("id_family_datalake", "time")
        )

        # Join the first and last times to the main dataframe
        df_long_periods = df_long.join(
            df_first, on="id_family_datalake", how="left", suffix="_first"
        )

        # Join the last time to the main dataframe
        df_long_periods = df_long_periods.join(
            df_last, on="id_family_datalake", how="left", suffix="_last"
        )

        # Remove families that only have one time difference between the first and last time (this filters out 57 families, leaving only the 616 remaining families)
        df_long_periods = df_long_periods.filter(
            pl.col("time_last").cast(pl.Int8) - pl.col("time_first").cast(pl.Int8)
            > 1
        )

        return df_long_periods
    return (
        cramers_v,
        enrich_first_and_last_time,
        get_answer_maps_per_question,
        get_answer_orders_per_question,
        get_vars_IGF,
        plot_variables,
    )


@app.cell
def _():
    mais_melhor = {
        "BathroomQualit": [
            "Box ou cortina que fecha o chuveiro"
            "Chuveiro com água quente"
            "Parede de azulejo"
            "Piso de azulejo"
            "Porta externa que fecha o banheiro"
            "Privada com tampa"
        ],
        "CommFacilities": [
            "Agua Encanada",
            "Água encanada",
            "Boas condições das ruas, vielas ou escadas que dão acesso à comunidade",
            "Creche Publica",
            "Creche Pública",
            "Escola Publica",
            "Escola Pública",
            "Esgoto",
            "Acesso à rede de esgoto",
            "Espaços para reuniões comunitárias",
            "Hospital Publico",
            "Hospital Público",
            "Iluminacao Publica",
            "Iluminação Pública",
            "Opcoes de Lazer",
            "Opções de lazer",
            "Pavimentação das ruas e vielas da comunidade",
            "Posto de Saude",
            "Posto de Saúde",
            "Transporte Publico",
            "Transporte Público",
            "Coleta de Lixo",
        ],
    }

    menos_melhor = {
        "HousingProblems": [
            "Alagamento Inundacao",
            "Chuva Goteiras",
            "Cozinha Com Lenha",
            "Cupim",
            "Deslizamento",
            "Desmoronamento",
            "Incendio",
            "Outro",
            "Ratos Baratas Animais Indesejados",
            "Solapamento",
            "Umidade Mofo",
        ],
        "HealthGenNames": ["TODAS"],  # [TODO]
        "HealthGenKidsNames": ["TODAS"],  # [TODO]
    }

    # %%
    # LISTA DE VARIÁVEIS COM MÚLTIPLAS ASSERÇÕES POR RESPOSTA

    # - "BathroomQualit" # MAIS MELHOR
    # - "HousingProblems" # MENOS MELHOR
    # - "CommFacilities" MAIS MELHOR
    # - "Garbage"  # SÓ LIMPAR AS RESPOSTAS (é uma asserção)
    # ----------- DOIS GRAFICOS TAMBEM ((1) contagem geral e (2) incidência)
    # - "HealthGenKidsNames"
    # - "HealthGenNames"
    # -----------
    # - "Internet"  # DOIS GRÁFICOS:
    # 1) Não tenho acesso à internet e Tenho acesso à internet (Todas as outras alternativas)
    # 2) Quantas vezes cada alternativa foi selecionada no tempo inicial e final
    # -----------
    # - "Documents"  # DOIS GRÁFICOS TAMBÉM, IGUAL INTERNET
    # [TODO]
    # "Não tenho nenhum documento" # [TODO] Não usar a coluna, criar uma nova para os casos que sejam só NA;NA..NA
    # -----------

    # %%
    return


@app.cell
def _(print):
    def find_in_columns_from_df_long(df_long, search_term, col_name="column"):
        filtered_list = [
            item
            for item in df_long.select(col_name).unique().to_series().to_list()
            if search_term in item
        ]
        print(filtered_list)
    return


if __name__ == "__main__":
    app.run()

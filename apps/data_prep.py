import marimo

__generated_with = "0.15.2"
app = marimo.App(width="columns")


@app.cell
def _(pl):
    def enrich_first_and_last_time(df_long):
        """Enriches the dataframe with the first and last time for each family. NAs are removed before computing first and last times.

        Args:
            df_long (pl.DataFrame): The polars DataFrame in long format (one row questionnaire entry, having columns 'question', 'answer', 'time', 'FavelaID').

        Returns:
            pl.DataFrame: The dataframe enriched with columns 'time_first' and 'time_last', with the first and last collection times for each family.
        """
        # Gets the first time for each family, per question
        df_first = (
            df_long.filter((pl.col("answer") != "NA"))
            .select("id_family_datalake", "column", "time", "question")
            .sort("id_family_datalake", "time")
            .group_by("id_family_datalake", "question")
            .first()
            .select("id_family_datalake", "question", "time")
        )

        # Gets the last time for each family, per question
        df_last = (
            df_long.filter((pl.col("answer") != "NA"))
            .select("id_family_datalake", "column", "time", "question")
            .sort("id_family_datalake", "time")
            .group_by("id_family_datalake", "question")
            .last()
            .select("id_family_datalake", "question", "time")
        )

        # Join the first and last times to the main dataframe
        # After this step df_long_periods will have T x F x Q rows, where T is the number of times, F is the number of families, and Q is the number of questions
        # Later on we'll filter to keep only the first and last times for each family/question, and
        # keep only the families that have more than one time difference between first and last time
        df_long_periods = df_long.join(
            df_first,
            on=["id_family_datalake", "question"],
            how="left",
            suffix="_first",
        )
        df_long_periods = df_long_periods.join(
            df_last,
            on=["id_family_datalake", "question"],
            how="left",
            suffix="_last",
        )


        return df_long_periods
    return (enrich_first_and_last_time,)


@app.cell
def _(mo, pl):
    # Leitura do df original em CSV
    base_1 = str(
        mo.notebook_location()
        / "public"
        / "Carol_DataBaseFull_26082025_anonimizado.csv"
    )
    base_2 = str(
        mo.notebook_location() / "public" / "Carol_DataBaseFull_Limpa.csv"
    )
    df_original = pl.read_csv(base_1)
    df_old = pl.read_csv(base_2)

    # Join com a versão anterior dos dados por conta de colunas que desapareceram na nova versão
    df_original = df_original.join(df_old, on=["id_family_datalake"])

    SINGLE_TIME_QUESTIONS = ["Gender", "HowManyPHHH", "Race"]
    return SINGLE_TIME_QUESTIONS, df_original


@app.cell
def _(pl):

    # PARA VALIDAR O ASSERTION_MAP
    def check_answer_count(df_long):
        return (
            df_long.select("question", "answer")
            .filter(
                True
                & (pl.col.question.str.contains("Average").not_())
                & (pl.col.question.str.contains("Categoria").not_())
                & (pl.col.question.is_in(["Income"]).not_())
            )
            .group_by("*")
            .len()
            .sort("question")
        )
    return


@app.cell
def _(
    add_derivate_cols,
    df_original,
    enrich_first_and_last_time,
    enrich_time,
    filter_cols_of_interest,
    filter_first_and_last,
    fix_multiple_assertion_sep,
    fix_wide_cols,
    list_to_str,
    long_to_wide,
    map_and_join_answers,
    mo,
    pl,
    remove_small_interval,
    rename_favela,
    set_first_and_last,
    split_assertions,
    wide_to_long,
):
    _df = (
        df_original.pipe(fix_wide_cols)
        .pipe(wide_to_long)
        .pipe(filter_cols_of_interest)
        .pipe(enrich_time)
        .pipe(fix_multiple_assertion_sep)
        .pipe(rename_favela)
        .pipe(split_assertions)
        # .pipe(add_new_questions) # TODO: Mudar! Mudou o nome das colunas para Health e HealthKids
        .pipe(enrich_first_and_last_time)
        .pipe(remove_small_interval)
        .pipe(filter_first_and_last)
        .pipe(map_and_join_answers)
        .filter(
            True
            # & (pl.col.id_family_datalake == "1..7.2021.338743R_311770")
            # & (pl.col.question == "Garbage")
            # & (pl.col.original_column == "CH_IB_Race_T0")
            # & (pl.col.question.is_in(["FamilyRelations", "DreamsKids"]))
        )
        .pipe(set_first_and_last)
    )

    _log_df = _df.select(
        "id_family_datalake", "question_name", "time_first", "time_last"
    ).unique()

    # _df = _df.pipe(check_answer_count) # AQUI PARA EXPORTAR AS RESPOSTAS E VALIDAR ASSERTION_MAP

    _df = (
        _df.filter(
            True
            # & (pl.col.id_family_datalake == "1..7.2021.338743R_311770")
            # & (pl.col.question == "Garbage")
            # & (pl.col.question.is_in(["DreamsKids_FIRST", "DreamsKids_LAST"]))
            # & (
            #     pl.col.question.is_in(
            #         [
            #             "Income_FIRST",
            #             "Income_LAST",
            #             "HowManyPHHH",
            #         ]
            #     )
            # )
            # & (pl.col.question.is_in(["Garbage_FIRST", "Garbage_LAST"]))
        )
        .pipe(long_to_wide)
        .pipe(list_to_str)
        .pipe(add_derivate_cols)
    )


    _idx_cols = ["id_family_datalake", "FavelaID"]
    _var_cols = sorted(list(set(_df.columns) - set(_idx_cols)))
    _final_cols = _idx_cols + _var_cols

    base_wide = _df
    base_log = _log_df
    base_long = (
        base_wide.unpivot(
            index=[
                "id_family_datalake",
                "FavelaID",
                "Gender",
                "Race",
                "HowManyPHHH",
            ],
            variable_name="original_column",
            value_name="answer",
        )
        .with_columns(
            question=pl.col("original_column").str.extract(r"(.*)_.*$", 1),
            time=pl.col("original_column").str.extract(r"(.*)_(.*)$", 2),
        )
        .select(
            "id_family_datalake",
            "question",
            "answer",
            "time",
            "FavelaID",
            "Gender",
            "Race",
            "HowManyPHHH",
        )
    )

    base_wide.write_csv(str(mo.notebook_location() / "public" / "base_wide.csv"))
    base_long.write_csv(str(mo.notebook_location() / "public" / "base_long.csv"))
    base_log.write_csv(str(mo.notebook_location() / "public" / "base_log.csv"))
    return base_log, base_long, base_wide


@app.cell
def _(base_log, base_long, base_wide, mo):
    mo.vstack(
        [
            base_wide,
            base_long,
            base_log,
        ]
        # .select(_final_cols)
    )
    return


@app.cell
def _(ASSERTION_MAP, SINGLE_TIME_QUESTIONS, pl):
    # def add_derivate_cols(df_wide):
    #     "Adiciona colunas derivadas de outras, como etapa final do processamento da base."

    #     # _df = df_wide
    #     _df = df_wide.with_columns(
    #         IncomePerCapita_FIRST=pl.when(pl.col.Income_FIRST.str.contains("NA"))
    #         .then(pl.lit("NA"))
    #         .otherwise(
    #             # pl.lit("NOT-NA")
    #             pl.col.Income_FIRST.cast(pl.Float64)
    #             / pl.col.HowManyPHHH.cast(pl.Int8)
    #         )
    #     )

    #     return _df


    def add_derivate_cols(df_wide):
        "Adiciona colunas derivadas de outras, como etapa final do processamento da base."

        # IncomePerCapita

        _df_first = df_wide.filter(
            (pl.col.Income_FIRST != "NA") & (pl.col.HowManyPHHH != "NA")
        ).with_columns(
            IncomePerCapita_FIRST=pl.col.Income_FIRST.cast(pl.Float32)
            / pl.col.HowManyPHHH.cast(pl.Int8)
        ).select("id_family_datalake","IncomePerCapita_FIRST")
        _df_last = df_wide.filter(
            (pl.col.Income_LAST != "NA") & (pl.col.HowManyPHHH != "NA")
        ).with_columns(
            IncomePerCapita_LAST=pl.col.Income_LAST.cast(pl.Float32)
            / pl.col.HowManyPHHH.cast(pl.Int8)
        ).select("id_family_datalake","IncomePerCapita_LAST")

        _df = (
            df_wide
                .join(_df_first, on="id_family_datalake", how="left")
                .join(_df_last, on="id_family_datalake", how="left")
        ).fill_null(pl.lit("NA"))

        return _df


    def remove_small_interval(df_long):
        """
        Remove perguntas de famílias caso a diferença entre FIRST e LAST seja menor ou igual a 1.
        A exceção são perguntas feitas uma única vez ("Gender", "HowManyPHHH", "Race").
        Ex.: Se uma família tem, para a pergunta "SchoolLast", respostas nos tempos 1 e 2, removemos essa pergunta dessa família.
        """

        return df_long.filter(
            (
                pl.col("time_last").cast(pl.Int8)
                - pl.col("time_first").cast(pl.Int8)
                > 1
            )
            | (pl.col.question.is_in(SINGLE_TIME_QUESTIONS))
        )


    def set_first_and_last(df_long):
        _df = df_long.select(
            "id_family_datalake",
            "FavelaID",
            "question",
            "answer",
            "time",
            "time_first",
            "time_last",
        )

        _df_first = (
            _df.filter(
                (pl.col.time == pl.col.time_first)
                & (pl.col.question.is_in(SINGLE_TIME_QUESTIONS).not_())
            )
            .with_columns(question_name=pl.col.question)
            .with_columns(question=pl.col.question + pl.lit("_FIRST"))
        )
        _df_last = (
            _df.filter(
                (pl.col.time == pl.col.time_last)
                & (pl.col.question.is_in(SINGLE_TIME_QUESTIONS).not_())
            )
            .with_columns(question_name=pl.col.question)
            .with_columns(question=pl.col.question + pl.lit("_LAST"))
        )
        _df_single_time = _df.filter(
            (pl.col.question.is_in(SINGLE_TIME_QUESTIONS))
        ).with_columns(question_name=pl.col.question)
        _df = pl.concat([_df_first, _df_last, _df_single_time])
        return _df
        # return _df.select("id_family_datalake", "FavelaID", "question", "answer")


    def long_to_wide(df_long):
        "Faz o pivot, passando de formato long (uma linha por resposta) para wide (uma linha por família)"
        return df_long.pivot(
            on="question",
            index=[
                "id_family_datalake",
                "FavelaID",
            ],
            values="answer",
            aggregate_function=pl.element().implode(),  # TODO find a better option (only aggregate the questions that need it)
        )


    def list_to_str(df_long):
        "Usada depois do long_to_wide(), transforma as listas de respostas em strings únicas separadas por ';'."
        return df_long.with_columns(
            # Transforma listas vazias em ["NA"] antes de fazer o join dos elementos das listas.
            pl.col(pl.List).map_elements(
                lambda x: ["NA"] if len(x) == 0 else x,
                return_dtype=pl.List(pl.Utf8),
            )
        ).with_columns(pl.col(pl.List).list.join(";"))


    def get_answer_map_per_question(question_name):
        assertion_map = ASSERTION_MAP
        new_to_list_of_old = assertion_map.get(question_name, {}).get("map", {})
        old_to_new = {
            value: key
            for key, values in new_to_list_of_old.items()
            for value in values
        }
        return old_to_new


    def map_and_join_answers(df_long):
        "Aplica o mapeamento de respostas de acordo com o ASSERTION_MAP, para ajustar respostas fora do padrão."
        # Your master dict structure (question -> answer mappings)
        mappings = {
            k: get_answer_map_per_question(k) for k, v in ASSERTION_MAP.items()
        }

        # Convert to a flat mapping dataframe
        mapping_rows = []
        for question, answer_map in mappings.items():
            for original, standardized in answer_map.items():
                mapping_rows.append(
                    {
                        "question": question,
                        "original_answer": original,
                        "standardized_answer": standardized,
                    }
                )

        mapping_df = pl.DataFrame(mapping_rows)

        # Join with your long-format data
        return (
            df_long.join(
                mapping_df,
                left_on=["question", "answer"],
                right_on=["question", "original_answer"],
                how="left",
            )
            .with_columns(
                # Keep original answer for inspection
                pl.col("answer").alias("original_answer")
            )
            .with_columns(
                # Use standardized answer if available, otherwise keep original
                pl.coalesce("standardized_answer", "answer").alias("answer")
            )
        )
    return (
        add_derivate_cols,
        list_to_str,
        long_to_wide,
        map_and_join_answers,
        remove_small_interval,
        set_first_and_last,
    )


@app.cell
def _(pl):
    def fix_wide_cols(df_wide):
        "Conserta problemas de coluna que precisam ser feitos antes de passar para long."

        _BathroomQualit_cols = [
            "M_DC_BathroomQualit_1_T0",
            "M_DC_BathroomQualit_2_T0",
            "M_DC_BathroomQualit_3_T0",
            "M_DC_BathroomQualit_4_T0",
            "M_DC_BathroomQualit_5_T0",
            "M_DC_BathroomQualit_6_T0",
            "M_DC_BathroomQualit_7_T0",
            "M_DC_BathroomQualit_8_T0",
        ]
        _df = (
            df_wide
            # [M_DC_BathroomQualit_..._T0] -> M_DC_BathroomQualit_T0
            .with_columns(
                M_DC_BathroomQualit_T0=pl.concat_str(
                    _BathroomQualit_cols, separator=";"
                )
            )
            # Merge E_DI_DreamsKids_T0 e E_DI_DreamsKids_T0 -> DreamsKids_T0
            .with_columns(
                DreamsKids_T0=pl.when(pl.col.E_DI_DreamsKids_T0 != "NA")
                .then(pl.col.E_DI_DreamsKids_T0)
                .otherwise(pl.col.P_DC_DreamsKids_T0)
            )
            # HealthGenNames e HealthGenKidsNames -> Health e HealthKids
            .with_columns(
                # HealthGenNames
                pl.when(pl.col.Ref_GenIndex_param_T0 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenNames_T0)
                .alias("Health_T0"),
                pl.when(pl.col.Ref_GenIndex_param_T1 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenNames_T1)
                .alias("Health_T1"),
                pl.when(pl.col.Ref_GenIndex_param_T2 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenNames_T2)
                .alias("Health_T2"),
                pl.when(pl.col.Ref_GenIndex_param_T3 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenNames_T3)
                .alias("Health_T3"),
                # HealthGenKidsNames
                pl.when(pl.col.Ref_GenKidsIndex_param_T0 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenKidsNames_T0)
                .alias("HealthKids_T0"),
                pl.when(pl.col.Ref_GenKidsIndex_param_T1 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenKidsNames_T1)
                .alias("HealthKids_T1"),
                pl.when(pl.col.Ref_GenKidsIndex_param_T2 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenKidsNames_T2)
                .alias("HealthKids_T2"),
                pl.when(pl.col.Ref_GenKidsIndex_param_T3 == "sem dado")
                .then(pl.lit("Nenhuma opção"))
                .otherwise(pl.col.HealthGenKidsNames_T3)
                .alias("HealthKids_T3"),
            )
        )

        return _df


    def wide_to_long(df_wide):
        """
        Faz o unpivot do dataframe wide (uma linha por família, várias colunas),
        passando-o para o formato long (uma linha por resposta)
        """
        col_dict = get_col_dict()

        return df_wide.unpivot(
            index=[
                "id_family_datalake",
                "FavelaID",
            ],  # + list(profile_cols.keys()),
            variable_name="original_column",
            value_name="answer",
        ).join(  # Join with the column dictionary to get the new column names
            pl.DataFrame(
                {
                    "original_column": list(col_dict.keys()),
                    "column": list(col_dict.values()),
                }
            ),
            on="original_column",
            how="left",
        )


    def enrich_time(df_long):
        """
        Extrai uma coluna time com o tempo em que a pergunta foi realizada [0,1,2,3], baseando-se no nome original da coluna.
        Cria a coluna question com o identificador da pergunta, que é o mesmo independente do tempo.
        """
        # Extract time
        return df_long.with_columns(
            time=pl.col("column").str.extract(r"_T(\d)$", 1),
            question=pl.col("column").str.extract(r"(.*_)*(.*)_T(\d)$", 2),
        )


    def fix_multiple_assertion_sep(df_long):
        """
        Padroniza o separador em questions com múltiplas asserções. Em algumas perguntas o separador é ','.
        Padroniza-se ';', usado no restante das perguntas.
        """

        #     # Padronizar o separados em questions com múltiplas asserções
        return df_long.with_columns(
            answer=pl.when(
                (pl.col("question") == "Health")
                | (pl.col("question") == "HealthKids")
                # (pl.col("question") == "HealthGenNames")
                # | (pl.col("question") == "HealthGenKidsNames")
            )
            .then(pl.col("answer").str.replace_all(",", ";"))
            .otherwise(pl.col("answer"))
        )


    def rename_favela(df_long):
        "Renomeia Boca do Sapo (São Paulo) -> Favela dos Sonhos (São Paulo)"

        return df_long.with_columns(
            FavelaID=pl.when(pl.col("FavelaID") == "Boca do Sapo (São Paulo)")
            .then(pl.lit("Favela dos Sonhos (São Paulo)"))
            .otherwise(pl.col("FavelaID"))
        )


    def filter_cols_of_interest(df_long):
        "Mantém apenas as colunas de interesse."

        col_dict = get_col_dict()
        return df_long.filter(pl.col("column").is_in(list(col_dict.values())))


    def split_assertions(df_long):
        "Separa as asserções no caso de haver múltiplas asserções na resposta. Assume que o separador é ';'."

        _df_long = pl.concat(
            [
                # Elimina os "NA" no meio das múltiplas asserções e faz o explode (ex.: 1 row com "A;B;NA;C" -> 3 rows: "A", "B", "C")
                (
                    df_long.filter((pl.col.answer != "NA"))
                    .with_columns(answer=pl.col("answer").str.split(";"))
                    .explode("answer")
                    .filter(pl.col("answer") != "NA")
                ),
                # Concatena com as respostas que são "NA;NA;...;NA" (nenhuma opção)
                df_long.filter(pl.col("answer").str.contains(r"^(NA;)+NA$")),
                # Concatena com as respostas que são "NA" (não respondidas)
                df_long.filter((pl.col.answer == "NA")),
            ]
        )

        return _df_long.with_columns(
            pl.when(pl.col("answer").str.contains(r"^(NA;)+NA$"))
            .then(pl.lit("Nenhuma opção"))
            .otherwise(pl.col("answer"))
            .alias("answer")
        )
    return (
        enrich_time,
        filter_cols_of_interest,
        fix_multiple_assertion_sep,
        fix_wide_cols,
        rename_favela,
        split_assertions,
        wide_to_long,
    )


@app.cell
def _(pl):
    def filter_first_and_last(df_long_enriched):
        """
        Filtra o datafram mantendo apenas a primeira (FIRST) e última (LAST) resposta de cada família para cada pergunta.

        Args:
            df_long_enriched (pl.DataFrame): Polars DataFrame com as colunas 'time_first' e 'time_last'.

        Returns:
            pl.DataFrame: Dataframe filtrado com a primeira e última resposta de cada família.
        """
        df_filtered = df_long_enriched.filter(
            (pl.col("time") == pl.col("time_first"))
            | (pl.col("time") == pl.col("time_last"))
        )

        return df_filtered
    return (filter_first_and_last,)


@app.cell
def _(add_new_questions, df_original, get_df_long):
    # Passa o dataframe para o formato long (uma row por resposta, ao invés de uma row por família)
    df_long = get_df_long(df_original)

    df_long = add_new_questions(df_long)
    # df_long.write_csv(str(mo.notebook_location() / "public" / "df_long.csv"))
    return


@app.cell
def _(pl):
    def add_new_questions(df_long):
        # MUDAR!!! MUDOU A COLUNA!

        _HealthGenKidsNames_df = (
            df_long.filter(pl.col("question") == "HealthGenKidsNames")
            .group_by("id_family_datalake", "time")
            .agg(
                pl.col("answer").alias("answer"),
            )
            .with_columns(
                answer=pl.when(pl.col("answer") == ["NA"])
                .then(pl.lit("Nenhuma doença"))
                .otherwise(pl.col("answer").list.len()),
                question=pl.lit("CountHealthGenKidsNames"),
            )
        )

        _HealthGenNames_df = (
            df_long.filter(pl.col("question") == "HealthGenNames")
            .group_by("id_family_datalake", "time")
            .agg(
                pl.col("answer").alias("answer"),
            )
            .with_columns(
                answer=pl.when(pl.col("answer") == ["NA"])
                .then(pl.lit("Nenhuma doença"))
                .otherwise(pl.col("answer").list.len()),
                question=pl.lit("CountHealthGenNames"),
                # .alias("answer"),
            )
        )

        return pl.concat(
            [
                df_long,
                _HealthGenNames_df.join(
                    df_long, on=["id_family_datalake", "time"], how="right"
                )
                .select(df_long.columns)
                .with_columns(
                    column=pl.col.question,
                    original_column=pl.col.question,
                )
                .unique(),
                _HealthGenKidsNames_df.join(
                    df_long, on=["id_family_datalake", "time"], how="right"
                )
                .select(df_long.columns)
                .with_columns(
                    column=pl.col.question,
                    original_column=pl.col.question,
                )
                .unique(),
            ]
        )
    return (add_new_questions,)


@app.cell
def _():
    import polars as pl
    import marimo as mo
    return mo, pl


@app.cell
def _(pl):
    def correct_answer_names(df_long, question_name, answer_map):
        """Corrects the answer names in the dataframe to match the expected values."""

        df_long = df_long.with_columns(
            answer=pl.when(pl.col("question") == question_name)
            .then(pl.col("answer").replace(answer_map))
            .otherwise(pl.col("answer"))
        )

        return df_long


    def find_in_columns_from_df_long(df_long, search_term, col_name="column"):
        filtered_list = [
            item
            for item in df_long.select(col_name).unique().to_series().to_list()
            if search_term in item
        ]
        print(filtered_list)
    return


@app.function
def get_col_dict():
    """Returns a dictionary with the column names to be used in the dataframe.
    The keys are the original column names and the values are the new column names.
    """
    vars_T1_3 = [
        # "HealthGenNames",
        # "HealthGenKidsNames",
        "Health",
        "HealthKids",
        "M_ATI_Walls",
        "M_ATI_Roof",
        "M_ATI_Floor",
        "M_ATI_Water",
        "M_ATI_WaterFrequency",
        "M_ATI_Eletricity",
        "M_ATI_Sewer",
        "M_ATI_Bathroom",
        "BathroomQualit",
        "CommFacilities",
        "HousingProblems",
        "H_ATI_FoodManytimes",
        "Internet",
        "Documents",
        "C_ATI_CEP",
        "E_ATI_SchoolLiteracy",
        "E_ATI_SchoolMathLit",
        "E_ATI_SchoolLast",
        "E_ATI_SchoolCurrent",
        "E_ATI_KidsSchool2N",
        "ES_ATI_Access",
        "ES_ATI_CulturalEvent",
        "R_ATI_Income",
        "R_ATI_IncomeWorkS3",
        "R_ATI_BankAccount",
        "Garbage",
        "HousingProblems",
        "IncomeDesc",
        "JobSatisfaction",
        "P_ATI_DreamsKids",
    ]
    complementary_vars = {
        "P_ATI_FamilyRelation_T1":"FamilyRelations_T1",
        "P_ATI_FamilyRelation_T2":"FamilyRelations_T2",
        "P_ATI_FamilyRelation_T3":"FamilyRelations_T3",
    }
    vars_T1_3_main = [
        var.split("_")[-1] if "_" in var else var for var in vars_T1_3
    ]

    dic_T1_3_list = [
        {f"{k}_T{t}": f"{v}_T{t}" for k, v in zip(vars_T1_3, vars_T1_3_main)}
        for t in range(1, 4)
    ]
    dic_T1_3 = (
        dic_T1_3_list[0]
        | dic_T1_3_list[1]
        | dic_T1_3_list[2]
        | complementary_vars
    )

    col_dict = (
        {
            "Categoria_IGF_T0": "CategoriaIGF_T0",
            "P_DC_FamilyRelations_T0": "FamilyRelations_T0",
            # TODO: Definir qual usar
            # "P_DC_DreamsKids_T0": "DreamsKids_T0"
            # "E_DI_DreamsKids_T0": "DreamsKids_T0",
            "DreamsKids_T0": "DreamsKids_T0",
            "Categoria_Income_T0": "CategoriaIncome_T0",
            "Categoria_Environment_T0": "CategoriaEnvironment_T0",
            "Categoria_Housing_T0": "CategoriaHousing_T0",
            "Categoria_Schooling_T0": "CategoriaSchooling_T0",
            "Categoria_Health_T0": "CategoriaHealth_T0",
            "Categoria_WomanAutonomy_T0": "CategoriaWomanAutonomy_T0",
            "Categoria_Citizenship_T0": "CategoriaCitizenship_T0",
            "Categoria_FirstInfancy_T0": "CategoriaFirstInfancy_T0",
            "Categoria_Culture_T0": "CategoriaCulture_T0",
            "Categoria_IGF_T1": "CategoriaIGF_T1",
            "Categoria_Income_T1": "CategoriaIncome_T1",
            "Categoria_Housing_T1": "CategoriaHousing_T1",
            "Categoria_Schooling_T1": "CategoriaSchooling_T1",
            "Categoria_Health_T1": "CategoriaHealth_T1",
            "Categoria_WomanAutonomy_T1": "CategoriaWomanAutonomy_T1",
            "Categoria_Citizenship_T1": "CategoriaCitizenship_T1",
            "Categoria_FirstInfancy_T1": "CategoriaFirstInfancy_T1",
            "Categoria_Culture_T1": "CategoriaCulture_T1",
            "Categoria_Environment_T1": "CategoriaEnvironment_T1",
            "Categoria_IGF_T2": "CategoriaIGF_T2",
            "Categoria_Income_T2": "CategoriaIncome_T2",
            "Categoria_Housing_T2": "CategoriaHousing_T2",
            "Categoria_Schooling_T2": "CategoriaSchooling_T2",
            "Categoria_Health_T2": "CategoriaHealth_T2",
            "Categoria_WomanAutonomy_T2": "CategoriaWomanAutonomy_T2",
            "Categoria_Citizenship_T2": "CategoriaCitizenship_T2",
            "Categoria_FirstInfancy_T2": "CategoriaFirstInfancy_T2",
            "Categoria_Culture_T2": "CategoriaCulture_T2",
            "Categoria_Environment_T2": "CategoriaEnvironment_T2",
            "Categoria_IGF_T3": "CategoriaIGF_T3",
            "Categoria_Income_T3": "CategoriaIncome_T3",
            "Categoria_Housing_T3": "CategoriaHousing_T3",
            "Categoria_Schooling_T3": "CategoriaSchooling_T3",
            "Categoria_Health_T3": "CategoriaHealth_T3",
            "Categoria_WomanAutonomy_T3": "CategoriaWomanAutonomy_T3",
            "Categoria_Citizenship_T3": "CategoriaCitizenship_T3",
            "Categoria_FirstInfancy_T3": "CategoriaFirstInfancy_T3",
            "Categoria_Culture_T3": "CategoriaCulture_T3",
            "Categoria_Environment_T3": "CategoriaEnvironment_T3",
            #
            "IGF_SimpleAverage_T0": "AverageIGF_T0",
            "Factor_Income_SimpleAverage_N_T0": "AverageIncome_T0",
            "Factor_Housing_SimpleAverage_N_T0": "AverageHousing_T0",
            "Factor_Schooling_SimpleAverage_Threshold_N_T0": "AverageSchooling_T0",
            "Factor_Health_SimpleAverage_N_T0": "AverageHealth_T0",
            "Factor_WomanAutonomy_SimpleAverage_N_T0": "AverageWomanAutonomy_T0",
            "Factor_Citizenship_SimpleAverage_N_T0": "AverageCitizenship_T0",
            "Factor_FirstInfancy_SimpleAverage_Threshold_N_T0": "AverageFirstInfancy_T0",
            "Factor_Culture_SimpleAverage_N_T0": "AverageCulture_T0",
            "Factor_Environment_SimpleAverage_N_T0": "AverageEnvironment_T0",
            "IGF_SimpleAverage_T1": "AverageIGF_T1",
            "Factor_Income_SimpleAverage_N_T1": "AverageIncome_T1",
            "Factor_Housing_SimpleAverage_N_T1": "AverageHousing_T1",
            "Factor_Schooling_SimpleAverage_Threshold_N_T1": "AverageSchooling_T1",
            "Factor_Health_SimpleAverage_N_T1": "AverageHealth_T1",
            "Factor_WomanAutonomy_SimpleAverage_N_T1": "AverageWomanAutonomy_T1",
            "Factor_Citizenship_SimpleAverage_N_T1": "AverageCitizenship_T1",
            "Factor_FirstInfancy_SimpleAverage_Threshold_N_T1": "AverageFirstInfancy_T1",
            "Factor_Culture_SimpleAverage_N_T1": "AverageCulture_T1",
            "Factor_Environment_SimpleAverage_N_T1": "AverageEnvironment_T1",
            "IGF_SimpleAverage_T2": "AverageIGF_T2",
            "Factor_Income_SimpleAverage_N_T2": "AverageIncome_T2",
            "Factor_Housing_SimpleAverage_N_T2": "AverageHousing_T2",
            "Factor_Schooling_SimpleAverage_Threshold_N_T2": "AverageSchooling_T2",
            "Factor_Health_SimpleAverage_N_T2": "AverageHealth_T2",
            "Factor_WomanAutonomy_SimpleAverage_N_T2": "AverageWomanAutonomy_T2",
            "Factor_Citizenship_SimpleAverage_N_T2": "AverageCitizenship_T2",
            "Factor_FirstInfancy_SimpleAverage_Threshold_N_T2": "AverageFirstInfancy_T2",
            "Factor_Culture_SimpleAverage_N_T2": "AverageCulture_T2",
            "Factor_Environment_SimpleAverage_N_T2": "AverageEnvironment_T2",
            "IGF_SimpleAverage_T3": "AverageIGF_T3",
            "Factor_Income_SimpleAverage_N_T3": "AverageIncome_T3",
            "Factor_Housing_SimpleAverage_N_T3": "AverageHousing_T3",
            "Factor_Schooling_SimpleAverage_Threshold_N_T3": "AverageSchooling_T3",
            "Factor_Health_SimpleAverage_N_T3": "AverageHealth_T3",
            "Factor_WomanAutonomy_SimpleAverage_N_T3": "AverageWomanAutonomy_T3",
            "Factor_Citizenship_SimpleAverage_N_T3": "AverageCitizenship_T3",
            "Factor_FirstInfancy_SimpleAverage_Threshold_N_T3": "AverageFirstInfancy_T3",
            "Factor_Culture_SimpleAverage_N_T3": "AverageCulture_T3",
            "Factor_Environment_SimpleAverage_N_T3": "AverageEnvironment_T3",
            # ----------------
            # ÍNDICE T0
            "M_DC_Walls_T0": "Walls_T0",
            "M_DC_Roof_T0": "Roof_T0",
            "M_DC_Floor_T0": "Floor_T0",
            # ----------------
            "M_DC_Water_T0": "Water_T0",
            "M_DC_WaterFrequency_T0": "WaterFrequency_T0",
            "M_DC_Eletricity_T0": "Eletricity_T0",
            "M_DC_Sewer_T0": "Sewer_T0",
            "M_DC_Bathroom_T0": "Bathroom_T0",
            "M_DC_BathroomQualit_T0": "BathroomQualit_T0",
            # MAIS MELHOR
            "CommFacilities_T0": "CommFacilities_T0",
            # ---
            # MENOS MELHOR
            "HousingProblems_T0": "HousingProblems_T0",
            # "HealthGenNames_T0": "HealthGenNames_T0",  # [TODO] Aqui também fazer visualização da incidência de cada doença no Tinicial e Tfinal
            # "HealthGenKidsNames_T0": "HealthGenKidsNames_T0",  # [TODO] Aqui também fazer visualização da incidência de cada doença no Tinicial e Tfinal
            # "Ref_GenIndex_param_T0": "HealthGenNAs_T0", # Indica onde tinha dado e onde era NA
            # "Ref_GenKidsIndex_param_T0": "HealthGenKidsNAs_T0", # Indica onde tinha dado e onde era 
            "Health_T0": "Health_T0",
            "HealthKids_T0": "HealthKids_T0",
            # ----------------
            "H_DC_FoodManytimes_T0": "FoodManytimes_T0",
            "Internet_T0": "Internet_T0",
            "Documents_T0": "Documents_T0",
            "C_DC_CEP_T0": "CEP_T0",
            "CH_E_DC_SchoolLiteracy_T0": "SchoolLiteracy_T0",
            "CH_E_DC_SchoolMathLit_T0": "SchoolMathLit_T0",
            "E_DI_SchoolLast_T0": "SchoolLast_T0",
            "E_DI_SchoolCurrent_T0": "SchoolCurrent_T0",  # [TODO] Juntar respostas Nao e Não
            "ES_DC_Access_T0": "Access_T0",
            "ES_DC_CulturalEvent_T0": "CulturalEvent_T0",
            "IB_HowManyPHHH_T0": "HowManyPHHH_T0",
            "R_DC_Income_T0": "Income_T0",
            "R_DI_IncomeWorkS3_T0": "IncomeWorkS3_T0",
            "R_DI_BankAccount_T0": "BankAccount_T0",
            "Garbage_T0": "Garbage_T0",
            "DI_IncomeDesc_T0": "IncomeDesc_T0",
            "IncomeDesc_T0": "IncomeDesc_T0",
            "DI_JobSatisfaction_T0": "JobSatisfaction_T0",
            "JobSatisfaction_T0": "JobSatisfaction_T0",
            "CH_IB_Race_T0": "Race_T0",
            "CH_IB_Gender_T0": "Gender_T0",
        }
        | dic_T1_3
    )

    return col_dict


@app.cell
def _(pl):
    def get_df_long(df):
        col_dict = get_col_dict()

        # profile_cols are columns that are only answered once and describe the profile of the family. They should become columns in the long format.
        profile_cols = {
            "Drogadicao": "Drogadicao",
            "Alcoolismo": "Alcoolismo",
            "Violencia_Mulher": "ViolenciaMulher",
            "Violencia_Criança": "ViolenciaCrianca",
            "CH_IB_Race_T0": "Race",
            "CH_IB_Gender_T0": "Gender",
        }

        race_categories = {
            "Parda": "Parda",
            "Preta": "Preta",
            "Branca": "Branca",
            "Indigena": "Indígena",
            "Amarela": "Amarela",
            "Amarela (Asiática)": "Amarela",
            "NA": "NA",
            "Não sabe/Não respondeu": "NA",
            "Não sabe": "NA",
        }
        gender_categories = {
            "Mulher cisgênero": "Mulher cis",
            "Homem cisgênero": "Homem cis",
            "Mulher transgênero": "Mulher trans",
            "Homem transgênero": "Homem trans",
            "Não binário": "Não binário",
            "NA": "NA",
            "Outro": "NA",
            "Prefiro não responder": "NA",
        }
        generic_categories = {
            "sim": "Sim",
            "nao": "Não",
            "NA": "NA",
            "talvez": "Sim",
            # "talvez": "Talvez",
        }

        df_long = (
            df.unpivot(
                index=["id_family_datalake", "FavelaID"]
                + list(profile_cols.keys()),
                variable_name="original_column",
                value_name="answer",
            )
            .join(  # Join with the column dictionary to get the new column names
                pl.DataFrame(
                    {
                        "original_column": list(col_dict.keys()),
                        "column": list(col_dict.values()),
                    }
                ),
                on="original_column",
                how="left",
            )
            # Joins with profile dataframes to get the profile information
            .join(
                pl.DataFrame(
                    {
                        "CH_IB_Race_T0": list(race_categories.keys()),
                        "race": list(race_categories.values()),
                    }
                ),
                on="CH_IB_Race_T0",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "CH_IB_Gender_T0": list(gender_categories.keys()),
                        "gender": list(gender_categories.values()),
                    }
                ),
                on="CH_IB_Gender_T0",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Drogadicao": list(generic_categories.keys()),
                        "drug_addiction": list(generic_categories.values()),
                    }
                ),
                on="Drogadicao",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Alcoolismo": list(generic_categories.keys()),
                        "alcoholism": list(generic_categories.values()),
                    }
                ),
                on="Alcoolismo",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Violencia_Mulher": list(generic_categories.keys()),
                        "violence_women": list(generic_categories.values()),
                    }
                ),
                on="Violencia_Mulher",
                how="left",
            )
            .join(
                pl.DataFrame(
                    {
                        "Violencia_Criança": list(generic_categories.keys()),
                        "violence_children": list(generic_categories.values()),
                    }
                ),
                on="Violencia_Criança",
                how="left",
            )
            # Extract time
            .with_columns(
                time=pl.col("column").str.extract(r"_T(\d)$", 1),
                question=pl.col("column").str.extract(r"(.*_)*(.*)_T(\d)$", 2),
            )
            # Padronizar o separados em questions com múltiplas asserções
            .with_columns(
                answer=pl.when(
                    (pl.col("question") == "HealthGenNames")
                    | (pl.col("question") == "HealthGenKidsNames")
                )
                .then(pl.col("answer").str.replace_all(",", ";"))
                .otherwise(pl.col("answer"))
            )
            .with_columns(
                FavelaID=pl.when(pl.col("FavelaID") == "Boca do Sapo (São Paulo)")
                .then(pl.lit("Favela dos Sonhos (São Paulo)"))
                .otherwise(pl.col("FavelaID"))
            )
            # .with_columns([pl.col(k).alias(v) for k, v in profile_cols.items()])
            .select(
                [
                    "id_family_datalake",
                    "FavelaID",
                    "time",
                    "question",
                    "answer",
                    "column",
                    "original_column",
                ]
                + [
                    "race",
                    "gender",
                    "drug_addiction",
                    "alcoholism",
                    "violence_women",
                    "violence_children",
                ]
            )
            .filter(
                pl.col("column").is_in(list(col_dict.values()))
            )  # Mantém apenas as colunas de interesse
        )

        # Separa as asserções no caso de haver múltiplas asserções na resposta
        df_long = pl.concat(
            [
                # [TODO] Verificar nas perguntas multi-asserções o que fazer com quem não respondeu nenhuma (ex.: "NA;NA;NA"). Algo como "Nenhuma opção". Importante para os gráficos em que vamos contar o número de asserções positivas ou negativas.
                df_long.filter((pl.col.answer != "NA"))
                .with_columns(answer=pl.col("answer").str.split(";"))
                .explode("answer")
                .filter(pl.col("answer") != "NA"),
                # Concatena com as respostas que são "NA;NA;...;NA" (nenhuma opção)
                df_long.filter(pl.col("answer").str.contains(r"^(NA;)+NA$")),
                # Concatena com as respostas que são "NA" (não respondidas)
                df_long.filter((pl.col.answer == "NA")),
            ]
        )

        df_long = df_long.with_columns(
            pl.when(pl.col("answer").str.contains(r"^(NA;)+NA$"))
            .then(pl.lit("Nenhuma opção"))
            .otherwise(pl.col("answer"))
            .alias("answer")
        )

        return df_long
    return (get_df_long,)


@app.cell
def _():
    ASSERTION_MAP = {
        "Gender": {"map": {"NA": ["Prefiro não responder", "Outro"]}},
        "Race": {
            "map": {
                "Indígena": ["Indigena"],
                "Amarela": ["Amarela (Asiática)"],
                "NA": ["Não sabe/Não respondeu", "Não sabe", "NA"],
            }
        },
        "Access": {
            "map": {
                "Existe algum membro que não tem acesso": [
                    "Sim, existe algum membro que não tem acesso"
                ],
                "Todos os membros tem acesso": [
                    "Não, todos os membros tem acesso",
                ],
            },
        },
        "SchoolCurrent": {
            "map": {
                "Não": [
                    "Nao",
                    "Não sabe",
                ],
            },
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
        },
        "DreamsKids": {
            "map": {
                "Não sei" : [
                    "(espontâneo) Não sei",
                    "Nao Sei",
                ],
                "Não": [
                    "Nao",
                    "Não",
                ],
                "Sim": [
                    "Sim o que?",
                    "Sim, o que?",
                ],
            },
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
        },
        "CulturalEvent": {
            "map": {},
        },
        "SchoolLiteracy": {
            "map": {
                "Não sei ler": [
                    "Não sei informar",
                ],
            },
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
        },
        "SchoolMathLit": {
            "map": {
                "Não sei fazer contas matemáticas": [
                    "Não sei informar",
                ],
            },
        },
        "WaterFrequency": {
            "map": {
                "Não tenho": [
                    "Nao Tenho",
                ],
            },
        },
        "Bathroom": {
            "map": {
                "Não tem": [
                    "Nao Tem",
                    "Outro",
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
        },
        "Documents": {
            "map": {
                "Outro": [
                    "1",
                    "0",
                ],
                "Registro Nacional de Estrangeiro (RNE)": [
                    "Permisso de entrada, Autorização de residência, Protocolo de situação de Refugio, RNE, RME, Refugiado",
                    "RNE",
                ],
                "Registro indígena": [
                    "Registro Administrativo de Nascimento Indígena",
                ],
                "RG": [
                    "Carteira de Identidade",
                ],
                "Cartão SUS": [
                    "Cartão SUS",
                    "SUS",
                ],
                "Certidão de nascimento": ["Certidão de Nascimento"],
                "Não tenho nenhum documento": [
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ],
            },
        },
        "HealthKids": {"map": {"Diarréia crônica": ["Diarréia Crônica"]}},
        "Health": {"map": {"Diarréia crônica": ["Diarréia Crônica"]}},
        "Internet": {
            "subtitle": "Todas as respostas",
            "map": {
                "Não tenho acesso à internet": [
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ]
            },
        },
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
                    "Coleta de Lixo",  # É isso mesmo???
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
        },
        "BathroomQualit": {
            "map": {
                "Outro": [
                    "Só a metade da parede.",
                    "Não tem chuveiro",
                    "Em obra",
                    "A casa não possui chuveiro",
                    "Azulejo todo ruim",
                    "ardosia piso",
                    "Está em obra",
                    "Banheiro de madeira",
                    "Azulejo até a metade do banheiro",
                    "Piso e parede de cimento",
                    "a privada não havia tampa. Moradores que tiveram a iniciativa e compraram, mas a imobiliária já ressarciu eles.",
                    "0",
                    "Tem pia.",
                ]
                # "map": {
                #     "Nenhuma opção": [
                #         "Nenhuma opção",
                #         "0",
                #     ],
                #     "Parede de azulejo": [
                #         "Azulejo até a metade do banheiro",
                #         "Azulejo todo ruim",
                #     ],
                #     "Outros": [
                #         "A casa não possui chuveiro",
                #         "Está em obra",
                #         "Não tem chuveiro",
                #         "Banheiro de madeira",
                #         "Em obra",
                #         "ardosia piso",
                #         "Outro",
                #         "a privada não havia tampa. Moradores que tiveram a iniciativa e compraram, mas a imobiliária já ressarciu eles.",
                #         "Só a metade da parede.",
                #         "Piso e parede de cimento",
                #         "Tem pia.",
                #     ],
            },
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
                "Lixeira na favela": [
                    "Cacamba",
                    "Caçamba mais proxima",
                    "Caçamba MB ais próxima",
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
                "Lixeira fora da favela": [
                    "Cacamba mais próxima da favela",
                    "Caçamba mais Px a favela",
                    "Lixeira",
                    "Caçamba mais próxima da favela",
                    "Caçamba mais próxima",
                ],
                "Coleta seletiva": [
                    "Coleta Seletiva",
                ],
                "Outro": [
                    "Nenhuma opção",  # Respostas do tipo "NA;NA;NA;...;NA;NA;NA"
                ],
            },
        },
    }
    return (ASSERTION_MAP,)


if __name__ == "__main__":
    app.run()

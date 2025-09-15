import marimo

__generated_with = "0.15.2"
app = marimo.App(width="columns")


@app.cell
def _(get_df_long, mo, pl):
    # Leitura do df original em CSV
    base_1 = str(
        mo.notebook_location()
        / "public"
        / "Carol_DataBaseFull_21052025_anonimizado.csv"
    )
    base_2 = str(
        mo.notebook_location() / "public" / "Carol_DataBaseFull_Limpa.csv"
    )
    df_original = pl.read_csv(base_1)
    # Join com a versão anterior dos dados por conta de colunas que desapareceram na nova versão
    df_old = pl.read_csv(base_2)
    df_original = df_original.join(df_old, on=["id_family_datalake"])

    # Passa o dataframe para o formato long (uma row por resposta, ao invés de uma row por família)
    df_long = get_df_long(df_original)
    df_long.write_csv(str(mo.notebook_location() / "public" / "df_long.csv"))
    return


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
        "HealthGenNames",
        "HealthGenKidsNames",
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
    ]
    complementary_vars = {}
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
            # ----------------
            # O QUE USAR AQUI? -> Vamos consolidar BathroomQualit em cada tempo de uma forma que a Carol vai ver [NOTE]
            # [NOTE] Variáveis que vamos consolidar contando o número de asserções de um subconjunto
            # [TODO] Fazer para
            # ---
            # "M_DC_BathroomQualit_1_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_2_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_3_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_4_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_5_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_6_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_7_T0": "BathroomQualit_T0",
            # "M_DC_BathroomQualit_8_T0": "BathroomQualit_T0",
            # ---
            # MAIS MELHOR
            "CommFacilities_T0": "CommFacilities_T0",
            # ---
            # MENOS MELHOR
            "HousingProblems_T0": "HousingProblems_T0",
            "HealthGenNames_T0": "HealthGenNames_T0",  # [TODO] Aqui também fazer visualização da incidência de cada doença no Tinicial e Tfinal
            "HealthGenKidsNames_T0": "HealthGenKidsNames_T0",  # [TODO] Aqui também fazer visualização da incidência de cada doença no Tinicial e Tfinal
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


if __name__ == "__main__":
    app.run()

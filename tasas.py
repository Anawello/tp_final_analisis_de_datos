import pandas as pd
from utils import ANIOS
from graficos import grafico_de_lineas

# ---------------------------------------------------------------------------- #
#                                     Tasas                                    #
# ---------------------------------------------------------------------------- #
def calcular_tasas_laborales(df_muestra: pd.DataFrame) -> pd.DataFrame:
    resultados  = []

    for anio in ANIOS:
        df_anual = df_muestra[df_muestra["ANO4"] ==  str(anio)]

        if df_anual.size == 0:
            continue

        for trimestre in range(4):
            df_trimestral = df_anual[df_anual["TRIMESTRE"] == str(trimestre + 1)]

            if df_trimestral.size == 0:
                continue

            # asegurar si no hay columnas
            if "ESTADO" not in df_trimestral.columns or "PONDERA" not in df_trimestral.columns:
                continue

            # transformar datos a numericos
            df_trimestral["ESTADO"] = pd.to_numeric(df_trimestral["ESTADO"], errors="coerce")
            df_trimestral["PONDERA"] = pd.to_numeric(df_trimestral["PONDERA"], errors="coerce")

            # filtrar población mayor a 10 anios
            df_trimestral = df_trimestral[df_trimestral["CH06"].astype(float) >= 10]

            # ponderacion (darle un peso a cada dato)
            ocupados    = df_trimestral[df_trimestral["ESTADO"] == 1]["PONDERA"].sum()
            desocupados = df_trimestral[df_trimestral["ESTADO"] == 2]["PONDERA"].sum()
            inactivos   = df_trimestral[df_trimestral["ESTADO"] == 3]["PONDERA"].sum()

            # calculo de tasas
            pea = ocupados + desocupados
            poblacion_total = pea + inactivos

            resultados.append({
                "anio": anio,
                "trimestre": trimestre + 1,
                "actividad": (pea / poblacion_total) * 100,
                "empleo":    (ocupados / poblacion_total) * 100,
                "desocupacion": (desocupados / pea) * 100
            })

    return pd.DataFrame(resultados)

# -------------------------------- Graficos de tasas generales -------------------------------- #
def graficar_tasas_aglomerado(aglomerado: str, df: pd.DataFrame):
    display_aglomerado = aglomerado.replace("_", " ")
    if display_aglomerado == "gba":
        display_aglomerado = display_aglomerado.upper()
    else:
        display_aglomerado = display_aglomerado.title()

    grafico_de_lineas(df, f"Evolución anual de las tasas de Actividad, Empleo y Desocupación de {display_aglomerado} (2016-2025)", "Años", "Tasa (%)", "anio", [
        {"valor":"actividad", "label":"actividad"},
        {"valor":"empleo", "label":"empleo"},
        {"valor":"desocupacion", "label":"desocupacion"},
    ])

def graficar_tasas_comparativa(tasa: str, df_gran_mendoza: pd.DataFrame, df_gba: pd.DataFrame):

    df_gran_mendoza = df_gran_mendoza[["anio", tasa]].rename(columns={tasa: "gran_mendoza"})
    df_gba = df_gba[["anio", tasa]].rename(columns={tasa: "gba"})

    df = df_gran_mendoza.merge(df_gba, on="anio")

    grafico_de_lineas(df, f"Evolución comparativa anual de la tasa de {tasa.title()} entre GBA y Gran Mendoza (2016-2025)", "Años", f"Tasa de {tasa.title()} (%)", "anio", [
        {"valor":"gran_mendoza", "label":"Mendoza"},
        {"valor":"gba", "label":"GBA"},
    ])

def graficar_tasas(df_gran_mendoza: pd.DataFrame, df_gba: pd.DataFrame) :
    df_tasas_gran_mendoza = calcular_tasas_laborales(df_gran_mendoza)
    df_tasas_gba = calcular_tasas_laborales(df_gba)

    graficar_tasas_comparativa("desocupacion", df_tasas_gran_mendoza, df_tasas_gba)
    graficar_tasas_comparativa("actividad", df_tasas_gran_mendoza, df_tasas_gba)
    graficar_tasas_comparativa("empleo", df_tasas_gran_mendoza, df_tasas_gba)

    graficar_tasas_aglomerado("gran_mendoza", df_tasas_gran_mendoza )
    graficar_tasas_aglomerado("gba", df_tasas_gba) 


# -------------------------------- Graficos de tasas segun el sexo -------------------------------- #
def graficar_tasa_segun_sexo(df_tasas_hombre: pd.DataFrame, df_tasas_mujer: pd.DataFrame, aglomerado: str, tasa: str):
    # uno las tasas segun su categorias (hombre | mujer)
    df_tasa_comparativa_sexo = pd.DataFrame();
    df_tasa_comparativa_sexo["anio"] = df_tasas_hombre["anio"]
    df_tasa_comparativa_sexo["trimestre"] = df_tasas_hombre["trimestre"]
    df_tasa_comparativa_sexo["hombre"] = df_tasas_hombre[tasa]
    df_tasa_comparativa_sexo["mujer"] = df_tasas_mujer[tasa]

    # decoro como se mostrara el aglomerado en el grafico
    display_aglomerado = aglomerado.replace("_", " ")
    if display_aglomerado == "gba":
        display_aglomerado = display_aglomerado.upper()
    else:
        display_aglomerado = display_aglomerado.title()

    grafico_de_lineas(
        df_tasa_comparativa_sexo, 
        f"Evolución comparativa de la tasa de {tasa.title()} segun sexo en {display_aglomerado} (2016-2025)", 
        "Años", 
        f"Tasa de {tasa}(%)", 
        "anio",
        [
            {"valor":"mujer", "label":"Mujer"},
            {"valor":"hombre", "label":"Hombre"}
        ]
    )

def graficar_tasas_segun_sexo(df_muestra: pd.DataFrame, aglomerado: str):
    # divido la muestra entre hombres y mujeres (aqui deben hacer la magia)
    df_hombre = df_muestra[df_muestra["CH04"] == "1"]
    df_mujer = df_muestra[df_muestra["CH04"] == "2"]

    # por cada categoria obtengo sus tasas
    df_tasas_hombre = calcular_tasas_laborales(df_hombre)
    df_tasas_mujer = calcular_tasas_laborales(df_mujer)

    # muestro los graficos para cada tipo de tasas (en cada uno se compara entre hombre y mujer)
    graficar_tasa_segun_sexo(df_tasas_hombre, df_tasas_mujer, aglomerado, "actividad")
    graficar_tasa_segun_sexo(df_tasas_hombre, df_tasas_mujer, aglomerado, "empleo")
    graficar_tasa_segun_sexo(df_tasas_hombre, df_tasas_mujer, aglomerado, "desocupacion")


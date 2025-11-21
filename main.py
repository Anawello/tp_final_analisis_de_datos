import geopandas as gpd
import pandas as pd
from enum import Enum
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
from typing import TypedDict, List

# ------------------------ enumeradores y diccionarios ----------------------- #
class CategoriasEPH(Enum):
    HOGAR = "hogar"
    INVIVIDUAL = "individual"

class DatoGrafico(TypedDict):
    valor: str
    label: str

# -------------------------------- constantes -------------------------------- #
AGLOMERADO = {"gran mendoza":"10", "gba":"33"}
AÑOS = [ 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

# ---------------------------- datos geospaciales ---------------------------- #
# TODO: hacer que se muestre en pantalla en un mapa segun un valor dado
# datos geoespaciales (prueba para obtener los datos del aglomerado de Mendoza)
# mapas_aglomerados = gpd.read_file("aglomerados_eph.json")
# print(mapas_aglomerados[mapas_aglomerados["eph_codagl"] == AGLOMERADO])

def init():
    # precargar datos si aun no lo estan
    if not os.path.exists("periodos/parquet"):
        print("Microdatos no precargados.")
        precargar_microdatos_eph("gran mendoza");
        precargar_microdatos_eph("gba");

def precargar_microdatos_eph(aglomerado: str):
    # precargo los datos en bruto y los transformo al formato Parquet (mas rapido)

    print("Iniciando precarga de datos.")

    # creo directorios
    os.makedirs(f"periodos/parquet/{aglomerado}/hogar", exist_ok=True)
    os.makedirs(f"periodos/parquet/{aglomerado}/individual", exist_ok=True)

    for año in AÑOS:
        print(f"\n--{año}--")

        for t in range(4):
            try:
                # obtengo df de cada trimestre (hogar e individual)
                df_hogar = pd.read_csv(f"periodos/raw/hogar/usu_hogar_T{t + 1}{año - 2000}.txt", sep=";", encoding="latin1", dtype=str, low_memory=False)
                df_individual = pd.read_csv(f"periodos/raw/individual/usu_individual_T{t + 1}{año - 2000}.txt", sep=";", encoding="latin1", dtype=str, low_memory=False)

                # filtro segun aglomerado
                df_hogar_filtrado = df_hogar.loc[df_hogar.AGLOMERADO == AGLOMERADO[aglomerado]]
                df_individual_filtrado = df_individual.loc[df_individual.AGLOMERADO == AGLOMERADO[aglomerado]]

                # almaceno df en formato parquet
                df_hogar_filtrado.to_parquet(f"periodos/parquet/{aglomerado}/hogar/usu_hogar_T{t + 1}{año - 2000}.parquet", index=False)
                df_individual_filtrado.to_parquet(f"periodos/parquet/{aglomerado}/individual/usu_individual_T{t + 1}{año - 2000}.parquet", index=False)

                print(f"✅ Microdatos del trimestre {t + 1} del año {año} agregado.")
            except FileNotFoundError:
                print(f"❌ Microdatos del trimestre {t + 1} del año {año} no encontrado.")
        

    print("\nDatos EPH precargados exitosamente!")

def obtener_datos(aglomerado: str, año: int, categoria: CategoriasEPH, trimestre: int = 0) -> pd.DataFrame:
    """Obtener Datos Del Año / Trimestre

    Args:
        año (int): año del dato (2016 - 2025)
        categoria (CategoriasEPH): categoria de la encuesta EPH (HOGAR | INDIVIDUAL)
        trimestre (int): 0 -> (default) obtiene todos los datos del año | 1,2,3,4 -> obtiene datos del trimestre

    Returns:
        pd.DataFrame: DataFrame con los datos del año o del trimestre elegido
    """
    
    # validaciones
    if(año < 2016 or año > 2025): return print("Año fuera de rango.")
    if(trimestre < 0 or trimestre > 4): return print("Trimestre fuera de rango.")

    try:
        df: pd.DataFrame

        if trimestre == 0:
            df = pd.DataFrame();

            for t in range(4):
                df_trimestral = obtener_datos(aglomerado, año, CategoriasEPH.INVIVIDUAL, t + 1)

                # si se obtuvieron los datos concateno el df con los trimestres anteriores
                if isinstance(df_trimestral, pd.DataFrame):
                    df = pd.concat([df, df_trimestral])
        else:
            df = pd.read_parquet(f"periodos/parquet/{aglomerado}/{categoria.value}/usu_{categoria.value}_T{trimestre}{año - 2000}.parquet")

        return df
    except FileNotFoundError:
        print(f"❌ Microdatos del trimestre {trimestre} del año {año} no encontrado.")
        return None

def grafico_de_lineas(df: pd.DataFrame, titulo: str, label_x: str, label_y: str, valor_ref: str, data: List[DatoGrafico]):
    """
    Generar Grafico de Lineas
    
    Args:
        df (pd.DataFrame): DataFrame con los datos a mostrar
        titulo (str): Titulo del grafico
        label_x (str): encabezado de los valores x
        label_y (str): encabezado de los valores y
        valor_ref (str): Nombre del valor x del DataFrame
        data (List[DatoGrafico]): Lista con los datos de cada linea ej: [{"valor": "media_anual", "label": "media"}, {"valor": "mediana_anual", ...}]
    """

    # tema y forma
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 5))

    # asigno tipo de grafico y datos por fila
    for d in data:
        sns.lineplot(data=df, x=df[valor_ref], y=df[d["valor"]], label=d["label"], marker="o", linewidth=2.5, markersize=8)

    # titulos y encabezados
    plt.title(titulo)
    plt.xlabel(label_x)
    plt.ylabel(label_y)

    plt.xticks(df[valor_ref]) # fuerzo a mostrar todos los años
    plt.tight_layout()
    plt.show()

def calcular_tasas_laborales(aglomerado: str):
    # -----------------------------
    # 1. Cargar todos los años juntos
    # -----------------------------
    dfs = []
    for anio in AÑOS:
        df = obtener_datos(aglomerado, anio, CategoriasEPH.INVIVIDUAL)
        if df is None:
            continue
        df = df.copy()
        df["anio"] = int(anio)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # -----------------------------
    # 2. Limpieza y preparación
    # -----------------------------
    df["ESTADO"] = pd.to_numeric(df["ESTADO"], errors="coerce")
    df["PONDERA"] = pd.to_numeric(df["PONDERA"], errors="coerce")
    df["CH06"] = pd.to_numeric(df["CH06"], errors="coerce")

    # Quitar filas inválidas
    df = df.dropna(subset=["ESTADO", "PONDERA", "CH06"])

    # Población de 10+ años (estándar EPH)
    df = df[df["CH06"] >= 10]

    # Solo valores válidos de ESTADO
    df = df[df["ESTADO"].isin([1, 2, 3])]

    # -----------------------------
    # 3. Vectorización total
    #    Pivot table = sumatoria por ESTADO y por año
    # -----------------------------
    tabla = df.pivot_table(
        values="PONDERA",
        index="anio",
        columns="ESTADO",
        aggfunc="sum",
        fill_value=0
    ).rename(columns={
        1: "ocupados",
        2: "desocupados",
        3: "inactivos"
    })

    # -----------------------------
    # 4. Cálculo de tasas
    # -----------------------------
    tabla["pea"] = tabla["ocupados"] + tabla["desocupados"]
    tabla["poblacion_total"] = tabla["pea"] + tabla["inactivos"]

    tabla["actividad"] = (tabla["pea"] / tabla["poblacion_total"]) * 100
    tabla["empleo"] = (tabla["ocupados"] / tabla["poblacion_total"]) * 100
    tabla["desocupacion"] = (tabla["desocupados"] / tabla["pea"]) * 100

    # Reset index para que quede como DF normal
    resultado = tabla.reset_index()[[
        "anio", "actividad", "empleo", "desocupacion"
    ]]

    return resultado

"""
def calcular_tasas_laborales(aglomerado: str):
    resultados = []

    for anio in AÑOS:
        df = obtener_datos(aglomerado, anio, CategoriasEPH.INVIVIDUAL)
        if df is None:
            continue

        # Asegurar que existan las columnas necesarias
        if "ESTADO" not in df.columns or "PONDERA" not in df.columns:
            print(f"Columnas faltantes en {anio}")
            continue

        df["ESTADO"] = pd.to_numeric(df["ESTADO"], errors="coerce")
        df["PONDERA"] = pd.to_numeric(df["PONDERA"], errors="coerce")

        # Filtrar población de 10 años o más (EPH estándar)
        df = df[df["CH06"].astype(float) >= 10]

        # Grupos principales
        ocupados       = df[df["ESTADO"] == 1]["PONDERA"].sum()
        desocupados    = df[df["ESTADO"] == 2]["PONDERA"].sum()
        inactivos      = df[df["ESTADO"] == 3]["PONDERA"].sum()

        pea = ocupados + desocupados
        poblacion_total = pea + inactivos

        if poblacion_total == 0 or pea == 0:
            continue

        tasa_actividad = (pea / poblacion_total) * 100
        tasa_empleo    = (ocupados / poblacion_total) * 100
        tasa_desoc     = (desocupados / pea) * 100

        resultados.append({
            "anio": anio,
            "actividad": tasa_actividad,
            "empleo": tasa_empleo,
            "desocupacion": tasa_desoc
        })

    return pd.DataFrame(resultados)
"""

init()

def graficar_tasas_aglomerado(aglomerado: str, df: pd.DataFrame):

    grafico_de_lineas(df, f"Evolución anual de las tasas de Actividad, Empleo y Desocupación de {aglomerado.title()} (2016-2025)", "Años", "Tasa (%)", "anio", [
        {"valor":"actividad", "label":"actividad"},
        {"valor":"empleo", "label":"empleo"},
        {"valor":"desocupacion", "label":"desocupacion"},
    ])

def graficar_tasa_comparativa(tasa: str, df_gran_mendoza: pd.DataFrame, df_gba):

    df_gran_mendoza = tasas_gran_mendoza[["anio", tasa]].rename(columns={tasa: "gran mendoza"})
    df_gba = tasas_gba[["anio", tasa]].rename(columns={tasa: "gba"})

    df = df_gran_mendoza.merge(df_gba, on="anio")

    grafico_de_lineas(df, f"Evolución comparativa anual de la tasa de {tasa.title()} entre GBA y Gran Mendoza (2016-2025)", "Años", f"Tasa de {tasa.title()} (%)", "anio", [
        {"valor":"gran mendoza", "label":"Mendoza"},
        {"valor":"gba", "label":"GBA"},
    ])


tasas_gran_mendoza = calcular_tasas_laborales("gran mendoza")
tasas_gba = calcular_tasas_laborales("gba")

graficar_tasa_comparativa("desocupacion", tasas_gran_mendoza, tasas_gba)
graficar_tasa_comparativa("actividad", tasas_gran_mendoza, tasas_gba)
graficar_tasa_comparativa("empleo", tasas_gran_mendoza, tasas_gba)

graficar_tasas_aglomerado("gran mendoza", tasas_gran_mendoza )
graficar_tasas_aglomerado("gba", tasas_gba)
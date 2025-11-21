import geopandas as gpd
import pandas as pd
from enum import Enum
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
from typing import TypedDict, List
from ipc_mensual import inflacion_mensual

# ------------------------ enumeradores y diccionarios ----------------------- #
class CategoriasEPH(Enum):
    HOGAR = "hogar"
    INVIVIDUAL = "individual"

class DatoGrafico(TypedDict):
    valor: str
    label: str

# -------------------------------- constantes -------------------------------- #
AGLOMERADO = {"gran_mendoza":"10", "gba":"33"}
AÑOS = [ 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
INFLACION_MENSUAL = pd.DataFrame(inflacion_mensual)

# ---------------------------- datos geospaciales ---------------------------- #
# TODO: hacer que se muestre en pantalla en un mapa segun un valor dado
# datos geoespaciales (prueba para obtener los datos del aglomerado de Mendoza)
# mapas_aglomerados = gpd.read_file("aglomerados_eph.json")
# print(mapas_aglomerados[mapas_aglomerados["eph_codagl"] == AGLOMERADO])

def init():
    # precargar datos si aun no lo estan
    if not os.path.exists("periodos/parquet"):
        print("Microdatos no precargados.")
        precargar_microdatos_eph("gran_mendoza");
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
    resultados  = []

    for anio in AÑOS:
        df = obtener_datos(aglomerado, anio, CategoriasEPH.INVIVIDUAL)
        if df is None:
            continue

        # asegurar si no hay columnas
        if "ESTADO" not in df.columns or "PONDERA" not in df.columns:
            continue

        # transformar datos a numericos
        df["ESTADO"] = pd.to_numeric(df["ESTADO"], errors="coerce")
        df["PONDERA"] = pd.to_numeric(df["PONDERA"], errors="coerce")

        # filtrar población mayor a 10 años
        df = df[df["CH06"].astype(float) >= 10]

        # ponderacion (darle un peso a cada dato)
        ocupados    = df[df["ESTADO"] == 1]["PONDERA"].sum()
        desocupados = df[df["ESTADO"] == 2]["PONDERA"].sum()
        inactivos   = df[df["ESTADO"] == 3]["PONDERA"].sum()

        # calculo de tasas
        pea = ocupados + desocupados
        poblacion_total = pea + inactivos

        resultados.append({
            "anio": anio,
            "actividad": (pea / poblacion_total) * 100,
            "empleo":    (ocupados / poblacion_total) * 100,
            "desocupacion": (desocupados / pea) * 100
        })

    return pd.DataFrame(resultados)

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

def graficar_tasa_comparativa(tasa: str, df_gran_mendoza: pd.DataFrame, df_gba: pd.DataFrame):

    df_gran_mendoza = df_gran_mendoza[["anio", tasa]].rename(columns={tasa: "gran_mendoza"})
    df_gba = df_gba[["anio", tasa]].rename(columns={tasa: "gba"})

    df = df_gran_mendoza.merge(df_gba, on="anio")

    grafico_de_lineas(df, f"Evolución comparativa anual de la tasa de {tasa.title()} entre GBA y Gran Mendoza (2016-2025)", "Años", f"Tasa de {tasa.title()} (%)", "anio", [
        {"valor":"gran_mendoza", "label":"Mendoza"},
        {"valor":"gba", "label":"GBA"},
    ])


def generar_graficos_tasas():
    tasas_gran_mendoza = calcular_tasas_laborales("gran_mendoza")
    tasas_gba = calcular_tasas_laborales("gba")

    graficar_tasa_comparativa("desocupacion", tasas_gran_mendoza, tasas_gba)
    graficar_tasa_comparativa("actividad", tasas_gran_mendoza, tasas_gba)
    graficar_tasa_comparativa("empleo", tasas_gran_mendoza, tasas_gba)

    graficar_tasas_aglomerado("gran_mendoza", tasas_gran_mendoza )
    graficar_tasas_aglomerado("gba", tasas_gba) 

def obtener_ipc_trimestral(): 
    ipc_trimestrales = []
    trimestre = 1

    for i in range(INFLACION_MENSUAL.size // 4):
        try:
            ipc_trimestral = 1

            for j in range(3):
                ipc_trimestral *= (INFLACION_MENSUAL.loc[i*3 + j, "ipc"]) / 100 + 1

            ipc_trimestral -= 1
            ipc_trimestrales.append({
                "año":int(INFLACION_MENSUAL.loc[i*3, "año"]), 
                "trimestre": trimestre,                     
                "ipc": float(ipc_trimestral)
            })

            if trimestre < 4:
                trimestre += 1
            else:
                trimestre = 1
        except:
            continue

    return pd.DataFrame(ipc_trimestrales)

def obtener_ipc_trimestral_acumulada():
    ipc_trimestrales = obtener_ipc_trimestral()
    # seteo punto de comparacion
    ipc_trimestrales.loc[0, "ipc"] = 0 

    df = pd.DataFrame()
    df["año"] = ipc_trimestrales["año"]
    df["trimestre"] = ipc_trimestrales["trimestre"]
    df["ipc_acumulado"] = (1 + ipc_trimestrales["ipc"]).cumprod()

    return df

def obtener_ingreso_medio(aglomerado: str):
    medias_trimestrales = []
    for año in AÑOS:

        for i in range(4):
            df = obtener_datos(aglomerado, año, CategoriasEPH.INVIVIDUAL, i+1)
            if isinstance(df, pd.DataFrame):
                ingreso_total_individual = pd.to_numeric(df["P47T"], errors="coerce")
                pondera = pd.to_numeric(df["PONDERA"], errors="coerce")
                media_ponderada = (ingreso_total_individual * pondera).sum() / pondera.sum()

                medias_trimestrales.append({"año":año, "trimestre": i + 1,"ingreso_media_ponderada": media_ponderada})

    return pd.DataFrame(medias_trimestrales);


def obtener_ingreso_real(aglomerado: str) -> pd.DataFrame:
    df_ipc_trimestral_acumulada = obtener_ipc_trimestral_acumulada()
    df_ingreso_medio_aglomerado = obtener_ingreso_medio(aglomerado)

    df = df_ipc_trimestral_acumulada.merge(df_ingreso_medio_aglomerado, on=["año", "trimestre"])
    df["ingreso_media_real"] = df["ingreso_media_ponderada"] / df["ipc_acumulado"]

    return df

df = obtener_ingreso_real("gran_mendoza")
print(df.head())
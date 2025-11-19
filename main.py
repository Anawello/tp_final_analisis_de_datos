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
AGLOMERADO = "10" # Mendoza
AÑOS = [2016, 2017,2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

# ---------------------------- datos geospaciales ---------------------------- #
# TODO: hacer que se muestre en pantalla en un mapa segun un valor dado
# datos geoespaciales (prueba para obtener los datos del aglomerado de Mendoza)
# mapas_aglomerados = gpd.read_file("aglomerados_eph.json")
# print(mapas_aglomerados[mapas_aglomerados["eph_codagl"] == AGLOMERADO])

def init():
    # precargar datos si aun no lo estan
    if not os.path.exists("periodos/parquet"):
        print("Microdatos no precargados.")
        precargar_microdatos_eph();

def precargar_microdatos_eph():
    # precargo los datos en bruto y los transformo al formato Parquet (mas rapido)

    print("Iniciando precarga de datos.")

    # creo directorios
    os.makedirs("periodos/parquet/hogar", exist_ok=True)
    os.makedirs("periodos/parquet/individual", exist_ok=True)

    for año in AÑOS:
        print(f"\n--{año}--")

        for t in range(4):
            try:
                # obtengo df de cada trimestre (hogar e individual)
                df_hogar = pd.read_csv(f"periodos/raw/hogar/usu_hogar_T{t + 1}{año - 2000}.txt", sep=";", encoding="latin1", dtype=str, low_memory=False)
                df_individual = pd.read_csv(f"periodos/raw/individual/usu_individual_T{t + 1}{año - 2000}.txt", sep=";", encoding="latin1", dtype=str, low_memory=False)

                # filtro segun aglomerado
                df_hogar_filtrado = df_hogar.loc[df_hogar.AGLOMERADO == AGLOMERADO]
                df_individual_filtrado = df_individual.loc[df_individual.AGLOMERADO == AGLOMERADO]

                # almaceno df en formato parquet
                df_hogar_filtrado.to_parquet(f"periodos/parquet/hogar/usu_hogar_T{t + 1}{año - 2000}.parquet", index=False)
                df_individual_filtrado.to_parquet(f"periodos/parquet/individual/usu_individual_T{t + 1}{año - 2000}.parquet", index=False)

                print(f"✅ Microdatos del trimestre {t + 1} del año {año} agregado.")
            except FileNotFoundError:
                print(f"❌ Microdatos del trimestre {t + 1} del año {año} no encontrado.")
        

    print("\nDatos EPH precargados exitosamente!")

def obtener_datos(año: int, categoria: CategoriasEPH, trimestre: int = 0) -> pd.DataFrame:
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
                df_trimestral = obtener_datos(año, CategoriasEPH.INVIVIDUAL, t + 1)

                # si se obtuvieron los datos concateno el df con los trimestres anteriores
                if isinstance(df_trimestral, pd.DataFrame):
                    df = pd.concat([df, df_trimestral])
        else:
            df = pd.read_parquet(f"periodos/parquet/{categoria.value}/usu_{categoria.value}_T{trimestre}{año - 2000}.parquet")

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

def mostrar_datos() :
    # ---------------- crear dataframe con medias y mediadas anuales del salario individual en bruto --------------- #
    medias_anuales = [] # array de datos

    # obtengo los datos del salario por año(sacando nulos)
    for año in AÑOS:
        datos_anuales = obtener_datos(año, CategoriasEPH.INVIVIDUAL)
        salarios_anual = pd.to_numeric(datos_anuales["PP08D1"]); # transformo a numerico para sacar media y mediana
        
        medias_anuales.append({
            "año" : año, 
            "media" : salarios_anual.mean(), 
            "mediana" : salarios_anual.median(),
        })

    # transformo el array de datos en un DataFrame
    df_medias_anuales = pd.DataFrame(medias_anuales);

    # ------------------------- mostrar grafico de lineas ------------------------ #
    grafico_de_lineas(
        df_medias_anuales, 
        "Evolución del salario", 
        "Años", 
        "Sueldos en pesos", 
        "año", 
        [
            {"valor": "media", "label" : "media"},
            {"valor": "mediana", "label" : "mediana"}
        ]
    )

init()
mostrar_datos()
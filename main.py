import geopandas as gpd
import pandas as pd
from enum import Enum
import numpy as np
import os
from utils import ANIOS
from typing import TypedDict, List
from ingresos import graficar_ingreso_real, graficar_ingreso_real_por_sexo
from tasas import graficar_tasas_segun_sexo, graficar_tasas
from modelado import imputar_ingresos
from graficos import graficar_distribucion_ingreso
from geo import graficar_gba_con_ingresos, graficar_aglomerado_con_ingresos

# ------------------------ enumeradores y diccionarios ----------------------- #
class CategoriasEPH(Enum):
    HOGAR = "hogar"
    INVIVIDUAL = "individual"


# -------------------------------- constantes -------------------------------- #
AGLOMERADO = {"gran_mendoza":"10", "gba":"33"}

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

    for año in ANIOS:
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

def obtener_datos_aual_trimestral(aglomerado: str, año: int, trimestre: int):
    if(año < 2016 or año > 2025): return print("Año fuera de rango.")
    if(trimestre < 0 or trimestre > 4): return print("Trimestre fuera de rango.")

    try:    
        df_trimestral = pd.read_parquet(f"periodos/parquet/{aglomerado}/{CategoriasEPH.INVIVIDUAL.value}/usu_{CategoriasEPH.INVIVIDUAL.value}_T{trimestre}{año - 2000}.parquet")

        return df_trimestral
    except FileNotFoundError:
        print(f"❌ Microdatos del trimestre {trimestre} del año {año} no encontrado.")
                
        return None
    
def obtener_datos_anual(aglomerado: str, año: int):
    if(año < 2016 or año > 2025): return print("Año fuera de rango.")

    df = pd.DataFrame();

    for t in range(4):
        df_trimestral = obtener_datos_aual_trimestral(aglomerado, año, t + 1)

        # si se obtuvieron los datos concateno el df con los trimestres anteriores
        if isinstance(df_trimestral, pd.DataFrame):
            df = pd.concat([df, df_trimestral])
        
    return df

def obtener_datos(aglomerado: str) -> pd.DataFrame:
    df_total = pd.DataFrame()

    for año in ANIOS:
        df_anual = obtener_datos_anual(aglomerado, año)
        df_total = pd.concat([df_total, df_anual])
        
    return df_total

init()
# obtengo datos de cada aglomerado
df_gba = obtener_datos("gba")
df_gran_mendoza = obtener_datos("gran_mendoza")

# muestro graficos 

graficar_tasas(df_gran_mendoza, df_gba)
graficar_tasas_segun_sexo(df_gba, "gba")
graficar_tasas_segun_sexo(df_gran_mendoza, "gran_mendoza")
graficar_ingreso_real(df_gran_mendoza, df_gba)
graficar_ingreso_real_por_sexo(df_gba, "gba")
graficar_ingreso_real_por_sexo(df_gran_mendoza, "gran_mendoza")

df_gba_2_2025 = obtener_datos_aual_trimestral("gba", 2025, 2)
df_gran_mendoza_2_2025 = obtener_datos_aual_trimestral("gran_mendoza", 2025, 2)

if (isinstance(df_gba, pd.DataFrame)):
    df_gba_imputado = imputar_ingresos(df_gba_2_2025, "GBA")
    graficar_distribucion_ingreso(df_gba_2_2025, df_gba_imputado, "GBA")

    df_gran_mendoza_inputado = imputar_ingresos(df_gran_mendoza_2_2025, "Gran Mendoza")
    graficar_distribucion_ingreso(df_gba_2_2025, df_gran_mendoza_inputado, "Gran Mendoza")

graficar_aglomerado_con_ingresos(df_gran_mendoza, "Gran Mendoza")
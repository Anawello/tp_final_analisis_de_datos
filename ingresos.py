import pandas as pd
from ipc_mensual import inflacion_mensual
from utils import ANIOS
from graficos import grafico_de_lineas

INFLACION_MENSUAL = pd.DataFrame(inflacion_mensual)

# ---------------------------------------------------------------------------- #
#                                    Sueldos                                   #
# ---------------------------------------------------------------------------- #
# ----------------------------- obtencion de ipc e ingreso medio nominal / real----------------------------- #
def obtener_ipc_trimestral() -> pd.DataFrame: 
    ipc_trimestrales = []
    trimestre = 1

    for i in range(INFLACION_MENSUAL.size // 4):
        try:
            ipc_trimestral = 1

            for j in range(3):
                ipc_trimestral *= (INFLACION_MENSUAL.loc[i*3 + j, "ipc"]) / 100 + 1

            ipc_trimestral -= 1
            ipc_trimestrales.append({
                "anio":int(INFLACION_MENSUAL.loc[i*3, "anio"]), 
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

def obtener_ipc_trimestral_acumulada() -> pd.DataFrame:
    ipc_trimestrales = obtener_ipc_trimestral()
    # seteo punto de comparacion
    ipc_trimestrales.loc[0, "ipc"] = 0 

    df = pd.DataFrame()
    df["anio"] = ipc_trimestrales["anio"]
    df["trimestre"] = ipc_trimestrales["trimestre"]
    df["ipc_acumulado"] = (1 + ipc_trimestrales["ipc"]).cumprod()

    return df

def obtener_ingreso_nominal(df_muestra: pd.DataFrame) -> pd.DataFrame:
    medias_trimestrales = []
    for anio in ANIOS:
        df_anual = df_muestra[df_muestra["ANO4"] ==  str(anio)]

        if df_anual.size == 0:
            continue

        for trimestre in range(4):
            df_trimestral = df_anual[df_anual["TRIMESTRE"] == str(trimestre + 1)]

            if df_trimestral.size == 0:
                continue
            
            ingreso_total_individual = pd.to_numeric(df_trimestral["P47T"], errors="coerce")
            pondera = pd.to_numeric(df_trimestral["PONDERA"], errors="coerce")
            media_ponderada = (ingreso_total_individual * pondera).sum() / pondera.sum()

            medias_trimestrales.append({"anio":anio, "trimestre": trimestre + 1,"ingreso_media_ponderada": media_ponderada})

    return pd.DataFrame(medias_trimestrales);

def obtener_ingreso_real(df_ingreso_medio: pd.DataFrame) -> pd.DataFrame:
    df_ipc_trimestral_acumulada = obtener_ipc_trimestral_acumulada()

    df = df_ipc_trimestral_acumulada.merge(df_ingreso_medio, on=["anio", "trimestre"])
    df["ingreso_media_real"] = df["ingreso_media_ponderada"] / df["ipc_acumulado"]

    return df

# --------------------------------- Graficos --------------------------------- #
def graficar_ingreso_real(df_gran_mendoza: pd.DataFrame, df_gba: pd.DataFrame):

    df_ingreso_medio_nominal_gran_mendoza = obtener_ingreso_nominal(df_gran_mendoza);
    df_ingreso_medio_nominal_gba = obtener_ingreso_nominal(df_gba);

    df_ingreso_medio_real_gran_mendoza = obtener_ingreso_real(df_ingreso_medio_nominal_gran_mendoza);
    df_ingreso_medio_real_gba = obtener_ingreso_real(df_ingreso_medio_nominal_gba);

    df_ingresos_reales = pd.DataFrame()
    df_ingresos_reales["anio"] = df_ingreso_medio_real_gba["anio"]
    df_ingresos_reales["trimestre"] = df_ingreso_medio_real_gba["trimestre"]
    df_ingresos_reales["gran_mendoza"] = df_ingreso_medio_real_gran_mendoza["ingreso_media_real"]
    df_ingresos_reales["gba"] = df_ingreso_medio_real_gba["ingreso_media_real"]

    grafico_de_lineas(
        df_ingresos_reales, 
        f"Evolución comparativa de del Ingreso Real en Gran Mendoza y GBA (2017-2025)", 
        "Años", 
        f"Media ponderada", 
        "anio",
        [
            {"valor":"gran_mendoza", "label":"Gran Mendoza"},
            {"valor":"gba", "label":"GBA"}
        ]
    )

def graficar_ingreso_real_por_sexo(df_muestra: pd.DataFrame, aglomerado: str):
    # divido la muestra entre hombres y mujeres (aqui deben hacer la magia)
    df_hombre = df_muestra[df_muestra["CH04"] == "1"]
    df_mujer = df_muestra[df_muestra["CH04"] == "2"]

    # por categoria obtengo su ingreso nominal y luego el real
    df_ingreso_nominal_hombre = obtener_ingreso_nominal(df_hombre)
    df_ingreso_nominal_mujer = obtener_ingreso_nominal(df_mujer)

    df_ingreso_hombre_real = obtener_ingreso_real(df_ingreso_nominal_hombre)
    df_ingreso_mujer_real = obtener_ingreso_real(df_ingreso_nominal_mujer)

    # uno los resultados segun su categoria en un df
    df_ingreso_real_por_sexo = pd.DataFrame();
    df_ingreso_real_por_sexo["anio"] = df_ingreso_hombre_real["anio"]
    df_ingreso_real_por_sexo["hombre"] = df_ingreso_hombre_real["ingreso_media_real"]
    df_ingreso_real_por_sexo["mujer"] = df_ingreso_mujer_real["ingreso_media_real"]

    # decoro como se mostrara el aglomerado en el grafico
    display_aglomerado = aglomerado.replace("_", " ")
    if display_aglomerado == "gba":
            display_aglomerado = display_aglomerado.capitalize()
    else:
            display_aglomerado = display_aglomerado.title()

    grafico_de_lineas(
        df_ingreso_real_por_sexo, 
        f"Evolución comparativa de del Ingreso Real segun sexo en {display_aglomerado} (2017-2025)", 
        "Años", 
        f"Media ponderada", 
        "anio",
        [
            {"valor":"mujer", "label":"Mujer"},
            {"valor":"hombre", "label":"Hombre"}
        ]
    )
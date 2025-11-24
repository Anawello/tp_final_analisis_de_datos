import contextily as ctx
import geopandas as  gpd
import os
import matplotlib.pyplot as plt
from ingresos import obtener_ingreso_nominal, obtener_ingreso_real
from graficos import grafico_de_lineas
import pandas as pd 

# ----------------------------------------------------------------------------- #
#                        MAPA DE AGLOMERADOS SIN JSON
# ----------------------------------------------------------------------------- #

def graficar_aglomerado_con_ingresos(df_aglomerado, aglomerado):
    """
    Muestra el mapa de aglomerado coloreado según ingreso real urbano y rural.
    """
    # --------------------- CARGAR MAPA --------------------- #
    ruta_json = os.path.join(os.path.dirname(__file__), "aglomerados_eph.json")
    mapas_aglomerados = gpd.read_file(ruta_json)
    cord_gran_mendoza = mapas_aglomerados[mapas_aglomerados["eph_aglome"] == aglomerado]

    if cord_gran_mendoza.empty:
        print(f"No se encontró {aglomerado} en el JSON.")
        return

    gdf = gpd.GeoDataFrame(cord_gran_mendoza, geometry="geometry")
    gdf = gdf.set_crs("EPSG:5343", allow_override=True).to_crs(epsg=3857)

    # --------------------- VALIDAR DATAFRAME --------------------- #
    if df_aglomerado is None or df_aglomerado.empty:
        print("df_gran_mendoza está vacío.")
        return

    # --------------------- DETECTAR ZONA --------------------- #
    def es_columna_zona(serie):
        vals = serie.dropna().astype(str).str.lower().unique()
        if set(vals).issubset({"1","2","1.0","2.0"}):
            return True
        if any(v in {"urbano","rural","urb","rur"} for v in vals):
            return True
        return False

    col_zona = None
    for c in df_aglomerado.columns:
        try:
            if es_columna_zona(df_aglomerado[c]):
                col_zona = c
                break
        except:
            continue

    if col_zona is None:
        print("No se encontró la columna ZONA.")
        return

    df = df_aglomerado.copy()
    df["ZONA"] = (
        df[col_zona].astype(str)
        .str.lower()
        .replace({"urbano":"1","urb":"1","rural":"2","rur":"2"})
        .astype(int)
    )

    # --------------------- INGRESOS --------------------- #
    df_urbano = df[df["ZONA"]==1]
    df_rural  = df[df["ZONA"]==2]

    print(df_rural.head())
    
    df_nom_urbano = obtener_ingreso_nominal(df_urbano)
    df_nom_rural  = obtener_ingreso_nominal(df_rural)

    print(df_nom_rural.head())

    df_real_urbano = obtener_ingreso_real(df_nom_urbano)
    df_real_rural  = obtener_ingreso_real(df_nom_rural)

    # Tomamos el último año disponible para colorear
    ingreso_urbano = df_real_urbano["ingreso_media_real"].iloc[-1]
    ingreso_rural  = df_real_rural["ingreso_media_real"].iloc[-1]

    # --------------------- MAPA CON COLORES --------------------- #
    fig, ax = plt.subplots(figsize=(12,12))

    # capa urbana
    gdf.plot(
        ax=ax,
        color="orangered",
        alpha=0.5,
        edgecolor="black",
        linewidth=1,
        label=f"Urbano: {ingreso_urbano:,.0f}"
    )

    # capa rural (semi-transparente)
    gdf.plot(
        ax=ax,
        color="royalblue",
        alpha=0.5,
        edgecolor="black",
        linewidth=1,
        label=f"Rural: {ingreso_rural:,.0f}"
    )

    # límites del mapa
    xmin, ymin, xmax, ymax = gdf.total_bounds
    ax.set_xlim(xmin, xmax); ax.set_ylim(ymin, ymax)

    # basemap
    ctx.add_basemap(ax, crs=gdf.crs)

    # título y leyenda
    ax.set_title(f"{aglomerado} – Ingreso Real Urbano vs Rural (último año)", fontsize=16)
    ax.legend(title="Ingreso Real (media ponderada)")
    plt.tight_layout()
    plt.show()

def graficar_gba_con_ingresos(df_gba):
    """
    Mapa de Partidos del GBA diferenciando cada aglomerado,
    y gráfico de ingreso real urbano por aglomerado.
    """

    # --------------------- MAPA --------------------- #
    ruta_json = os.path.join(os.path.dirname(__file__), "aglomerados_eph.json")
    mapas_aglomerados = gpd.read_file(ruta_json)
    mapas_aglomerados["eph_aglome_norm"] = mapas_aglomerados["eph_aglome"].str.lower().str.strip()

    # Filtrar solo CABA y Partidos del GBA
    gdf = mapas_aglomerados[mapas_aglomerados["eph_aglome_norm"].isin(["partidos del gba"])].copy()
    if gdf.empty:
        print("No se encontraron Partidos del GBA.")
        return

    gdf = gdf.set_crs("EPSG:5343", allow_override=True).to_crs(epsg=3857)

    # Colores por aglomerado
    colores = {"partidos del gba": "orange"}
    gdf["color"] = gdf["eph_aglome_norm"].map(colores)

    # Plot del mapa
    fig, ax = plt.subplots(figsize=(12, 12))
    for aglome, df_agl in gdf.groupby("eph_aglome_norm"):
        df_agl.plot(
            ax=ax,
            color=df_agl["color"].iloc[0],
            alpha=0.6,
            edgecolor="black",
            linewidth=1
        )

    xmin, ymin, xmax, ymax = gdf.total_bounds
    ax.set_xlim(xmin, xmax); ax.set_ylim(ymin, ymax)
    ctx.add_basemap(ax, crs=gdf.crs)
    ax.set_title("Partidos del GBA", fontsize=16)

    # Leyenda manual
    from matplotlib.patches import Patch
    handles = [Patch(facecolor=color, label=aglome.upper()) for aglome, color in colores.items()]
    ax.legend(handles=handles, title="Aglomerado")
    plt.tight_layout()
    plt.show()

    # --------------------- VALIDACIÓN --------------------- #
    if df_gba is None or df_gba.empty:
        print("df_gba está vacío.")
        return

    # Solo urbano (GBA son urbanos)
    df_gba["ZONA"] = 1

    # --------------------- INGRESO REAL POR AGLOMERADO --------------------- #
    ingresos_aglomerado = {}
    
    if df_gba.empty:
        print(f"No hay datos para {aglome}")
    df_nom = obtener_ingreso_nominal(df_gba)
        
    df_real = obtener_ingreso_real(df_nom)
    ingresos_aglomerado[aglome] = df_real

    if not ingresos_aglomerado:
        print("No hay datos para graficar ingresos reales.")
        return

    # --------------------- DF FINAL PARA GRAFICO --------------------- #
    df_ingresos = pd.DataFrame()
    df_ingresos["anio"] = ingresos_aglomerado[list(ingresos_aglomerado.keys())[0]]["anio"]
    for aglome, df_real in ingresos_aglomerado.items():
        df_ingresos[aglome.lower().replace(" ", "_")] = df_real["ingreso_media_real"]

    print("\nDF FINAL (df_ingresos):")
    print(df_ingresos.head())

    # --------------------- GRÁFICO --------------------- #
    lineas = [{"valor": col, "label": col.replace("_", " ").title()} for col in df_ingresos.columns if col != "anio"]
    grafico_de_lineas(
        df_ingresos,
        "Ingreso Real Urbano – GBA (2017–2025)",
        "Años",
        "Ingreso Real (media ponderada)",
        "anio",
        lineas
    )


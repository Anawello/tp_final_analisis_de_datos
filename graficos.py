import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from typing import TypedDict, List
import numpy as np

class DatoGrafico(TypedDict):
    valor: str
    label: str

# ---------------------------------------------------------------------------- #
#                                   Graficos                                   #
# ---------------------------------------------------------------------------- #
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

    plt.xticks(df[valor_ref]) # fuerzo a mostrar todos los anios
    plt.tight_layout()
    plt.show()

def graficar_distribucion_ingreso(df_original: pd.DataFrame, df_imputado: pd.DataFrame, aglomerado: str):
    """Compara la distribución de P47T (transformada a logaritmo) antes y después de la imputación."""
    
    # Prepara los DataFrames (filtrando solo ocupados con ingresos > 0)
    df_original_clean = df_original[df_original['P47T'].astype(float) > 0].copy()
    df_imputado_clean = df_imputado[df_imputado['P47T'].astype(float) > 0].copy()

    # --- LIMPIEZA DEFINITIVA DE P47T ---
    # Convertir todo a string primero, por si hay mezclas raras
    df_original_clean['P47T'] = df_original_clean['P47T'].astype(str)

    # Reemplazar comas, espacios, símbolos
    df_original_clean['P47T'] = df_original_clean['P47T'].str.replace(",", ".", regex=False)
    df_original_clean['P47T'] = df_original_clean['P47T'].str.replace("$", "", regex=False)
    df_original_clean['P47T'] = df_original_clean['P47T'].str.replace(" ", "", regex=False)

    # Convertir definitivamente a numérico
    df_original_clean['P47T'] = pd.to_numeric(df_original_clean['P47T'], errors='coerce')

    # --- LIMPIEZA DEFINITIVA DE P47T ---
    # Convertir todo a string primero, por si hay mezclas raras
    df_imputado_clean['P47T'] = df_original_clean['P47T'].astype(str)

    # Reemplazar comas, espacios, símbolos
    df_imputado_clean['P47T'] = df_imputado_clean['P47T'].str.replace(",", ".", regex=False)
    df_imputado_clean['P47T'] = df_imputado_clean['P47T'].str.replace("$", "", regex=False)
    df_imputado_clean['P47T'] = df_imputado_clean['P47T'].str.replace(" ", "", regex=False)

    # Convertir definitivamente a numérico
    df_imputado_clean['P47T'] = pd.to_numeric(df_imputado_clean['P47T'], errors='coerce')

    plt.figure(figsize=(10, 6))
    
    # Histograma del logaritmo del ingreso original
    sns.histplot(np.log(df_original_clean['P47T']), 
                 kde=True, color='blue', alpha=0.5, 
                 label='Original (solo reportados)', stat="density", bins=30)
    
    # Histograma del logaritmo del ingreso imputado 
    sns.histplot(np.log(df_imputado_clean['P47T']), 
                 kde=True, color='red', alpha=0.3, 
                 label='Imputado (ocupados)', stat="density", bins=30)
    
    plt.title(f'Distribución del Log(Ingreso) en {aglomerado} (Original vs. Imputado)')
    plt.xlabel('Logaritmo de P47T')
    plt.legend()
    plt.show()
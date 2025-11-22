import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from typing import TypedDict, List

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

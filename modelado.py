import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error
import matplotlib.pyplot as plt

def imputar_ingresos(df: pd.DataFrame, aglomerado_name: str = "AGLOMERADO") -> pd.DataFrame:
    """
    Rellena P47T faltantes usando RandomForest sobre log(P47T).
    Devuelve df con P47T imputado.
    """
    df = limpiar_eph(df)
    df = quitar_outliers_percentiles(df, "P47T")
    df_model = df.copy()

    # ---------------------------
    # Marcar códigos que indican no-respondió / no aplica
    # (Ej.: -9 u otros - revisar si aparecen en tu tabla)
    # ---------------------------
    if "P47T" in df_model.columns:
        # INDEC usa códigos negativos para indicar valores especiales; transformamos a NaN.
        df_model.loc[df_model["P47T"] < 0, "P47T"] = np.nan

    # ---------------------------
    # Variables candidatas (adaptalas si querés otras)
    # ---------------------------
    posibles_features = [
        "ANO4", "TRIMESTRE", "CH04", "CH06", "CAT_OCUP", "ESTADO",
        "NIVEL_ED", "REGION", "AGLOMERADO", "PP3E_TOT", "PP3F_TOT",
        "ITF", "P21"
    ]
    # dejar solo las que realmente existen
    features = [c for c in posibles_features if c in df_model.columns]

    # ---------------------------
    # Asegurar que las features sean numéricas (coerce -> NaN si no convertible)
    # ---------------------------
    for c in features:
        df_model[c] = pd.to_numeric(df_model[c], errors="coerce")

    # ---------------------------
    # Crear variable objetivo transformada: P47T_LOG = log1p(P47T)
    # ---------------------------
    # (1) marcar filas con P47T válidos; (2) crear log
    df_model["P47T_ORIG"] = df_model["P47T"]  # backup
    df_model.loc[df_model["P47T_ORIG"].isna(), "P47T_ORIG"] = np.nan
    # ahora crear P47T_LOG solo para valores no nulos
    df_model["P47T_LOG"] = np.nan
    mask_valid = df_model["P47T_ORIG"].notna() & (df_model["P47T_ORIG"] >= 0)
    df_model.loc[mask_valid, "P47T_LOG"] = np.log1p(df_model.loc[mask_valid, "P47T_ORIG"])

    # ---------------------------
    # Split train / a predecir
    # ---------------------------
    df_train = df_model[df_model["P47T_LOG"].notna()].copy()
    df_pred  = df_model[df_model["P47T_LOG"].isna()].copy()

    if df_pred.empty:
        print("No hay valores faltantes en P47T para imputar.")
        return df_model

    # ---------------------------
    # Preparar X, y (usar columnas que no estén vacías)
    # ---------------------------
    X = df_train[features].copy()
    y = df_train["P47T_LOG"].copy()

    # Si todas las columnas están vacías o X vacío -> abortar
    if X.shape[1] == 0:
        raise RuntimeError("No existen features válidas para entrenar el modelo. Revisa 'features'.")

    # Convertir a num y rellenar medianas donde haga falta
    X = X.apply(pd.to_numeric, errors="coerce")
    # eliminar columnas que quedan totalmente vacías (no sirven)
    X = X.dropna(axis=1, how="all")
    # rellenar NaNs por mediana (solo columnas numéricas)
    X = X.fillna(X.median(numeric_only=True))

    # Guardar columnas finales usadas
    final_features = X.columns.tolist()
    print("Features usadas para el modelo:", final_features)

    # ---------------------------
    # Entrenamiento
    # ---------------------------
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=300, max_depth=12, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    # Evaluación (en espacio log)
    y_pred_test_log = model.predict(X_test)
    r2 = r2_score(y_test, y_pred_test_log)
    mae_log = mean_absolute_error(y_test, y_pred_test_log)
    print(f"Evaluación (log-space) — R2: {r2:.4f}, MAE (log): {mae_log:.4f}")

    # ---------------------------
    # Predecir en los registros que faltan P47T
    # ---------------------------
    X_pred = df_pred[final_features].copy()
    X_pred = X_pred.apply(pd.to_numeric, errors="coerce")
    X_pred = X_pred.fillna(X.median(numeric_only=True))

    y_pred_log = model.predict(X_pred)
    # volver a escala original
    y_pred_orig = np.expm1(y_pred_log)

    # asignar las predicciones (en la columna P47T)
    df_model.loc[df_model["P47T"].isna(), "P47T"] = y_pred_orig

    # ---------------------------
    # Gráfico: Real vs Predicho (en escala original)
    # ---------------------------
    # PREPARAR datos reales vs predichos en el train
    X_train_full = X_train.copy()
    y_train_orig = np.expm1(y_train)  # convertir train (log->orig)
    y_train_pred_orig = np.expm1(model.predict(X_train))

    # Estadísticas de error en escala original
    r2_orig = r2_score(y_train_orig, y_train_pred_orig)
    mae_orig = mean_absolute_error(y_train_orig, y_train_pred_orig)
    print(f"Evaluación (orig-space, sobre train): R2: {r2_orig:.4f}, MAE: {mae_orig:.2f}")

    plt.figure(figsize=(7, 5))
    plt.scatter(y_train_orig, y_train_pred_orig, alpha=0.3)
    max_val = max(y_train_orig.max(), y_train_pred_orig.max())
    plt.plot([0, max_val], [0, max_val], linestyle="--")
    plt.xlabel("Ingreso real P47T (original)")
    plt.ylabel("Ingreso predicho por el modelo (original)")
    plt.title(f"Dispersión Real vs Predicho - {aglomerado_name}\nR2={r2_orig:.3f}  MAE={mae_orig:.0f}")
    plt.tight_layout()
    plt.show()

    return df_model

def limpiar_eph(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1) Normalizar strings que representan nulos
    nul_values = ["None", "none", "NONE", "Nan", "NaN", "nan", "", " ", "   ", "    "]
    df = df.replace(nul_values, np.nan).infer_objects(copy=False)

    # 2) Coma decimal -> punto decimal en columnas object que parezcan numéricas
    for col in df.columns:
        if df[col].dtype == object:
            # si al menos un valor tiene formato dígito,coma,dígito
            if df[col].astype(str).str.contains(r"^\s*\d+,\d+\s*$", regex=True).any():
                df[col] = df[col].astype(str).str.replace(",", ".", regex=False)

    # 3) Intentar convertir columnas que sean completamente numéricas
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col], errors="raise")
            except Exception:
                # si falla, dejar como está (texto)
                pass

    return df
def quitar_outliers_percentiles(df, col="P47T", p_low=0.01, p_high=0.99):
    df = df.copy()
    q_low  = df[col].quantile(p_low)
    q_high = df[col].quantile(p_high)
    return df[(df[col] >= q_low) & (df[col] <= q_high)]
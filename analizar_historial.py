"""
Analizador de Historial de Bots de Binance
==========================================
Lee el CSV histórico, agrupa por strategy_id y fecha, calcula evolución de copias
e identifica bots en crecimiento, declive, nuevos y muertos.
Exporta los resultados a Google Sheets.
"""

import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

# ── Configuración ──────────────────────────────────────────────
DATA_DIR = Path(__file__).parent / "data"
HISTORIAL_CSV = DATA_DIR / "historial_bots.csv"
CREDENTIALS_JSON = Path(__file__).parent / "credentials.json"

# Alcance para Google Sheets
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def cargar_historial() -> pd.DataFrame:
    """Carga el CSV histórico si existe."""
    if not HISTORIAL_CSV.exists():
        print(f"❌ Archivo {HISTORIAL_CSV} no encontrado")
        return pd.DataFrame()
    
    df = pd.read_csv(HISTORIAL_CSV)
    print(f"✅ Historial cargado: {len(df)} registros")
    return df


def analizar_evoluciones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa por strategy_id y fecha, calcula evolución de copias.
    Retorna un DataFrame con análisis de cambios, crecimiento, declive, etc.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Asegurar tipos correctos
    df["fecha"] = pd.to_datetime(df["fecha"])
    df["copias"] = pd.to_numeric(df["copias"], errors="coerce")
    df["strategy_id"] = df["strategy_id"].astype(str)
    
    # Agrupar por strategy_id y obtener el máximo de información
    analisis = []
    
    for strategy_id, grupo in df.groupby("strategy_id"):
        grupo = grupo.sort_values("fecha")
        
        primera_fecha = grupo["fecha"].min()
        ultima_fecha = grupo["fecha"].max()
        copias_inicial = grupo["copias"].iloc[0] if not grupo.empty else 0
        copias_final = grupo["copias"].iloc[-1] if not grupo.empty else 0
        copias_max = grupo["copias"].max()
        copias_min = grupo["copias"].min()
        
        # Calcular variación
        variacion = copias_final - copias_inicial
        porcentaje_variacion = (variacion / copias_inicial * 100) if copias_inicial > 0 else 0
        
        # Determinar estado
        if primera_fecha == ultima_fecha:
            estado = "NUEVO"
        elif copias_final == 0:
            estado = "MUERTO"
        elif variacion > 0:
            estado = "CRECIMIENTO"
        elif variacion < 0:
            estado = "DECLIVE"
        else:
            estado = "ESTABLE"
        
        # Información del último snapshot
        ultima_fila = grupo.iloc[-1]
        par = ultima_fila.get("par", "")
        roi = ultima_fila.get("roi", "")
        pnl = ultima_fila.get("pnl", "")
        
        analisis.append({
            "strategy_id": strategy_id,
            "par": par,
            "roi": roi,
            "pnl": pnl,
            "primera_fecha": primera_fecha,
            "ultima_fecha": ultima_fecha,
            "copias_inicial": copias_inicial,
            "copias_final": copias_final,
            "copias_max": copias_max,
            "copias_min": copias_min,
            "variacion_copias": variacion,
            "porcentaje_variacion": round(porcentaje_variacion, 2),
            "estado": estado,
            "dias_registrados": len(grupo)
        })
    
    return pd.DataFrame(analisis)


def conectar_google_sheets(nombre_hoja: str) -> tuple:
    """
    Se conecta a Google Sheets usando credenciales de servicio (service account).
    Retorna (gc, worksheet) o (None, None) si falla.
    """
    if not CREDENTIALS_JSON.exists():
        print(f"❌ Archivo {CREDENTIALS_JSON} no encontrado")
        return None, None
    
    try:
        creds = ServiceAccountCredentials.from_service_account_file(
            str(CREDENTIALS_JSON),
            scopes=SCOPES
        )
        gc = gspread.authorize(creds)
        print(f"✅ Conectado a Google Sheets")
        
        # Buscar o crear hoja
        try:
            sh = gc.open(nombre_hoja)
            print(f"✅ Hoja '{nombre_hoja}' abierta")
        except gspread.SpreadsheetNotFound:
            sh = gc.create(nombre_hoja)
            print(f"✅ Hoja '{nombre_hoja}' creada")
        
        # Acceder a la primera hoja de trabajo
        ws = sh.sheet1
        return gc, ws
    
    except Exception as e:
        print(f"❌ Error conectando a Google Sheets: {e}")
        return None, None


def exportar_a_google_sheets(df_analisis: pd.DataFrame, nombre_hoja: str = "Bot Binance - Análisis"):
    """
    Exporta el análisis a una hoja de Google Sheets.
    """
    gc, ws = conectar_google_sheets(nombre_hoja)
    if gc is None or ws is None:
        print("⚠️  No se pudo exportar a Google Sheets. Guardando solo localmente.")
        return False
    
    try:
        # Limpiar la hoja existente
        ws.clear()
        print("✅ Hoja limpiada")
        
        # Convertir DataFrame a lista de listas (incluyendo headers)
        datos = [df_analisis.columns.tolist()] + df_analisis.values.tolist()
        
        # Actualizar valores en la hoja
        ws.update("A1", datos)
        print(f"✅ {len(df_analisis)} registros exportados a Google Sheets")
        
        return True
    
    except Exception as e:
        print(f"❌ Error exportando a Google Sheets: {e}")
        return False


def guardar_localmente(df_analisis: pd.DataFrame):
    """
    Guarda el análisis en un CSV local para referencia.
    """
    output_csv = DATA_DIR / "analisis_bots.csv"
    df_analisis.to_csv(output_csv, index=False)
    print(f"💾 Análisis guardado en {output_csv}")


def mostrar_resumen(df_analisis: pd.DataFrame):
    """
    Imprime un resumen del análisis en consola.
    """
    if df_analisis.empty:
        print("❌ No hay datos para analizar")
        return
    
    print("\n" + "="*80)
    print("📊 RESUMEN DE ANÁLISIS DE BOTS")
    print("="*80)
    
    # Contar por estado
    estados = df_analisis["estado"].value_counts()
    print(f"\n📈 Distribución por estado:")
    for estado, count in estados.items():
        print(f"  {estado}: {count}")
    
    # Top 5 en crecimiento
    print(f"\n🚀 Top 5 bots en CRECIMIENTO:")
    crecimiento = df_analisis[df_analisis["estado"] == "CRECIMIENTO"].nlargest(5, "variacion_copias")
    if not crecimiento.empty:
        for _, row in crecimiento.iterrows():
            print(f"  {row['strategy_id']} ({row['par']}): +{int(row['variacion_copias'])} copias ({row['porcentaje_variacion']:.1f}%)")
    else:
        print("  Ninguno")
    
    # Top 5 en declive
    print(f"\n📉 Top 5 bots en DECLIVE:")
    declive = df_analisis[df_analisis["estado"] == "DECLIVE"].nsmallest(5, "variacion_copias")
    if not declive.empty:
        for _, row in declive.iterrows():
            print(f"  {row['strategy_id']} ({row['par']}): {int(row['variacion_copias'])} copias ({row['porcentaje_variacion']:.1f}%)")
    else:
        print("  Ninguno")
    
    # Estadísticas generales
    print(f"\n📋 Estadísticas generales:")
    print(f"  Total de bots únicos: {len(df_analisis)}")
    print(f"  Bots nuevos: {len(df_analisis[df_analisis['estado'] == 'NUEVO'])}")
    print(f"  Bots muertos: {len(df_analisis[df_analisis['estado'] == 'MUERTO'])}")
    print(f"  Promedio de copias actual: {df_analisis['copias_final'].mean():.0f}")
    print(f"  Máximo de copias registrado: {df_analisis['copias_max'].max():.0f}")
    print("\n" + "="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Analizador del historial de bots de Binance"
    )
    parser.add_argument(
        "--google-sheets", action="store_true",
        help="Exportar análisis a Google Sheets (requiere credentials.json)"
    )
    parser.add_argument(
        "--nombre-hoja", type=str, default="Bot Binance - Análisis",
        help="Nombre de la hoja de Google Sheets (default: 'Bot Binance - Análisis')"
    )
    
    args = parser.parse_args()
    
    print("🚀 Iniciando análisis del historial...")
    
    # Cargar historial
    df_historial = cargar_historial()
    if df_historial.empty:
        return
    
    # Analizar evoluciones
    print("🔍 Analizando evoluciones...")
    df_analisis = analizar_evoluciones(df_historial)
    
    if df_analisis.empty:
        print("❌ No se pudieron generar análisis")
        return
    
    # Mostrar resumen
    mostrar_resumen(df_analisis)
    
    # Guardar localmente
    guardar_localmente(df_analisis)
    
    # Exportar a Google Sheets si se solicita
    if args.google_sheets:
        exportar_a_google_sheets(df_analisis, args.nombre_hoja)
    
    print("✅ Análisis completado")


if __name__ == "__main__":
    main()

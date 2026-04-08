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
import os
import urllib.parse
import urllib.request
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


def seleccionar_mejores_bots(
    df_analisis: pd.DataFrame,
    min_copias: int = 20,
    min_dias: int = 3,
    top_n: int = 5,
) -> pd.DataFrame:
    """Selecciona mejores bots usando score combinado de popularidad y crecimiento."""
    if df_analisis.empty:
        return pd.DataFrame()

    df = df_analisis.copy()
    df["copias_final"] = pd.to_numeric(df["copias_final"], errors="coerce").fillna(0)
    df["variacion_copias"] = pd.to_numeric(df["variacion_copias"], errors="coerce").fillna(0)
    df["dias_registrados"] = pd.to_numeric(df["dias_registrados"], errors="coerce").fillna(0)

    crecimiento = df[
        (df["estado"] == "CRECIMIENTO")
        & (df["copias_final"] >= min_copias)
        & (df["dias_registrados"] >= min_dias)
    ].copy()

    if crecimiento.empty:
        # Fallback para etapas tempranas: priorizar NUEVO con más copias
        crecimiento = df[df["estado"].isin(["NUEVO", "ESTABLE", "CRECIMIENTO"])].copy()

    if crecimiento.empty:
        return pd.DataFrame()

    max_copias = max(crecimiento["copias_final"].max(), 1)
    max_var = max(crecimiento["variacion_copias"].clip(lower=0).max(), 1)

    crecimiento["score_popularidad"] = crecimiento["copias_final"] / max_copias
    crecimiento["score_crecimiento"] = crecimiento["variacion_copias"].clip(lower=0) / max_var
    crecimiento["score_total"] = (0.6 * crecimiento["score_popularidad"] + 0.4 * crecimiento["score_crecimiento"]).round(4)

    columnas = [
        "strategy_id", "par", "estado", "copias_final",
        "variacion_copias", "porcentaje_variacion", "dias_registrados", "score_total"
    ]
    return crecimiento.sort_values(["score_total", "copias_final"], ascending=False)[columnas].head(top_n)


def enviar_telegram(mensaje: str, bot_token: str, chat_id: str) -> bool:
    """Envía un mensaje a Telegram usando la API Bot."""
    if not bot_token or not chat_id:
        print("⚠️  Telegram no configurado (faltan token/chat_id)")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensaje,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=20) as resp:
            ok = resp.status == 200
        if ok:
            print("✅ Mensaje enviado a Telegram")
            return True
        print("❌ Telegram respondió con estado no exitoso")
        return False
    except Exception as e:
        print(f"❌ Error enviando Telegram: {e}")
        return False


def construir_mensaje_mejores_bots(df_top: pd.DataFrame) -> str:
    """Construye el mensaje de recomendación para Telegram."""
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if df_top.empty:
        return (
            f"📊 <b>Bot Binance - Selección</b>\n"
            f"🕒 {fecha}\n\n"
            f"No hay bots que cumplan los criterios en este ciclo."
        )

    lineas = [
        "📊 <b>Bot Binance - Mejores Bots</b>",
        f"🕒 {fecha}",
        "",
    ]

    for i, (_, row) in enumerate(df_top.iterrows(), start=1):
        lineas.append(
            f"{i}. <b>{row['par']}</b> | ID: <code>{row['strategy_id']}</code>\n"
            f"   Estado: {row['estado']} | Copias: {int(row['copias_final'])}\n"
            f"   Δ Copias: {int(row['variacion_copias'])} | Score: {row['score_total']:.3f}"
        )

    return "\n".join(lineas)


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


def conectar_google_sheets(nombre_hoja: str, spreadsheet_id: str | None = None) -> tuple:
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

        if spreadsheet_id:
            sh = gc.open_by_key(spreadsheet_id)
            print(f"✅ Hoja con ID '{spreadsheet_id}' abierta")
        else:
            sh = gc.open(nombre_hoja)
            print(f"✅ Hoja '{nombre_hoja}' abierta")
        
        # Acceder a la primera hoja de trabajo
        ws = sh.sheet1
        return gc, ws

    except gspread.SpreadsheetNotFound:
        print("❌ No se encontró la hoja. Créala en tu Google Drive y compártela con la cuenta de servicio.")
        return None, None
    
    except Exception as e:
        print(f"❌ Error conectando a Google Sheets: {e}")
        return None, None


def exportar_a_google_sheets(
    df_analisis: pd.DataFrame,
    nombre_hoja: str = "Bot Binance - Análisis",
    spreadsheet_id: str | None = None,
):
    """
    Exporta el análisis a una hoja de Google Sheets.
    """
    gc, ws = conectar_google_sheets(nombre_hoja, spreadsheet_id)
    if gc is None or ws is None:
        print("⚠️  No se pudo exportar a Google Sheets. Guardando solo localmente.")
        return False
    
    try:
        # Limpiar la hoja existente
        ws.clear()
        print("✅ Hoja limpiada")

        df_exportar = df_analisis.copy()
        for columna in df_exportar.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            df_exportar[columna] = df_exportar[columna].dt.strftime("%Y-%m-%d %H:%M:%S")

        datos = [df_exportar.columns.tolist()] + df_exportar.astype(object).where(pd.notna(df_exportar), "").values.tolist()

        # Actualizar valores en la hoja
        ws.update(range_name="A1", values=datos)
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
    parser.add_argument(
        "--spreadsheet-id", type=str, default=None,
        help="ID de una hoja existente de Google Sheets ya compartida con la cuenta de servicio"
    )
    parser.add_argument(
        "--telegram", action="store_true",
        help="Enviar selección de mejores bots por Telegram"
    )
    parser.add_argument(
        "--telegram-token", type=str, default=None,
        help="Token del bot de Telegram (si no se envía, usa TELEGRAM_BOT_TOKEN)"
    )
    parser.add_argument(
        "--telegram-chat-id", type=str, default=None,
        help="Chat ID de Telegram (si no se envía, usa TELEGRAM_CHAT_ID)"
    )
    parser.add_argument(
        "--top-n", type=int, default=5,
        help="Cantidad de bots a seleccionar para enviar por Telegram"
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
        exportar_a_google_sheets(df_analisis, args.nombre_hoja, args.spreadsheet_id)

    # Selección y envío por Telegram
    if args.telegram:
        df_top = seleccionar_mejores_bots(df_analisis, top_n=args.top_n)
        mensaje = construir_mensaje_mejores_bots(df_top)
        token = args.telegram_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = args.telegram_chat_id or os.getenv("TELEGRAM_CHAT_ID", "")
        enviar_telegram(mensaje, token, chat_id)
    
    print("✅ Análisis completado")


if __name__ == "__main__":
    main()

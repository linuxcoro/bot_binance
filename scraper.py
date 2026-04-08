"""
Scraper del Mercado de Bots de Binance (Grid de Futuros)
=========================================================
Usa Playwright para navegar a la página de trading bots de Binance,
interceptar las llamadas internas a la API (bapi/algo) y extraer
la lista de bots con sus detalles en tiempo real.
"""

import argparse
import asyncio
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright
from tabulate import tabulate

# ── Configuración ──────────────────────────────────────────────
URL_BOTS = "https://www.binance.com/es-LA/trading-bots"
WAIT_PAGE_LOAD = 8        # segundos para espera inicial

WAIT_BETWEEN_CLICKS = 3   # segundos entre clic en cada bot
HEADLESS = True            # True para oculto, False para ver el navegador
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
HISTORIAL_CSV = DATA_DIR / "historial_bots.csv"


def parse_args():
    """Argumentos de línea de comandos para filtrar resultados."""
    parser = argparse.ArgumentParser(
        description="Scraper del Mercado de Bots de Binance (Grid de Futuros)"
    )
    parser.add_argument(
        "--inversion-max", type=float, default=None,
        help="Inversión máxima en USDT (ej: --inversion-max 150)"
    )
    parser.add_argument(
        "--direccion", type=str, default=None,
        choices=["Long", "Short", "Neutral", "long", "short", "neutral"],
        help="Filtrar por dirección: Long, Short o Neutral"
    )
    parser.add_argument(
        "--copias-min", type=int, default=None,
        help="Filtrar por copias mínimas (ej: --copias-min 10)"
    )
    parser.add_argument(
        "--copias-max", type=int, default=None,
        help="Filtrar por copias máximas (ej: --copias-max 200)"
    )
    parser.add_argument(
        "--paginas-max", type=int, default=50,
        help="Máximo de páginas a recorrer (incluye la página actual). Ej: 50"
    )
    return parser.parse_args()


async def interceptar_respuestas(page, respuestas_api: list):
    """Listener que captura respuestas de los endpoints bapi/algo."""
    async def on_response(response):
        url = response.url
        if "bapi/algo" in url or "bapi/futures" in url:
            try:
                body = await response.json()
                respuestas_api.append({
                    "url": url,
                    "status": response.status,
                    "data": body,
                    "timestamp": datetime.now().isoformat()
                })
            except Exception:
                pass
    page.on("response", on_response)


async def navegar_a_grid_futuros(page):
    """Navega a la pestaña 'Grid de futuros' y aplica filtro 1-7 días."""
    await page.goto(URL_BOTS, wait_until="domcontentloaded")
    print("⏳ Esperando carga completa de la página...")
    await page.wait_for_timeout(WAIT_PAGE_LOAD * 1000)

    # Click en pestaña Grid de futuros
    try:
        tab = page.get_by_text("Grid de futuros", exact=True)
        await tab.click()
        print("✅ Pestaña 'Grid de futuros' seleccionada")
        await page.wait_for_timeout(3000)
    except Exception:
        try:
            tab = page.get_by_text("Futures Grid", exact=True)
            await tab.click()
            print("✅ Pestaña 'Futures Grid' seleccionada")
            await page.wait_for_timeout(3000)
        except Exception:
            print("⚠️  Continuando con la pestaña por defecto")

    # Aplicar filtro de duración 1-7 días
    try:
        # Si ya está activo por defecto, no hacer nada
        valor_actual = page.locator(".bn-select-field-input").filter(has_text=re.compile(r"1\s*-\s*7\s*d[ií]as", re.IGNORECASE)).first
        if await valor_actual.count() > 0:
            print("✅ Filtro '1-7 días' ya estaba activo (por defecto)")
            return

        # Abrir el dropdown de duración (contiene texto de días)
        dur_btn = page.locator(
            "button:has-text('día'), button:has-text('días'), "
            "[class*='filter']:has-text('día'), [class*='select']:has-text('día')"
        ).first
        await dur_btn.click()
        await page.wait_for_timeout(1000)
        # Seleccionar opción 1-7 días
        opcion_1_7 = page.locator("[role='option']").filter(has_text=re.compile(r"1\s*-\s*7\s*d[ií]as", re.IGNORECASE)).first
        await opcion_1_7.click()
        print("✅ Filtro '1-7 días' aplicado")
        await page.wait_for_timeout(2000)
    except Exception as e:
        print(f"⚠️  No se pudo aplicar filtro de duración: {e}")


async def cambiar_a_vista_lista(page):
    """Cambia a vista de lista para facilitar la extracción."""
    try:
        # Buscar el ícono de vista lista (generalmente el segundo ícono de vista)
        view_buttons = await page.query_selector_all('[class*="icon"], [class*="view"], [class*="list"]')
        for btn in view_buttons:
            aria = await btn.get_attribute("aria-label") or ""
            title = await btn.get_attribute("title") or ""
            if "list" in aria.lower() or "lista" in aria.lower() or "list" in title.lower():
                await btn.click()
                print("✅ Vista de lista activada")
                await page.wait_for_timeout(2000)
                return
    except Exception:
        pass
    print("ℹ️  Continuando con la vista actual")


async def optimizar_rendimiento(context):
        """Reduce carga de recursos pesados para evitar lentitud de la máquina."""
        async def _route(route):
                req = route.request
                if req.resource_type in {"image", "media", "font"}:
                        await route.abort()
                else:
                        await route.continue_()

        await context.route("**/*", _route)


async def ir_a_siguiente_pagina(page) -> bool:
        """Intenta avanzar a la siguiente página del marketplace."""
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(700)

        # Intentos por selectores comunes
        selectores_next = [
                "[class*='pagination'] [class*='next']",
                "[class*='pagination-next']",
                "button[aria-label*='next' i]",
                "[role='button'][aria-label*='next' i]",
        ]
        for sel in selectores_next:
                try:
                        btn = page.locator(sel).first
                        if await btn.count() > 0:
                                await btn.click(timeout=2000)
                                await page.wait_for_timeout(1800)
                                return True
                except Exception:
                        continue

        # Fallback JS: localizar contenedor de paginación y pulsar el siguiente
        try:
                clicked = await page.evaluate(
                        """
                        () => {
                            const containers = Array.from(document.querySelectorAll('div,nav,ul'))
                                .filter(el => /pagination/i.test((el.className || '').toString()));
                            for (const c of containers) {
                                const items = Array.from(c.querySelectorAll('button,a,li,[role="button"],div'))
                                    .filter(el => !!el && el.offsetParent !== null);

                                const idx = items.findIndex(el => {
                                    const cls = (el.className || '').toString();
                                    return /active|current/i.test(cls) || el.getAttribute('aria-current') === 'page';
                                });

                                if (idx >= 0 && idx + 1 < items.length) {
                                    items[idx + 1].click();
                                    return true;
                                }

                                const nextByText = items.find(el => /^(>|»|›|→)$/.test((el.textContent || '').trim()));
                                if (nextByText) {
                                    nextByText.click();
                                    return true;
                                }
                            }
                            return false;
                        }
                        """
                )
                if clicked:
                        await page.wait_for_timeout(1800)
                        return True
        except Exception:
                pass

        return False


async def extraer_lista_desde_dom(page) -> list[dict]:
    """Extrae la lista de bots directamente del DOM de la tabla."""
    bots = []

    # Intentar extraer de la tabla (vista lista)
    rows = await page.query_selector_all("table tbody tr, [class*='row'], [class*='strategy']")

    if not rows:
        print("⚠️  No se encontraron filas en la tabla, intentando con tarjetas...")
        rows = await page.query_selector_all("[class*='card'], [class*='item']")

    for row in rows:
        texto = await row.inner_text()
        if not texto.strip():
            continue
        bots.append({"texto_raw": texto.strip()})

    return bots


async def extraer_detalles_por_clic(page, respuestas_api: list) -> list[dict]:
    """Hace clic en cada bot para abrir el modal de detalles y capturar la respuesta."""
    detalles = []

    # Buscar botones "Copiar" que abren el modal de cada bot
    botones_copiar = await page.query_selector_all("text=Copiar")
    print(f"📋 Se encontraron {len(botones_copiar)} bots con botón 'Copiar'")

    for i, boton in enumerate(botones_copiar):
        try:
            n_antes = len(respuestas_api)
            await boton.click()
            print(f"  🔍 Abriendo detalle del bot {i + 1}...")
            await page.wait_for_timeout(WAIT_BETWEEN_CLICKS * 1000)

            # Extraer texto del modal si está abierto
            modal = await page.query_selector("[class*='modal'], [class*='dialog'], [class*='drawer'], [class*='popup']")
            if modal:
                texto_modal = await modal.inner_text()
                detalles.append({
                    "bot_index": i + 1,
                    "texto_modal": texto_modal.strip(),
                    "nuevas_respuestas_api": len(respuestas_api) - n_antes
                })

            # Cerrar el modal
            close_btn = await page.query_selector("[class*='close'], [aria-label='Close'], [class*='cerrar']")
            if close_btn:
                await close_btn.click()
                await page.wait_for_timeout(1000)
            else:
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(1000)

        except Exception as e:
            print(f"  ⚠️  Error en bot {i + 1}: {e}")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

    return detalles


# Mapeo de dirección numérica a texto
_DIR_MAP = {"1": "Neutral", "2": "Long", "3": "Short",
            1: "Neutral", 2: "Long", 3: "Short",
            "NEUTRAL": "Neutral", "LONG": "Long", "SHORT": "Short"}


def _segundos_a_legible(seg) -> str:
    """Convierte segundos a formato 'Xd Yh Zm'."""
    try:
        seg = int(float(seg))
    except (TypeError, ValueError):
        return str(seg)
    d = seg // 86400
    h = (seg % 86400) // 3600
    m = (seg % 3600) // 60
    partes = []
    if d:
        partes.append(f"{d}d")
    if h:
        partes.append(f"{h}h")
    if m or not partes:
        partes.append(f"{m}m")
    return " ".join(partes)


def _redondear(valor, decimales=2) -> str:
    """Convierte un valor numérico a string con N decimales."""
    try:
        return f"{float(valor):.{decimales}f}"
    except (TypeError, ValueError):
        return str(valor) if valor else ""


def _normalizar_roi(valor) -> str:
    """Normaliza ROI a string con 2 decimales, acepta formatos con '%' o texto."""
    if valor is None:
        return ""
    txt = str(valor).strip().replace("%", "")
    m = re.search(r"-?\d+(?:\.\d+)?", txt)
    if not m:
        return ""
    return _redondear(m.group(0), 2)


def parsear_respuestas_api(respuestas_api: list) -> pd.DataFrame:
    """Convierte las respuestas capturadas de la API en un DataFrame."""
    bots_data = []

    for resp in respuestas_api:
        data = resp.get("data", {})
        if not isinstance(data, dict):
            continue

        # Buscar la lista de estrategias en la respuesta
        items = data.get("data", data.get("list", []))
        if isinstance(items, dict):
            items = items.get("list", items.get("strategyList", []))
        if not isinstance(items, list):
            continue

        for item in items:
            if not isinstance(item, dict):
                continue

            # Dirección: puede ser número o string
            dir_raw = item.get("direction", "")
            direccion = _DIR_MAP.get(dir_raw, _DIR_MAP.get(str(dir_raw), str(dir_raw)))

            # MDD: intentar varios nombres de campo posibles
            mdd_raw = (
                item.get("maxDrawdown") or
                item.get("maxDrawdownRate") or
                item.get("mdd") or
                item.get("mdd7d") or
                item.get("drawdownRate") or
                item.get("weeklyMaxDrawdown") or
                item.get("maxDd") or
                ""
            )
            mdd = _redondear(mdd_raw) if mdd_raw != "" else ""

            bot = {
                "par": item.get("symbol", ""),
                "direccion": direccion,
                "roi": _redondear(item.get("roi", item.get("roiStr", ""))),
                "pnl": _redondear(item.get("pnl", item.get("pnlStr", ""))),
                "duracion": _segundos_a_legible(item.get("runningTime", item.get("duration", ""))),
                "≤1 día": mdd,
                "inversion_min": _redondear(item.get("minInvestment", item.get("copyMinInvestment", ""))),
                "copias": item.get("copyCount", ""),
                "grids": item.get("gridCount", item.get("gridNum", item.get("grid", ""))),
                "strategy_id": item.get("strategyId", item.get("id", "")),
            }
            # Solo agregar filas con par y al menos ROI o PnL
            if bot["par"] and (bot["roi"] or bot["pnl"]):
                bots_data.append(bot)

    if bots_data:
        df = pd.DataFrame(bots_data)
        df = df.drop_duplicates(subset=["strategy_id"], keep="last") if "strategy_id" in df.columns else df.drop_duplicates()
        return df

    return pd.DataFrame()


def _extraer_grid_desde_texto_modal(texto: str) -> str:
    """Extrae la cantidad de grids desde el texto del modal de detalle."""
    if not texto:
        return ""

    patrones = [
        r"Cantidad\s+de\s+grids\s*/\s*Modo\s*\n\s*(\d+)\s*/",
        r"(\d+)\s*/\s*(?:Aritm[eé]tico|Geom[eé]trico)",
    ]
    for patron in patrones:
        m = re.search(patron, texto, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return ""


def _extraer_numero_grids_desde_valor(valor: str) -> str:
    """Extrae el número de grids desde un valor tipo '169 / Aritmético'."""
    if not valor:
        return ""
    m = re.search(r"(\d+)\s*/", valor)
    if m:
        return m.group(1)
    m = re.search(r"\d+", valor)
    return m.group(0) if m else ""


async def disparar_detalles(page, respuestas_api: list) -> list[dict]:
    """Hace clic en cada bot y extrae par/dirección/grids desde el modal."""
    botones = page.locator("text=Copiar")
    total = await botones.count()
    print(f"🔎 Obteniendo grids de {total} bots...")
    detalles_modal = []

    for i in range(total):
        try:
            boton = botones.nth(i)
            await boton.scroll_into_view_if_needed()

            # Capturar contexto de la fila visible antes de abrir detalle
            row_text = ""
            par_row = ""
            direccion_row = ""
            roi_row = ""
            try:
                row_text = await boton.evaluate(
                    """
                    (el) => {
                      const row = el.closest('tr')
                        || el.closest('[class*=row]')
                        || el.closest('[class*=table-row]')
                        || el.parentElement;
                      return row ? row.innerText : '';
                    }
                    """
                )
                m_par = re.search(r"\b([A-Z0-9_]{3,}(?:USDT|USDC))\b", row_text)
                if m_par:
                    par_row = m_par.group(1)

                m_dir = re.search(r"\b(Long|Short|Neutral)\b", row_text, flags=re.IGNORECASE)
                if m_dir:
                    direccion_row = m_dir.group(1).capitalize()

                m_roi = re.search(r"(-?\d+(?:\.\d+)?)%", row_text)
                if m_roi:
                    roi_row = _normalizar_roi(m_roi.group(1))
            except Exception:
                pass

            await boton.click(timeout=5000, force=True)

            # Esperar apertura de modal/panel de detalle
            modal_abierto = False
            try:
                await page.get_by_text("Detalles del bot", exact=True).first.wait_for(timeout=3500)
                modal_abierto = True
            except Exception:
                # Fallback: en algunos casos el título tarda o no está visible
                try:
                    await page.get_by_text("Inf. básica", exact=True).last.wait_for(timeout=2500)
                    modal_abierto = True
                except Exception:
                    modal_abierto = False

            if not modal_abierto:
                detalles_modal.append({"par": "", "direccion": "", "grids": ""})
                continue

            await page.wait_for_timeout(600)

            grid_extraido = ""
            texto_modal = ""
            par_modal = ""
            direccion_modal = ""

            # 1) Intento directo: leer valor en la fila "Cantidad de grids / Modo"
            try:
                valor_grids = await page.locator(
                    "xpath=(//div[normalize-space()='Cantidad de grids / Modo']/following-sibling::div[1])[last()]"
                ).inner_text(timeout=2000)
                grid_extraido = _extraer_numero_grids_desde_valor(valor_grids)
            except Exception:
                pass

            # 2) Fallback por texto completo del modal
            modal = page.locator("xpath=(//div[contains(., 'Inf. básica')])[last()]").first
            if await modal.count() > 0:
                try:
                    texto_modal = await modal.inner_text(timeout=1800)
                except Exception:
                    texto_modal = ""

            if not grid_extraido:
                grid_extraido = _extraer_grid_desde_texto_modal(texto_modal)

            if texto_modal:
                m_par = re.search(r"\b([A-Z0-9_]{3,}(?:USDT|USDC))\b", texto_modal)
                if m_par:
                    par_modal = m_par.group(1)

                m_dir = re.search(r"Dirección\s*\n\s*(Long|Short|Neutral)", texto_modal, flags=re.IGNORECASE)
                if m_dir:
                    direccion_modal = m_dir.group(1).capitalize()
                else:
                    m_dir2 = re.search(r"\b(Long|Short|Neutral)\b", texto_modal, flags=re.IGNORECASE)
                    if m_dir2:
                        direccion_modal = m_dir2.group(1).capitalize()

            detalles_modal.append({
                "par": par_modal or par_row,
                "direccion": direccion_modal or direccion_row,
                "roi": roi_row,
                "grids": grid_extraido,
            })

            # Cerrar modal/panel
            cerrado = False
            try:
                close_btn = page.locator("[aria-label='Close'], [class*='close'], [class*='cerrar']").first
                if await close_btn.count() > 0:
                    await close_btn.click(timeout=1200)
                    cerrado = True
            except Exception:
                cerrado = False

            if not cerrado:
                await page.keyboard.press("Escape")

            await page.wait_for_timeout(500)
        except Exception as e:
            print(f"  ⚠️ Bot {i + 1}: {e}")
            detalles_modal.append({"par": "", "direccion": "", "roi": "", "grids": ""})
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(300)

    return detalles_modal


async def capturar_detalles_con_scroll(page, respuestas_api: list, pasadas: int = 4) -> list[dict]:
    """Captura detalles de bots en varias pasadas con scroll para cubrir listas virtualizadas."""
    todos = []
    for n in range(pasadas):
        detalles = await disparar_detalles(page, respuestas_api)
        todos.extend(detalles)
        # avanzar en contenedores scrolleables para que se rendericen otros rows/botones "Copiar"
        await page.evaluate(
            """
            () => {
                const nodes = Array.from(document.querySelectorAll('div'));
                for (const el of nodes) {
                    const style = window.getComputedStyle(el);
                    const scrollable = (style.overflowY === 'auto' || style.overflowY === 'scroll')
                        && el.scrollHeight > el.clientHeight + 20;
                    if (scrollable) {
                        el.scrollBy(0, 900);
                    }
                }
                window.scrollBy(0, 500);
            }
            """
        )
        await page.wait_for_timeout(1200)

    # volver arriba por consistencia visual
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(800)
    return todos


def parsear_detalles_grids(respuestas_api: list) -> dict:
    """Extrae {strategyId: (grids, modo)} de las respuestas de la API de detalle."""
    grids_data = {}
    for resp in respuestas_api:
        if "bapi/algo" not in resp.get("url", ""):
            continue

        data_obj = resp.get("data", {})
        if not isinstance(data_obj, dict):
            continue

        payload = data_obj.get("data", {})
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict) and isinstance(payload.get("list"), list):
            candidates = payload.get("list", [])
        elif isinstance(payload, dict):
            candidates = [payload]
        else:
            candidates = []

        for item in candidates:
            if not isinstance(item, dict):
                continue
            sid = str(item.get("strategyId", item.get("id", "")))
            if not sid:
                continue
            grid_count = (
                item.get("gridCount") or item.get("gridNum") or
                item.get("grid") or item.get("gridNumber") or
                item.get("gridQuantity") or item.get("numberOfGrids") or ""
            )
            if grid_count not in (None, ""):
                grids_data[sid] = str(grid_count)
    return grids_data


def completar_grids_por_indice(df: pd.DataFrame, grids_por_indice: list[str]) -> pd.DataFrame:
    """Completa grids faltantes por orden visual de lista (fallback UI modal)."""
    if df.empty or not grids_por_indice:
        return df

    if "grids" not in df.columns:
        df["grids"] = ""

    limite = min(len(df), len(grids_por_indice))
    for i in range(limite):
        actual = str(df.iloc[i].get("grids", "")).strip()
        if not actual and grids_por_indice[i]:
            df.iat[i, df.columns.get_loc("grids")] = grids_por_indice[i]

    return df


def completar_grids_por_clave(df: pd.DataFrame, detalles_modal: list[dict]) -> pd.DataFrame:
    """Completa grids faltantes usando clave (par, direccion) extraída del modal."""
    if df.empty or not detalles_modal:
        return df

    if "grids" not in df.columns:
        df["grids"] = ""

    lookup_exacta = {}
    lookup_relajada = {}

    for d in detalles_modal:
        par = str(d.get("par", "")).strip().upper()
        direccion = str(d.get("direccion", "")).strip().capitalize()
        roi = _normalizar_roi(d.get("roi", ""))
        grids = str(d.get("grids", "")).strip()
        if par and direccion and roi and grids:
            lookup_exacta[(par, direccion, roi)] = grids
        if par and direccion and grids:
            lookup_relajada.setdefault((par, direccion), set()).add(grids)

    if not lookup_exacta and not lookup_relajada:
        return df

    for i in range(len(df)):
        actual = str(df.iloc[i].get("grids", "")).strip()
        if actual:
            continue
        par = str(df.iloc[i].get("par", "")).strip().upper()
        direccion = str(df.iloc[i].get("direccion", "")).strip().capitalize()
        roi = _normalizar_roi(df.iloc[i].get("roi", ""))

        nuevo = lookup_exacta.get((par, direccion, roi), "")
        if not nuevo:
            candidatos = lookup_relajada.get((par, direccion), set())
            # Solo usar clave relajada si no genera ambigüedad
            if len(candidatos) == 1:
                nuevo = next(iter(candidatos))

        if nuevo:
            df.iat[i, df.columns.get_loc("grids")] = nuevo

    return df


def _buscar_grid_en_obj(obj) -> str:
    """Busca recursivamente un valor de grids en un objeto JSON arbitrario."""
    claves_grid = {
        "gridCount", "gridNum", "grid", "gridNumber",
        "gridQuantity", "numberOfGrids", "grids", "grid_count"
    }

    if isinstance(obj, dict):
        # Prioridad: claves conocidas de grid
        for k, v in obj.items():
            if k in claves_grid:
                if isinstance(v, (int, float)):
                    return str(int(v))
                if isinstance(v, str):
                    m = re.search(r"\d+", v)
                    if m:
                        return m.group(0)

        # Recorrer hijos
        for v in obj.values():
            encontrado = _buscar_grid_en_obj(v)
            if encontrado:
                return encontrado

    elif isinstance(obj, list):
        for item in obj:
            encontrado = _buscar_grid_en_obj(item)
            if encontrado:
                return encontrado

    return ""


async def completar_grids_por_fetch(page, df: pd.DataFrame) -> pd.DataFrame:
    """Completa grids faltantes consultando detalle por strategy_id vía fetch en la sesión actual."""
    if df.empty or "strategy_id" not in df.columns:
        return df

    if "grids" not in df.columns:
        df["grids"] = ""

    faltantes = df[
        df["strategy_id"].astype(str).str.strip().ne("")
        & df["grids"].astype(str).str.strip().eq("")
    ]
    if faltantes.empty:
        return df

    print(f"🌐 Consultando grids por API detalle para {len(faltantes)} bots faltantes...")

    for idx, row in faltantes.iterrows():
        sid = str(row.get("strategy_id", "")).strip()
        if not sid:
            continue

        try:
            payload = await page.evaluate(
                """
                async ({sid}) => {
                  try {
                    const r = await fetch('https://www.binance.com/bapi/algo/v1/friendly/algo/public/strategy/detail', {
                      method: 'POST',
                      credentials: 'include',
                      headers: {
                        'content-type': 'application/json',
                        'clienttype': 'web',
                        'lang': 'es-LA'
                      },
                      body: JSON.stringify({ strategyId: sid })
                    });
                    return await r.json();
                  } catch (e) {
                    return { error: String(e) };
                  }
                }
                """,
                {"sid": sid},
            )

            # búsqueda robusta en cualquier estructura de respuesta
            grid_count = _buscar_grid_en_obj(payload)
            if grid_count:
                df.at[idx, "grids"] = str(grid_count)
                continue

            # fallback parse texto serializado si la estructura cambia
            texto = json.dumps(payload, ensure_ascii=False) if isinstance(payload, dict) else str(payload)
            m = re.search(r'"(?:gridCount|gridNum|gridNumber|gridQuantity|numberOfGrids)"\s*:\s*"?(\d+)"?', texto)
            if m:
                df.at[idx, "grids"] = m.group(1)
        except Exception:
            continue

        await page.wait_for_timeout(120)

    return df


def parsear_texto_modal(detalles: list) -> list[dict]:
    """Parsea el texto extraído de los modales de detalle."""
    bots_parseados = []

    for detalle in detalles:
        texto = detalle.get("texto_modal", "")
        lineas = [l.strip() for l in texto.split("\n") if l.strip()]

        bot = {"bot_index": detalle["bot_index"]}

        for i, linea in enumerate(lineas):
            linea_lower = linea.lower()
            # Par y tipo
            if "usdt" in linea_lower and i < 3:
                bot["par"] = linea.split()[0] if linea.split() else linea

            # Valores numéricos con etiquetas
            if "roi" in linea_lower:
                for l in lineas[max(0, i-1):i+2]:
                    if "%" in l and "roi" not in l.lower():
                        bot["roi"] = l.strip()
            if "pnl" in linea_lower:
                for l in lineas[max(0, i-1):i+2]:
                    if "$" in l or "+" in l or "-" in l:
                        bot["pnl"] = l.strip()
            if "duración" in linea_lower or "duration" in linea_lower:
                if i + 1 < len(lineas):
                    bot["duracion"] = lineas[i + 1] if "día" in lineas[i + 1].lower() or "d" in lineas[i + 1].lower() else linea.split(":")[-1].strip() if ":" in linea else ""
            if "mdd" in linea_lower:
                for l in lineas[max(0, i-1):i+2]:
                    if "%" in l and "mdd" not in l.lower():
                        bot["mdd_7d"] = l.strip()
            if "dirección" in linea_lower or "direction" in linea_lower:
                if i + 1 < len(lineas):
                    bot["direccion"] = lineas[i + 1]
            if "rango" in linea_lower or "range" in linea_lower:
                if i + 1 < len(lineas):
                    bot["rango_precios"] = lineas[i + 1]
            if "grid" in linea_lower and ("cantidad" in linea_lower or "count" in linea_lower or "mode" in linea_lower or "modo" in linea_lower):
                if i + 1 < len(lineas):
                    bot["grids"] = lineas[i + 1]
            if "ganancia/grid" in linea_lower or "profit/grid" in linea_lower:
                if i + 1 < len(lineas):
                    bot["ganancia_grid"] = lineas[i + 1]
            if "inversión" in linea_lower or "investment" in linea_lower:
                for l in lineas[max(0, i-1):i+2]:
                    if "usdt" in l.lower():
                        bot["inversion_min"] = l.strip()

        if len(bot) > 1:
            bots_parseados.append(bot)

    return bots_parseados


def filtrar_resultados(df: pd.DataFrame, args) -> pd.DataFrame:
    """Filtra el DataFrame según los argumentos de línea de comandos."""
    if df.empty:
        return df

    df_filtrado = df.copy()

    # Filtrar por inversión máxima
    if args.inversion_max is not None and "inversion_min" in df_filtrado.columns:
        df_filtrado["_inv_num"] = pd.to_numeric(
            df_filtrado["inversion_min"].astype(str).str.replace(r"[^\d.]", "", regex=True),
            errors="coerce"
        )
        df_filtrado = df_filtrado[df_filtrado["_inv_num"] <= args.inversion_max]
        df_filtrado = df_filtrado.drop(columns=["_inv_num"])
        print(f"🔍 Filtro inversión máx: ≤ {args.inversion_max} USDT → {len(df_filtrado)} bots")

    # Filtrar por dirección
    if args.direccion is not None and "direccion" in df_filtrado.columns:
        dir_filter = args.direccion.capitalize()
        df_filtrado = df_filtrado[
            df_filtrado["direccion"].astype(str).str.capitalize() == dir_filter
        ]
        print(f"🔍 Filtro dirección: {dir_filter} → {len(df_filtrado)} bots")

    # Filtrar por copias
    if (args.copias_min is not None or args.copias_max is not None) and "copias" in df_filtrado.columns:
        df_filtrado["_copias_num"] = pd.to_numeric(
            df_filtrado["copias"].astype(str).str.replace(r"[^\d]", "", regex=True),
            errors="coerce"
        )
        if args.copias_min is not None:
            df_filtrado = df_filtrado[df_filtrado["_copias_num"] >= args.copias_min]
            print(f"🔍 Filtro copias mín: ≥ {args.copias_min} → {len(df_filtrado)} bots")
        if args.copias_max is not None:
            df_filtrado = df_filtrado[df_filtrado["_copias_num"] <= args.copias_max]
            print(f"🔍 Filtro copias máx: ≤ {args.copias_max} → {len(df_filtrado)} bots")
        df_filtrado = df_filtrado.drop(columns=["_copias_num"], errors="ignore")

    return df_filtrado.reset_index(drop=True)


def mostrar_resultados(df: pd.DataFrame, titulo: str = ""):
    """Muestra los resultados en formato tabla en la consola."""
    if df.empty:
        print("❌ No se obtuvieron datos")
        return

    # Columnas visibles en orden fijo, strategy_id primero
    columnas_orden = ["strategy_id", "par", "direccion", "roi", "pnl", "duracion", "inversion_min", "copias"]
    cols_visibles = [c for c in columnas_orden if c in df.columns]

    df_mostrar = df[cols_visibles] if cols_visibles else df

    print(f"\n{'='*100}")
    if titulo:
        print(f"  {titulo}")
    print(f"  Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Total bots: {len(df_mostrar)}")
    print(f"{'='*100}\n")

    print(tabulate(df_mostrar, headers="keys", tablefmt="fancy_grid", showindex=False))
    print()


def cargar_snapshot_anterior(path: Path) -> pd.DataFrame:
    """Carga el último snapshot guardado en el CSV histórico."""
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, dtype={"strategy_id": str})
        if "timestamp" in df.columns:
            df = df.sort_values("timestamp").groupby("strategy_id", as_index=False).last()
        return df
    except Exception as e:
        print(f"⚠️  No se pudo cargar historial: {e}")
        return pd.DataFrame()


def guardar_snapshot(df: pd.DataFrame, path: Path):
    """Guarda el snapshot actual en el CSV histórico (append)."""
    if df.empty:
        return
    df_guardar = df.copy()
    now = datetime.now()
    df_guardar["fecha"] = now.strftime("%Y-%m-%d")
    df_guardar["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
    escribir_header = not path.exists()
    df_guardar.to_csv(path, mode="a", index=False, header=escribir_header)
    print(f"💾 Snapshot guardado en {path.name} ({len(df_guardar)} bots, {df_guardar['timestamp'].iloc[0]})")





async def main():
    args = parse_args()

    print("🚀 Iniciando scraper del Mercado de Bots de Binance...")
    print(f"   URL: {URL_BOTS}")
    print(f"   Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   Filtro duración usado: 1-7 días")
    print(f"   Páginas máximas a recorrer: {args.paginas_max}")
    if args.inversion_max:
        print(f"   Filtro inversión máx: ≤ {args.inversion_max} USDT")
    if args.direccion:
        print(f"   Filtro dirección: {args.direccion.capitalize()}")
    if args.copias_min is not None:
        print(f"   Filtro copias mín: ≥ {args.copias_min}")
    if args.copias_max is not None:
        print(f"   Filtro copias máx: ≤ {args.copias_max}")
    if args.solo_cambios:
        print("   Mostrando: solo bots con cambios en copias")
    print()

    respuestas_api = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="es-LA",
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        await optimizar_rendimiento(context)
        page = await context.new_page()

        # Paso 1: Configurar interceptor de respuestas API
        await interceptar_respuestas(page, respuestas_api)

        # Paso 2: Navegar a Grid de futuros
        await navegar_a_grid_futuros(page)

        print(f"📡 Respuestas API capturadas durante carga: {len(respuestas_api)}")

        # Paso 3: Intentar cambiar a vista de lista
        await cambiar_a_vista_lista(page)

        # Paso 4: Scroll ligero para asegurar carga inicial
        await page.evaluate("window.scrollBy(0, 700)")
        await page.wait_for_timeout(700)

        print(f"📡 Respuestas API capturadas tras scroll: {len(respuestas_api)}")

        # Paso 5: Recorrer paginación (más bots)
        paginas_cargadas = 1
        for _ in range(max(0, args.paginas_max - 1)):
            antes = len(respuestas_api)
            ok = await ir_a_siguiente_pagina(page)
            if not ok:
                break

            paginas_cargadas += 1
            await page.wait_for_timeout(1200)
            # pequeño scroll para disparar render/listado
            await page.evaluate("window.scrollBy(0, 300)")
            await page.wait_for_timeout(500)

            # si no hubo respuestas nuevas, salir para no forzar CPU
            if len(respuestas_api) == antes:
                break

        print(f"📄 Páginas recorridas: {paginas_cargadas}")

        # Paso 6: Parsear lista de bots
        df_api = parsear_respuestas_api(respuestas_api)


        # Paso 7: Filtrar, quitar columnas ocultas y mostrar (NO eliminar strategy_id)
        df_api = df_api.drop(columns=["grids", "≤1 día"], errors="ignore")
        df_api = filtrar_resultados(df_api, args)

        if not df_api.empty:
            # Paso 8: Guardar snapshot actual
            guardar_snapshot(df_api, HISTORIAL_CSV)

            # Paso 9: Mostrar resultados
            mostrar_resultados(df_api, "📊 MERCADO DE BOTS — GRID DE FUTUROS (1-7 días)")
        else:
            print("⚠️  No se capturaron datos de la API")

        await browser.close()

    print("\n✅ Scraping completado")


if __name__ == "__main__":
    asyncio.run(main())

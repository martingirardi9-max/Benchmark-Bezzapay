#!/usr/bin/env python3
"""
scraper.py — Benchmark Bezza Pay
Corre cada lunes 9AM via GitHub Actions.
Visita páginas de aranceles de cada agrupador,
compara con Supabase y registra cambios.
"""

import os, json, re, time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]  # service role key (secret)

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "es-AR,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── PÁGINAS A MONITOREAR ──────────────────────────────────────────
# Cada entrada tiene: agrupador_id, url, función de parsing
TARGETS = [
    {"id": 0, "nombre": "Mercado Pago",  "url": "https://www.mercadopago.com.ar/ayuda/recibir-pagos-costos_220"},
    {"id": 1, "nombre": "Ualá Bis",      "url": "https://www.ualabis.com.ar/tarifas"},
    {"id": 2, "nombre": "Getnet",        "url": "https://www.getnet.com.ar/tarifas"},
    {"id": 3, "nombre": "NX Toque",      "url": "https://www.naranjax.com/personas/cobrar"},
    {"id": 4, "nombre": "Nave",          "url": "https://www.nave.com.ar/tarifas"},
    {"id": 5, "nombre": "+Pagos Nación", "url": "https://www.bna.com.ar/Personas/PagosBNA"},
    {"id": 6, "nombre": "Bezza Pay",     "url": "https://www.bezzapay.com.ar/comisiones"},
    {"id": 7, "nombre": "Viumi",         "url": "https://www.viumi.com.ar/tarifas"},
]

# ── SUPABASE HELPERS ──────────────────────────────────────────────
def sb_get(table, params=""):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{params}", headers=HEADERS_SB)
    r.raise_for_status()
    return r.json()

def sb_update(table, row_id, data):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}",
        headers=HEADERS_SB,
        json=data
    )
    r.raise_for_status()

def sb_insert_cambio(agrupador_id, campo, valor_anterior, valor_nuevo):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/cambios",
        headers=HEADERS_SB,
        json={
            "agrupador_id": agrupador_id,
            "tabla": "aranceles",
            "campo": campo,
            "valor_anterior": str(valor_anterior),
            "valor_nuevo": str(valor_nuevo),
            "detectado_at": datetime.utcnow().isoformat()
        }
    )
    r.raise_for_status()

# ── PARSERS POR AGRUPADOR ─────────────────────────────────────────
def parse_mercadopago(soup):
    """Extrae aranceles de la página de costos de Mercado Pago."""
    results = {}
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                texto = cells[0].get_text(strip=True).lower()
                valor_txt = cells[1].get_text(strip=True)
                match = re.search(r"(\d+[,.]?\d*)\s*%", valor_txt)
                if match:
                    val = float(match.group(1).replace(",", "."))
                    results[texto] = val
    return results

def parse_generic(soup):
    """Parser genérico: busca patrones de porcentaje en la página."""
    results = {}
    # Buscar en tablas
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            text_cells = [c.get_text(strip=True) for c in cells]
            for i, cell in enumerate(text_cells):
                match = re.search(r"(\d+[,.]?\d*)\s*%", cell)
                if match and i > 0:
                    key = text_cells[i-1].lower()[:50]
                    val = float(match.group(1).replace(",", "."))
                    results[key] = val
    return results

PARSERS = {
    0: parse_mercadopago,
    1: parse_generic,
    2: parse_generic,
    3: parse_generic,
    4: parse_generic,
    5: parse_generic,
    6: parse_generic,
    7: parse_generic,
}

# ── COMPARAR CON SUPABASE ─────────────────────────────────────────
def check_for_changes(agrupador_id, nombre, scraped_data):
    """
    Compara datos scrapeados con lo que hay en Supabase.
    Si detecta diferencias, registra un cambio en la tabla cambios.
    """
    if not scraped_data:
        print(f"  ⚠ Sin datos scrapeados para {nombre}")
        return

    # Obtener aranceles actuales de Supabase
    rows = sb_get("aranceles", f"agrupador_id=eq.{agrupador_id}&select=id,tipo,plazo,valor,medio")

    cambios_detectados = 0
    for row in rows:
        if row["valor"] is None:
            continue
        # Buscar coincidencia aproximada en los datos scrapeados
        plazo_key = row["plazo"].lower()
        tipo_key = row["tipo"].lower()
        search_key = f"{tipo_key}.*{plazo_key}"

        for scraped_key, scraped_val in scraped_data.items():
            if re.search(plazo_key[:10], scraped_key):
                supabase_val = float(row["valor"])
                if abs(scraped_val - supabase_val) > 0.05:  # tolerancia 0.05%
                    print(f"  🔔 CAMBIO en {nombre}: {row['tipo']} {row['plazo']} "
                          f"{supabase_val}% → {scraped_val}%")
                    # Actualizar en Supabase
                    sb_update("aranceles", row["id"], {"valor": scraped_val})
                    # Registrar cambio
                    sb_insert_cambio(
                        agrupador_id,
                        f"{row['medio']} · {row['tipo']} · {row['plazo']}",
                        f"{supabase_val}%",
                        f"{scraped_val}%"
                    )
                    cambios_detectados += 1
                break

    if cambios_detectados == 0:
        print(f"  ✓ Sin cambios en {nombre}")
    else:
        print(f"  ⚡ {cambios_detectados} cambio(s) detectado(s) en {nombre}")

# ── MAIN ──────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*50}")
    print(f"Scraper Benchmark Bezza Pay — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*50}\n")

    for target in TARGETS:
        print(f"→ Procesando {target['nombre']}...")
        try:
            resp = requests.get(target["url"], headers=HEADERS_WEB, timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            parser = PARSERS.get(target["id"], parse_generic)
            scraped = parser(soup)
            print(f"  Encontrados {len(scraped)} valores en la página")
            check_for_changes(target["id"], target["nombre"], scraped)
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error al acceder a {target['nombre']}: {e}")
        except Exception as e:
            print(f"  ✗ Error procesando {target['nombre']}: {e}")

        time.sleep(2)  # pausa entre requests

    print(f"\n{'='*50}")
    print("Scraper finalizado.")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()

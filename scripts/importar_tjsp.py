"""
scripts/importar_tjsp.py — Importa ementas de jurisprudência pública do TJSP.

Estratégia de importação (em ordem de preferência):
    1. Selenium + e-SAJ CJSG (ementas completas — requer Chrome/chromedriver)
    2. DataJud API (metadados — sempre funciona, sem ementa)

O e-SAJ do TJSP usa PrimeFaces (JSF) que exige JavaScript para renderizar
resultados de busca. Sem Selenium, usamos apenas DataJud.

Uso:
    python scripts/importar_tjsp.py --modo datajud --limite 500
    python scripts/importar_tjsp.py --modo selenium --todos-temas --limite 200
    python scripts/importar_tjsp.py --modo selenium --assunto "plano de saude autismo"
"""

import argparse
import hashlib
import html as html_mod
import json
import logging
import os
import re
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from banco import inicializar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Temas → termos de busca
TEMAS_BUSCA = {
    "TEA/Autismo": '"plano de saúde" E "autismo"',
    "Oncologia": '"plano de saúde" E "quimioterapia"',
    "Home care": '"plano de saúde" E "home care"',
    "Cobertura de medicamento": '"plano de saúde" E "medicamento" E "cobertura"',
    "Cobertura de procedimento": '"plano de saúde" E "procedimento" E "negativa"',
    "Reajuste/mensalidade": '"plano de saúde" E "reajuste"',
    "Rescisao/cancelamento": '"plano de saúde" E "rescisão" E "contrato"',
    "Transplante": '"plano de saúde" E "transplante"',
    "Saude mental": '"plano de saúde" E "psiquiatria"',
    "Urgencia/emergencia": '"plano de saúde" E "urgência" E "cobertura"',
    "Reembolso": '"plano de saúde" E "reembolso"',
    "Ortopedia/protese": '"plano de saúde" E "prótese"',
    "Cobertura contratual": '"plano de saúde" E "contratual" E "exclusão"',
}

# ================================================================
# MODO SELENIUM (ementas completas via e-SAJ CJSG)
# ================================================================

def _check_selenium():
    """Verifica se Selenium está disponível."""
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        return True
    except ImportError:
        return False


def importar_selenium(tema, termo_busca, limite=200, conn=None):
    """Importa ementas via Selenium + e-SAJ."""
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    log.info(f"\n--- Selenium: {tema} ---")
    log.info(f"  Busca: {termo_busca}")

    close_conn = False
    if conn is None:
        conn = inicializar()
        close_conn = True

    importados = 0
    duplicados = 0

    # Chrome headless
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    driver = None
    try:
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(30)

        BASE = "https://esaj.tjsp.jus.br/cjsg"

        # 1. Acessar formulário
        log.info("  Acessando formulário e-SAJ...")
        driver.get(f"{BASE}/consultaCompleta.do")
        time.sleep(2)

        # 2. Preencher busca
        campo_busca = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "iddados.buscaInteiroTeor"))
        )
        campo_busca.clear()
        campo_busca.send_keys(termo_busca)

        # Marcar apenas acórdãos
        try:
            checkbox_a = driver.find_element(By.CSS_SELECTOR,
                "input[value='A'][name='tipoDecisaoSelecionados']")
            if not checkbox_a.is_selected():
                checkbox_a.click()
        except:
            pass

        # 3. Clicar em pesquisar
        btn = driver.find_element(By.ID, "pbSubmit")
        btn.click()

        # 4. Esperar resultados
        time.sleep(3)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".fundocinza1, .resultadoEmenta, .esajResultadoEmenta"))
        )

        pagina = 1
        max_paginas = limite // 20 + 1

        while pagina <= max_paginas and importados < limite:
            log.info(f"  Página {pagina}...")

            # Parsear resultados da página atual
            html_content = driver.page_source
            acordaos = _parsear_html_selenium(html_content)

            if not acordaos:
                log.info(f"  Sem resultados na página {pagina}")
                break

            for ac in acordaos:
                hash_em = hashlib.md5(
                    (ac.get("ementa", "") or ac["numero_processo"]).encode("utf-8")
                ).hexdigest()

                tema_class = _classificar_tema(ac.get("ementa", "")) or tema

                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO acordaos
                        (numero_processo, tribunal, orgao_julgador, relator,
                         data_julgamento, data_publicacao, classe_processual,
                         ementa, tema, fonte, hash_ementa)
                        VALUES (?, 'TJSP', ?, ?, ?, ?, ?, ?, ?, 'tjsp_esaj', ?)
                    """, (
                        ac["numero_processo"], ac.get("orgao_julgador", ""),
                        ac.get("relator", ""), ac.get("data_julgamento", ""),
                        ac.get("data_publicacao", ""), ac.get("classe_processual", ""),
                        ac.get("ementa", ""), tema_class, hash_em
                    ))

                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        importados += 1
                    else:
                        duplicados += 1
                except Exception as e:
                    log.warning(f"  Erro inserindo: {e}")
                    duplicados += 1

            conn.commit()
            log.info(f"  +{len(acordaos)} encontrados, {importados} novos")

            # Próxima página
            pagina += 1
            if pagina <= max_paginas:
                try:
                    # Clicar no link da próxima página
                    next_link = driver.find_element(
                        By.XPATH, f"//a[contains(@onclick, 'pagina={pagina}')]"
                    )
                    next_link.click()
                    time.sleep(3)
                except:
                    log.info("  Sem mais páginas")
                    break

    except Exception as e:
        log.error(f"  Erro Selenium: {e}")
    finally:
        if driver:
            driver.quit()
        if close_conn:
            conn.close()

    log.info(f"  {tema}: {importados} importados, {duplicados} duplicados")
    return importados, duplicados


def _parsear_html_selenium(html_content):
    """Extrai acórdãos do HTML renderizado pelo Selenium."""
    acordaos = []

    # Buscar blocos de resultado
    # O e-SAJ usa tabelas com classe fundocinza1
    blocos = re.split(r'(?=\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})', html_content)

    for block in blocos:
        proc_match = re.search(r'(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})', block)
        if not proc_match:
            continue

        numero = proc_match.group(1)
        block_clean = re.sub(r'<[^>]+>', ' ', block)
        block_clean = html_mod.unescape(block_clean)

        # Extrair campos
        classe = _extrair_campo(block_clean, r'Classe[^:]*:\s*(.+?)(?:\n|Relator|Comarca)')
        relator = _extrair_campo(block_clean, r'Relator[^:]*:\s*(.+?)(?:\n|Comarca|Data|Ementa)')
        orgao = _extrair_campo(block_clean, r'[ÓO]rg[aã]o\s+[Jj]ulgador[^:]*:\s*(.+?)(?:\n|Comarca|Data|Relator)')
        data_julg = _extrair_campo(block_clean, r'Data\s+(?:do\s+)?[Jj]ulgamento[^:]*:\s*(\d{2}/\d{2}/\d{4})')
        data_pub = _extrair_campo(block_clean, r'Data\s+(?:de\s+)?[Pp]ublica[^:]*:\s*(\d{2}/\d{2}/\d{4})')

        # Ementa: texto entre "Ementa:" e o próximo campo
        ementa = _extrair_campo(block_clean,
            r'[Ee]menta[^:]*:\s*(.+?)(?:Relator|[ÓO]rg[aã]o|Classe|Data\s+d[oe]|$)')

        # Normalizar datas
        data_julg = _normalizar_data(data_julg)
        data_pub = _normalizar_data(data_pub)

        if numero and (ementa or classe):
            acordaos.append({
                "numero_processo": numero,
                "classe_processual": (classe or "").strip(),
                "relator": (relator or "").strip(),
                "orgao_julgador": (orgao or "").strip(),
                "data_julgamento": data_julg,
                "data_publicacao": data_pub,
                "ementa": (ementa or "").strip()[:5000],
            })

    return acordaos


def _extrair_campo(texto, pattern):
    """Extrai um campo via regex."""
    m = re.search(pattern, texto, re.DOTALL)
    return m.group(1).strip() if m else ""


def _normalizar_data(data_str):
    """DD/MM/YYYY → YYYY-MM-DD."""
    if not data_str:
        return ""
    m = re.match(r'(\d{2})/(\d{2})/(\d{4})', data_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return data_str


# ================================================================
# MODO DATAJUD (metadados — sempre funciona)
# ================================================================

def importar_datajud_tjsp(limite=500, conn=None):
    """Importa processos TJSP via DataJud (fallback sem Selenium)."""
    log.info("Usando DataJud como fonte para TJSP (sem ementas)...")
    log.info("Para ementas completas, use --modo selenium")

    # Importar diretamente do script datajud
    from scripts.importar_datajud import importar_tribunal
    return importar_tribunal("TJSP", limite=limite, conn=conn)


# ================================================================
# CLASSIFICAÇÃO DE TEMA
# ================================================================

def _classificar_tema(ementa):
    """Classifica tema baseado na ementa."""
    if not ementa:
        return None

    ementa_up = ementa.upper()

    mapa = {
        "TEA/Autismo": ["AUTIS", "TEA", "ESPECTRO AUTISTA", "TERAPIA ABA"],
        "Oncologia": ["QUIMIOTERAPIA", "RADIOTERAPIA", "ONCOLOG", "CANCER", "NEOPLASIA"],
        "Home care": ["HOME CARE", "INTERNACAO DOMICILIAR", "INTERNAÇÃO DOMICILIAR"],
        "Cobertura de medicamento": ["MEDICAMENTO", "FORNECIMENTO DE MEDICAMENTO", "OFF LABEL"],
        "Reajuste/mensalidade": ["REAJUSTE", "MENSALIDADE", "FAIXA ETARIA", "FAIXA ETÁRIA"],
        "Rescisao/cancelamento": ["RESCISAO", "RESCISÃO", "CANCELAMENTO DO PLANO"],
        "Transplante": ["TRANSPLANTE"],
        "Saude mental": ["PSIQUIATR", "DEPENDENCIA QUIMICA", "DEPENDÊNCIA QUÍMICA"],
        "Urgencia/emergencia": ["URGENCIA", "URGÊNCIA", "EMERGENCIA", "EMERGÊNCIA"],
        "Reembolso": ["REEMBOLSO"],
        "Ortopedia/protese": ["PROTESE", "PRÓTESE", "ORTESE", "ÓRTESE", "IMPLANTE"],
    }

    for tema, termos in mapa.items():
        if any(t in ementa_up for t in termos):
            return tema

    if "PLANO DE SAUDE" in ementa_up or "PLANO DE SAÚDE" in ementa_up:
        if "COBERTURA" in ementa_up:
            return "Cobertura de procedimento"
        return "Cobertura contratual"

    return None


# ================================================================
# IMPORTAÇÃO EM LOTE
# ================================================================

def importar_todos_temas_selenium(limite_por_tema=200):
    """Importa ementas de todos os temas via Selenium."""
    if not _check_selenium():
        log.error("Selenium não instalado! Use: pip install selenium")
        log.info("Alternativa: python scripts/importar_tjsp.py --modo datajud")
        return

    conn = inicializar()
    conn.execute("""
        INSERT INTO log_importacao (fonte, tribunal, data_inicio, status)
        VALUES ('tjsp_esaj', 'TJSP', datetime('now'), 'em_andamento')
    """)
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    total_imp = 0
    total_dup = 0

    try:
        for tema, termo in TEMAS_BUSCA.items():
            try:
                imp, dup = importar_selenium(tema, termo, limite=limite_por_tema, conn=conn)
                total_imp += imp
                total_dup += dup
            except Exception as e:
                log.error(f"Erro no tema {tema}: {e}")
            time.sleep(5)

        conn.execute("""
            UPDATE log_importacao
            SET data_fim=datetime('now'), registros_importados=?,
                registros_duplicados=?, status='concluido'
            WHERE id=?
        """, (total_imp, total_dup, log_id))
        conn.commit()
    except Exception as e:
        conn.execute("UPDATE log_importacao SET status='erro', erro=? WHERE id=?",
                     (str(e), log_id))
        conn.commit()
    finally:
        conn.close()

    log.info(f"\n=== TJSP e-SAJ (Selenium) ===")
    log.info(f"  Total importados: {total_imp}")
    log.info(f"  Total duplicados: {total_dup}")


def main():
    parser = argparse.ArgumentParser(description="Importar jurisprudência TJSP")
    parser.add_argument("--modo", choices=["selenium", "datajud"], default="datajud",
                        help="Modo de importação (default: datajud)")
    parser.add_argument("--assunto", help="Termo de busca livre (modo selenium)")
    parser.add_argument("--tema", help="Tema específico (modo selenium)")
    parser.add_argument("--todos-temas", action="store_true", help="Todos os temas")
    parser.add_argument("--limite", type=int, default=200, help="Limite (default: 200)")
    args = parser.parse_args()

    if args.modo == "selenium":
        if not _check_selenium():
            log.error("Selenium não disponível!")
            log.info("Instale: pip install selenium")
            log.info("Ou use: --modo datajud")
            return

        if args.todos_temas:
            importar_todos_temas_selenium(limite_por_tema=args.limite)
        elif args.tema and args.tema in TEMAS_BUSCA:
            importar_selenium(args.tema, TEMAS_BUSCA[args.tema], limite=args.limite)
        elif args.assunto:
            importar_selenium("Busca livre", args.assunto, limite=args.limite)
        else:
            log.info("Teste com TEA/Autismo...")
            importar_selenium("TEA/Autismo", TEMAS_BUSCA["TEA/Autismo"],
                            limite=min(args.limite, 20))
    else:
        # Modo DataJud
        importar_datajud_tjsp(limite=args.limite)


if __name__ == "__main__":
    main()

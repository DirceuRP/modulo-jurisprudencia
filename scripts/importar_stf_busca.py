"""
scripts/importar_stf_busca.py — Importa decisoes do STF via portal jurisprudencia.stf.jus.br

Estrategia: Selenium acessa busca, expande espelhos, extrai ementa.
Foco: Reclamacoes constitucionais, ADIs, ADCs, ADPFs sobre saude suplementar.

Uso:
    python scripts/importar_stf_busca.py --termo "Hapvida" --limite 30
    python scripts/importar_stf_busca.py --termo "rol ANS" --limite 50
"""
import argparse
import hashlib
import logging
import os
import re
import sys
import time

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from banco import inicializar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

URL_BUSCA = "https://jurisprudencia.stf.jus.br/pages/search?base=acordaos&queryString={termo}&sort=_score&sortBy=desc"


def _criar_driver():
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--ignore-ssl-errors=yes")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36")
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver


def _expandir_espelhos(driver):
    """Clica no botao 'Expandir espelho' de todos os resultados visiveis."""
    from selenium.webdriver.common.by import By
    botoes = driver.find_elements(By.CSS_SELECTOR, '[mattooltip="Expandir espelho"], [mattooltip="Mostrar inteiro teor"]')
    log.info(f"  Botoes 'Expandir' encontrados: {len(botoes)}")
    for i, btn in enumerate(botoes):
        try:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.2)
        except Exception:
            pass
    time.sleep(2)
    return len(botoes)


def _parsear_resultados(html):
    """Extrai resultados da busca STF (Angular SPA)."""
    resultados = []
    vistos = set()

    # Padrao do titulo: <h4 class="ng-star-inserted">Rcl 84287 AgR-ED</h4>
    # ou: <h4 class="ng-star-inserted">RE 597064</h4>
    # ou: <h4 class="ng-star-inserted">ADI 7265</h4>
    pattern_h4 = re.compile(
        r'<h4[^>]*class="[^"]*ng-star-inserted"[^>]*>'
        r'((?:Rcl|RE|ADI|ADC|ADPF|ARE|HC|AgR|AC|AI|MS|RHC)[^<]+)'
        r'</h4>',
        re.IGNORECASE,
    )

    # Encontrar todos os blocos
    h4_matches = list(pattern_h4.finditer(html))
    log.info(f"  Resultados encontrados: {len(h4_matches)}")

    for m in h4_matches:
        titulo = m.group(1).strip()

        # Extrair classe e numero
        cls_match = re.match(r"(Rcl|RE|ADI|ADC|ADPF|ARE|HC|AgR|AC|AI|MS|RHC)\s+(\d+)", titulo, re.IGNORECASE)
        if not cls_match:
            continue
        classe = cls_match.group(1).upper()
        numero = cls_match.group(2)

        chave = f"{classe}-{numero}"
        if chave in vistos:
            continue
        vistos.add(chave)

        # Pegar contexto apos o h4 — relator, ementa, data
        contexto_inicio = m.end()
        contexto_fim = min(len(html), contexto_inicio + 8000)
        contexto = html[contexto_inicio:contexto_fim]

        # Relator: buscar padrao "Relator: NOME"
        relator = ""
        rel_match = re.search(r"Relator(?:\(a\))?[:\s]+([A-ZÀÉÍÓÚ][A-Za-zÀ-ÿ\s\.]+?)(?:</|<|\n|Julgamento)",
                              contexto[:2000])
        if rel_match:
            relator = rel_match.group(1).strip()[:100]

        # Data julgamento
        data = ""
        data_match = re.search(r"(\d{2}/\d{2}/\d{4})", contexto[:3000])
        if data_match:
            d = data_match.group(1)
            data = f"{d[6:10]}-{d[3:5]}-{d[0:2]}"

        # Ementa: busca por trecho longo apos espelho expandido
        ementa = ""
        # Tentar capturar texto dentro de divs apos h4 (pode estar em mat-card-content ou similar)
        em_match = re.search(r'<div[^>]*class="[^"]*(?:ementa|inteiro)[^"]*"[^>]*>(.*?)</div>', contexto, re.DOTALL)
        if em_match:
            ementa = _limpar_html(em_match.group(1))

        if not ementa:
            # Fallback: pegar texto plano (200-2000 chars)
            texto_plano = re.sub(r"<[^>]+>", " ", contexto[:5000])
            texto_plano = re.sub(r"\s+", " ", texto_plano).strip()
            if len(texto_plano) > 200:
                ementa = texto_plano[:3000]

        resultados.append({
            "classe": classe,
            "numero": numero,
            "titulo": titulo,
            "relator": relator,
            "data_julgamento": data,
            "ementa": ementa,
        })

    return resultados


def _limpar_html(texto):
    if not texto:
        return ""
    import html as html_mod
    texto = html_mod.unescape(texto)
    texto = re.sub(r"<br\s*/?>", "\n", texto)
    texto = re.sub(r"<[^>]+>", " ", texto)
    texto = re.sub(r"&nbsp;", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def importar_busca(termo, limite=20, max_paginas=3, conn=None):
    log.info(f"\n{'='*60}\n  STF - Termo: {termo}\n{'='*60}")
    close = conn is None
    if conn is None:
        conn = inicializar()

    importados = 0
    duplicados = 0
    driver = None

    try:
        driver = _criar_driver()
        from urllib.parse import quote
        url = URL_BUSCA.format(termo=quote(termo))
        log.info(f"  Acessando STF...")
        driver.get(url)
        # Aguardar Angular renderizar todos os resultados (h4 com Rcl/RE/etc.)
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'h4.ng-star-inserted'))
            )
            time.sleep(5)  # margem extra para todos renderizarem
        except Exception as e:
            log.warning(f"  Timeout aguardando Angular: {e}")
            time.sleep(15)
        log.info(f"  TITLE: {driver.title}")
        # Verificar quantos h4 ha agora
        h4s = driver.find_elements(By.CSS_SELECTOR, 'h4.ng-star-inserted')
        log.info(f"  h4 elementos visiveis: {len(h4s)}")

        pagina = 1
        while pagina <= max_paginas and importados < limite:
            log.info(f"  Pagina {pagina}...")

            # Pegar metadados via Selenium ANTES de expandir
            from selenium.webdriver.common.by import By
            cards = driver.find_elements(By.CSS_SELECTOR, 'h4.ng-star-inserted')
            log.info(f"  Cards: {len(cards)}")
            metadados = []
            for c in cards:
                try:
                    titulo = c.text.strip()
                    if titulo and re.match(r"(Rcl|RE|ADI|ADC|ADPF|ARE|HC|AC|AI|MS|RHC)\s+\d+", titulo, re.IGNORECASE):
                        metadados.append(titulo)
                except Exception:
                    pass

            # Agora expandir todos para pegar ementas
            _expandir_espelhos(driver)
            html = driver.page_source

            # Construir resultados a partir dos metadados
            resultados = []
            for titulo in metadados:
                m = re.match(r"(Rcl|RE|ADI|ADC|ADPF|ARE|HC|AC|AI|MS|RHC)\s+(\d+)", titulo, re.IGNORECASE)
                if not m:
                    continue
                classe = m.group(1).upper()
                numero = m.group(2)

                # Pegar contexto apos esse titulo no HTML
                escaped = re.escape(titulo)
                ctx_match = re.search(escaped + r"</h4>(.{0,5000})", html, re.DOTALL)
                ementa = ""
                relator = ""
                data = ""
                if ctx_match:
                    ctx = ctx_match.group(1)
                    txt = re.sub(r"<[^>]+>", " ", ctx)
                    txt = re.sub(r"\s+", " ", txt).strip()
                    if len(txt) > 100:
                        ementa = txt[:3000]
                    rel = re.search(r"Relator(?:\(a\))?[:\s]+([^|\n<]{5,80})", txt[:1000])
                    if rel:
                        relator = rel.group(1).strip()
                    dm = re.search(r"(\d{2}/\d{2}/\d{4})", txt[:2000])
                    if dm:
                        d = dm.group(1)
                        data = f"{d[6:10]}-{d[3:5]}-{d[0:2]}"

                resultados.append({
                    "classe": classe,
                    "numero": numero,
                    "titulo": titulo,
                    "relator": relator,
                    "data_julgamento": data,
                    "ementa": ementa,
                })

            log.info(f"  Resultados parseados: {len(resultados)}")
            for r in resultados:
                if importados >= limite:
                    break

                hash_em = hashlib.md5((r.get("ementa", "") or r["numero"]).encode("utf-8")).hexdigest()
                tema_class = "Saude suplementar"  # generico, refinar depois

                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO acordaos
                        (numero_processo, tribunal, orgao_julgador, relator,
                         data_julgamento, classe_processual,
                         ementa, tema, fonte, hash_ementa)
                        VALUES (?, 'STF', ?, ?, ?, ?, ?, ?, 'stf_jurisp_portal', ?)""",
                        (f"{r['classe']} {r['numero']}", "STF",
                         r.get("relator", ""), r.get("data_julgamento", ""),
                         r["classe"], r.get("ementa", ""), tema_class, hash_em),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        importados += 1
                        log.info(f"    OK {r['titulo']}")
                    else:
                        duplicados += 1
                except Exception as e:
                    log.warning(f"  Erro insert {r['titulo']}: {e}")

            conn.commit()

            # Tentar avancar pagina
            try:
                # Varios seletores possiveis
                avancou = False
                for sel in ['button[aria-label*="Próxima"]', 'button[aria-label*="Proxima"]',
                            '.mat-paginator-navigation-next', 'button.mat-mdc-icon-button:last-child']:
                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, sel)
                        if next_btn.is_enabled() and next_btn.is_displayed():
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                            time.sleep(1)
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(6)
                            avancou = True
                            break
                    except Exception:
                        continue
                if not avancou:
                    log.info("  Sem proxima pagina disponivel")
                    break
                pagina += 1
            except Exception as e:
                log.warning(f"  Paginacao falhou: {e}")
                break

    except Exception as e:
        log.error(f"  Erro: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass
        if close:
            conn.close()

    log.info(f"  STF '{termo}': {importados} importados, {duplicados} duplicados")
    return importados, duplicados


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--termo", required=True)
    p.add_argument("--limite", type=int, default=30)
    p.add_argument("--paginas", type=int, default=3)
    args = p.parse_args()
    importar_busca(args.termo, limite=args.limite, max_paginas=args.paginas)


if __name__ == "__main__":
    main()

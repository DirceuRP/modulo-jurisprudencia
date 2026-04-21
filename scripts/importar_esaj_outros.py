"""
scripts/importar_esaj_outros.py — Importa jurisprudencia de TJs com e-SAJ
(TJCE, TJAL, e potencialmente outros) reusando logica do TJSP.

Uso:
    python scripts/importar_esaj_outros.py --tribunal TJCE --todos-temas --limite 50
    python scripts/importar_esaj_outros.py --tribunal TJAL --tema "TEA/Autismo" --limite 30
"""
import argparse
import hashlib
import html as html_mod
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

# Config dos TJs com e-SAJ confirmado (sem captcha bloqueante)
TJS_ESAJ = {
    "TJCE": "https://esaj.tjce.jus.br/cjsg",
    "TJAL": "https://www2.tjal.jus.br/cjsg",
    "TJAC": "https://esaj.tjac.jus.br/cjsg",
    "TJMS": "https://esaj.tjms.jus.br/cjsg",
    "TJRN": "https://esaj.tjrn.jus.br/cjsg",
}

# Temas de saude suplementar (mesmo padrao do TJSP)
TEMAS_BUSCA = {
    "TEA/Autismo": '"plano de saude" autismo',
    "Oncologia": '"plano de saude" quimioterapia',
    "Home care": '"plano de saude" "home care"',
    "Cobertura de medicamento": '"plano de saude" medicamento negativa',
    "Reajuste/mensalidade": '"plano de saude" reajuste',
    "Rescisao/cancelamento": '"plano de saude" cancelamento',
    "Saude mental": '"plano de saude" psiquiatria',
    "Urgencia/emergencia": '"plano de saude" urgencia emergencia',
    "Reembolso": '"plano de saude" reembolso',
    "Carencia": '"plano de saude" carencia',
}


def _criar_driver():
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver


def _executar_busca(driver, base_url, termo):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    driver.get(f"{base_url}/consultaCompleta.do")
    time.sleep(3)

    campo = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "iddados.buscaInteiroTeor"))
    )
    campo.clear()
    campo.send_keys(termo)

    # Tentar marcar checkbox de Acordaos
    try:
        cb = driver.find_element(By.CSS_SELECTOR, "input[value='A'][name='tipoDecisaoSelecionados']")
        if not cb.is_selected():
            cb.click()
    except Exception:
        pass

    btn = driver.find_element(By.ID, "pbSubmit")
    btn.click()
    time.sleep(4)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.ementaClass2, .ementaClass"))
        )
    except Exception:
        pass


def _ir_proxima_pagina(driver, pagina):
    from selenium.webdriver.common.by import By
    try:
        next_btn = driver.find_element(
            By.CSS_SELECTOR, 'div.trocaDePagina a[title="Pr\u00f3xima p\u00e1gina"]'
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
        time.sleep(0.5)
        next_btn.click()
        time.sleep(3)
        return True
    except Exception:
        return False


def _parsear_resultados(html):
    acordaos = []
    vistos = set()
    blocos = re.split(r'<td[^>]*class="ementaClass"[^>]*>\s*<strong>\s*\d+', html)
    for bloco in blocos[1:]:
        ac = _parsear_bloco(bloco)
        if ac and ac["numero_processo"] not in vistos:
            vistos.add(ac["numero_processo"])
            acordaos.append(ac)
    return acordaos


def _parsear_bloco(bloco):
    proc_match = re.search(r"(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})", bloco)
    if not proc_match:
        return None
    numero = proc_match.group(1)

    ementa = ""
    em_match = re.search(r'class="mensagemSemFormatacao">\s*(.*?)\s*</div>', bloco, re.DOTALL)
    if em_match:
        ementa = _limpar_html(em_match.group(1))
    if not ementa:
        em2 = re.search(r'<strong>Ementa:\s*</strong>(.*?)(?:</div>|</td>)', bloco, re.DOTALL)
        if em2:
            ementa = _limpar_html(em2.group(1))

    classe = _meta(bloco, r"Classe/Assunto:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    relator = _meta(bloco, r"Relator\(a\):\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    comarca = _meta(bloco, r"Comarca:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    orgao = _meta(bloco, r"[OÓ]rg[aã]o\s+julgador:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    data_julg = _meta(bloco, r"Data do julgamento:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    data_pub = _meta(bloco, r"Data de publica[cç][aã]o:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")

    if not ementa and not classe:
        return None

    return {
        "numero_processo": numero,
        "classe_processual": _limpar_html(classe).strip(),
        "relator": _limpar_html(relator).strip(),
        "comarca": _limpar_html(comarca).strip(),
        "orgao_julgador": _limpar_html(orgao).strip(),
        "data_julgamento": _norm_data(_limpar_html(data_julg).strip()),
        "data_publicacao": _norm_data(_limpar_html(data_pub).strip()),
        "ementa": ementa.strip()[:10000],
    }


def _meta(bloco, pattern, default=""):
    m = re.search(pattern, bloco, re.DOTALL | re.IGNORECASE)
    return m.group(1) if m else default


def _limpar_html(texto):
    if not texto:
        return ""
    texto = html_mod.unescape(texto)
    texto = re.sub(r"<br\s*/?>", "\n", texto)
    texto = re.sub(r"<[^>]+>", " ", texto)
    texto = re.sub(r"&nbsp;", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def _norm_data(s):
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", s.strip())
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}" if m else s


def _classificar(ementa):
    if not ementa:
        return None
    up = ementa.upper()
    mapa = {
        "TEA/Autismo": ["AUTIS", "TEA ", "ESPECTRO AUTISTA", "TERAPIA ABA"],
        "Oncologia": ["QUIMIOTERAPIA", "RADIOTERAPIA", "ONCOLOG", "CANCER", "NEOPLASIA", "CÂNCER"],
        "Home care": ["HOME CARE", "INTERNACAO DOMICILIAR", "INTERNAÇÃO DOMICILIAR"],
        "Cobertura de medicamento": ["MEDICAMENTO", "OFF LABEL", "OFF-LABEL"],
        "Reajuste/mensalidade": ["REAJUSTE", "MENSALIDADE", "FAIXA ETARIA", "FAIXA ETÁRIA"],
        "Rescisao/cancelamento": ["RESCISAO", "RESCISÃO", "CANCELAMENTO"],
        "Saude mental": ["PSIQUIATR", "DEPENDENCIA QUIMICA"],
        "Urgencia/emergencia": ["URGENCIA", "URGÊNCIA", "EMERGENCIA", "EMERGÊNCIA"],
        "Reembolso": ["REEMBOLSO"],
        "Carencia": ["CARENCIA", "CARÊNCIA"],
    }
    for tema, termos in mapa.items():
        if any(t in up for t in termos):
            return tema
    return None


def importar_tema(tribunal, base_url, tema, termo, limite=50, max_paginas=5, conn=None):
    log.info(f"\n{'='*60}\n  {tribunal} - {tema} | termos: {termo}\n{'='*60}")
    close = conn is None
    if conn is None:
        conn = inicializar()

    fonte = f"{tribunal.lower()}_esaj"
    importados = duplicados = 0
    driver = None

    try:
        driver = _criar_driver()
        _executar_busca(driver, base_url, termo)
        pagina = 1
        while pagina <= max_paginas and importados < limite:
            html = driver.page_source
            acordaos = _parsear_resultados(html)
            if not acordaos:
                break
            for ac in acordaos:
                if importados >= limite:
                    break
                tema_class = _classificar(ac.get("ementa", "")) or tema
                hash_em = hashlib.md5(ac.get("ementa", "").encode("utf-8")).hexdigest()
                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO acordaos
                        (numero_processo, tribunal, orgao_julgador, relator,
                         data_julgamento, data_publicacao, classe_processual,
                         ementa, tema, fonte, hash_ementa)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (ac["numero_processo"], tribunal, ac.get("orgao_julgador", ""),
                         ac.get("relator", ""), ac.get("data_julgamento", ""),
                         ac.get("data_publicacao", ""), ac.get("classe_processual", ""),
                         ac.get("ementa", ""), tema_class, fonte, hash_em),
                    )
                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        importados += 1
                    else:
                        duplicados += 1
                except Exception as e:
                    log.warning(f"  Erro insert: {e}")
                    duplicados += 1
            conn.commit()
            log.info(f"  {tribunal} pagina {pagina}: {importados} importados")
            pagina += 1
            if pagina <= max_paginas and importados < limite:
                if not _ir_proxima_pagina(driver, pagina - 1):
                    break
                time.sleep(2)
    except Exception as e:
        log.error(f"Erro: {e}")
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass
        if close:
            conn.close()

    log.info(f"  {tribunal} {tema}: {importados} importados, {duplicados} duplicados")
    return importados, duplicados


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tribunal", required=True, choices=list(TJS_ESAJ.keys()))
    p.add_argument("--tema")
    p.add_argument("--todos-temas", action="store_true")
    p.add_argument("--limite", type=int, default=50)
    p.add_argument("--paginas", type=int, default=5)
    args = p.parse_args()

    base_url = TJS_ESAJ[args.tribunal]

    if args.todos_temas:
        conn = inicializar()
        total_imp = total_dup = 0
        try:
            for tema, termo in TEMAS_BUSCA.items():
                imp, dup = importar_tema(args.tribunal, base_url, tema, termo,
                                          limite=args.limite, max_paginas=args.paginas, conn=conn)
                total_imp += imp
                total_dup += dup
                time.sleep(5)
        finally:
            conn.close()
        log.info(f"\nTOTAL {args.tribunal}: {total_imp} importados, {total_dup} duplicados")
    elif args.tema:
        if args.tema in TEMAS_BUSCA:
            importar_tema(args.tribunal, base_url, args.tema, TEMAS_BUSCA[args.tema],
                          limite=args.limite, max_paginas=args.paginas)
    else:
        p.print_help()


if __name__ == "__main__":
    main()

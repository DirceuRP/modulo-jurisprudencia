"""
scripts/importar_tjrj.py — Importa ementas de jurisprudencia publica do TJRJ via eJURIS (sistema legado).

URL: https://www3.tjrj.jus.br/ejuris/ConsultarJurisprudencia.aspx
ASP.NET com __doPostBack — Selenium obrigatorio.

Uso:
    python scripts/importar_tjrj.py --todos-temas --limite 100
    python scripts/importar_tjrj.py --tema "TEA/Autismo" --limite 30
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

# Temas de saude suplementar — termos otimizados para o eJURIS TJRJ
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

URL_BUSCA = "https://www3.tjrj.jus.br/ejuris/ConsultarJurisprudencia.aspx"


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


def _executar_busca(driver, termo):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    log.info(f"  Acessando eJURIS TJRJ...")
    driver.get(URL_BUSCA)
    time.sleep(4)

    # Campo de busca livre
    campo = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "ContentPlaceHolder1_txtTextoPesq"))
    )
    campo.clear()
    campo.send_keys(termo)
    time.sleep(0.5)

    # Restringir a Acordaos via tipo de decisao (se houver radio/checkbox)
    try:
        chk = driver.find_element(By.CSS_SELECTOR, "input[type='checkbox'][value*='Acordao']")
        if not chk.is_selected():
            chk.click()
    except Exception:
        pass

    # Botao "Pesquisar" (vai variar entre versoes)
    for selector in [
        "input[id*='btnPesquisar']",
        "input[value='Pesquisar']",
        "input[type='submit']",
    ]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, selector)
            driver.execute_script("arguments[0].click();", btn)
            break
        except Exception:
            continue
    else:
        # Fallback: submit por enter
        from selenium.webdriver.common.keys import Keys
        campo.send_keys(Keys.RETURN)

    time.sleep(5)
    log.info(f"  Busca submetida")
    return True


def _parsear_resultados(html):
    """Extrai acordaos do HTML do eJURIS.

    Padrao do eJURIS TJRJ:
    - Cada resultado em um div ou tabela com class contendo 'Resultado' ou 'item'
    - Numero do processo: padrao CNJ
    - Ementa em texto plano apos rotulo "Ementa:"
    - Relator: apos "Des(a)." ou "Relator:"
    """
    acordaos = []
    vistos = set()

    # Estrategia generica: dividir por ocorrencias de "Processo" ou padrao CNJ
    # Padrao CNJ: NNNNNNN-DD.AAAA.J.TT.OOOO
    cnj_pattern = re.compile(r"(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})")

    # Tenta dividir por blocos de resultado (varios padroes)
    # Padrao 1: divs com classe contendo "Resultado"
    blocos = re.split(
        r'<div[^>]*class="[^"]*(?:Resultado|item|registro)[^"]*"[^>]*>',
        html, flags=re.IGNORECASE,
    )

    if len(blocos) <= 1:
        # Padrao 2: tabela com linhas (tr) contendo CNJ
        # Buscar todas as ocorrencias de CNJ + extrair contexto
        for m in cnj_pattern.finditer(html):
            inicio = max(0, m.start() - 200)
            fim = min(len(html), m.end() + 3000)
            blocos.append(html[inicio:fim])

    for bloco in blocos:
        ac = _parsear_bloco(bloco)
        if ac and ac["numero_processo"] not in vistos:
            vistos.add(ac["numero_processo"])
            acordaos.append(ac)

    return acordaos


def _parsear_bloco(bloco):
    cnj_match = re.search(r"(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})", bloco)
    if not cnj_match:
        return None

    numero = cnj_match.group(1)

    # Ementa: tentar varios padroes
    ementa = ""
    for pattern in [
        r"Ementa[:\s]*</[^>]+>\s*(.*?)(?:</?(?:div|p|td)|$)",
        r"<[^>]*Ementa[^>]*>\s*(.*?)<",
        r"Ementa[:\s]*([^<]{50,3000})",
    ]:
        m = re.search(pattern, bloco, re.DOTALL | re.IGNORECASE)
        if m:
            ementa = _limpar_html(m.group(1))
            if len(ementa) > 50:
                break

    relator = ""
    rel_match = re.search(
        r"(?:Relator|Des(?:embargador)?(?:\(a\))?)[\s.:]*([A-ZÀ-Üa-zà-ü\s\.]+?)(?:\s+-|<|\n)",
        bloco, re.IGNORECASE,
    )
    if rel_match:
        relator = rel_match.group(1).strip()[:150]

    orgao = ""
    org_match = re.search(
        r"(?:[OÓ]rg[aã]o|C[aâ]mara)[\s:]*([^<\n]{5,150})",
        bloco, re.IGNORECASE,
    )
    if org_match:
        orgao = _limpar_html(org_match.group(1)).strip()[:150]

    data = ""
    data_match = re.search(r"(\d{2}/\d{2}/\d{4})", bloco)
    if data_match:
        data = _normalizar_data(data_match.group(1))

    if not ementa:
        return None

    return {
        "numero_processo": numero,
        "classe_processual": "",
        "relator": relator,
        "orgao_julgador": orgao,
        "data_julgamento": data,
        "data_publicacao": data,
        "ementa": ementa[:10000],
    }


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


def _normalizar_data(data_str):
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", data_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return data_str


def _classificar_tema(ementa):
    if not ementa:
        return None
    ementa_up = ementa.upper()
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
        if any(t in ementa_up for t in termos):
            return tema
    return None


def importar_tema(tema, termo_busca, limite=50, conn=None):
    log.info(f"\n{'='*60}\n  TJRJ - TEMA: {tema}\n  Termos: {termo_busca}\n{'='*60}")

    close_conn = False
    if conn is None:
        conn = inicializar()
        close_conn = True

    importados = 0
    duplicados = 0
    driver = None

    try:
        driver = _criar_driver()
        _executar_busca(driver, termo_busca)

        # Parse primeira pagina
        html = driver.page_source

        # Salvar HTML para debug se necessario
        log.info(f"  Pagina retornou {len(html):,} chars")

        acordaos = _parsear_resultados(html)
        log.info(f"  Acordaos parseados: {len(acordaos)}")

        for ac in acordaos[:limite]:
            tema_class = _classificar_tema(ac.get("ementa", "")) or tema
            hash_em = hashlib.md5(ac.get("ementa", "").encode("utf-8")).hexdigest()
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO acordaos
                    (numero_processo, tribunal, orgao_julgador, relator,
                     data_julgamento, data_publicacao, classe_processual,
                     ementa, tema, fonte, hash_ementa)
                    VALUES (?, 'TJRJ', ?, ?, ?, ?, ?, ?, ?, 'tjrj_ejuris', ?)
                    """,
                    (ac["numero_processo"], ac.get("orgao_julgador", ""),
                     ac.get("relator", ""), ac.get("data_julgamento", ""),
                     ac.get("data_publicacao", ""), ac.get("classe_processual", ""),
                     ac.get("ementa", ""), tema_class, hash_em),
                )
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    importados += 1
                else:
                    duplicados += 1
            except Exception as e:
                log.warning(f"  Erro inserindo {ac['numero_processo']}: {e}")
                duplicados += 1

        conn.commit()
    except Exception as e:
        log.error(f"  Erro: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass
        if close_conn:
            conn.close()

    log.info(f"  TJRJ {tema}: {importados} importados, {duplicados} duplicados")
    return importados, duplicados


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tema", type=str)
    parser.add_argument("--todos-temas", action="store_true")
    parser.add_argument("--limite", type=int, default=50)
    args = parser.parse_args()

    if args.todos_temas:
        conn = inicializar()
        total_imp = total_dup = 0
        try:
            for tema, termo in TEMAS_BUSCA.items():
                imp, dup = importar_tema(tema, termo, limite=args.limite, conn=conn)
                total_imp += imp
                total_dup += dup
                time.sleep(5)
        finally:
            conn.close()
        log.info(f"\nTOTAL TJRJ: {total_imp} importados, {total_dup} duplicados")
    elif args.tema:
        if args.tema in TEMAS_BUSCA:
            importar_tema(args.tema, TEMAS_BUSCA[args.tema], limite=args.limite)
        else:
            log.error(f"Tema desconhecido: {args.tema}. Disponiveis: {', '.join(TEMAS_BUSCA.keys())}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

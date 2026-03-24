"""
scripts/importar_natjus.py — Importa notas técnicas e pareceres do e-NatJus (CNJ).

Pareceres técnico-científicos (~80-160):
    https://www.pje.jus.br/e-natjus/parecerTecnico-listar.php
    - Baixa PDFs para dados/natjus_pareceres/

Notas técnicas (~394.000 total, importar por tema/CID):
    https://www.pje.jus.br/e-natjus/pesquisaPublica.php
    - Apenas metadados (sem download de PDF)

Uso:
    python scripts/importar_natjus.py --pareceres
    python scripts/importar_natjus.py --notas --tema "TEA/Autismo" --limite 50
    python scripts/importar_natjus.py --notas --todos-temas --limite 100
"""

import argparse
import logging
import os
import re
import sys
import time
import unicodedata

sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from banco import inicializar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# Diretório para PDFs dos pareceres
DADOS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dados")
PARECERES_DIR = os.path.join(DADOS_DIR, "natjus_pareceres")

# URLs
URL_PARECERES = "https://www.pje.jus.br/e-natjus/parecerTecnico-listar.php"
URL_NOTAS = "https://www.pje.jus.br/e-natjus/pesquisaPublica.php"
URL_BASE = "https://www.pje.jus.br/e-natjus/"

# Rate limiting
DELAY_ENTRE_REQUESTS = 2  # segundos

# Temas e CIDs para busca de notas técnicas
TEMAS_CID = {
    "TEA/Autismo": {"cids": ["F84", "F84.0", "F84.1"], "textos": ["autismo", "ABA"]},
    "Oncologia": {"cids": ["C50", "C61", "C34", "C18", "C91"], "textos": ["quimioterapia", "imunoterapia"]},
    "Saude mental": {"cids": ["F20", "F31", "F32", "F33", "F10"], "textos": ["internação psiquiátrica"]},
    "Home care": {"cids": [], "textos": ["home care", "internação domiciliar", "ventilação mecânica"]},
    "Transplante": {"cids": [], "textos": ["transplante renal", "transplante hepático", "transplante medula"]},
    "Cirurgia": {"cids": [], "textos": ["bariátrica", "cirurgia reparadora"]},
    "Cobertura de medicamento": {"cids": [], "textos": ["canabidiol", "medicamento", "imunobiológico", "insulina"]},
    "Cobertura de procedimento": {"cids": [], "textos": ["fisioterapia", "fonoaudiologia", "terapia ocupacional", "psicoterapia"]},
    "Protese/ortese": {"cids": [], "textos": ["prótese", "órtese", "implante coclear", "stent"]},
    "Reajuste/mensalidade": {"cids": [], "textos": []},  # NatJus não cobre temas contratuais
    "Rescisao/cancelamento": {"cids": [], "textos": []},
    "Urgencia/emergencia": {"cids": [], "textos": ["urgência", "emergência"]},
    "Rede credenciada": {"cids": [], "textos": []},
    "Reembolso": {"cids": [], "textos": []},
    "Carencia": {"cids": [], "textos": []},
    "Dano moral": {"cids": [], "textos": []},
    "Rol da ANS": {"cids": [], "textos": ["rol de procedimentos", "cobertura obrigatória"]},
}


def _limpar_nome_arquivo(texto, max_len=80):
    """Remove caracteres especiais para nome de arquivo."""
    if not texto:
        return "sem_titulo"
    # Normalizar unicode, remover acentos
    nfkd = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Manter apenas alfanumericos, espaços e hifens
    limpo = re.sub(r"[^\w\s-]", "", sem_acento).strip()
    limpo = re.sub(r"\s+", "_", limpo)
    return limpo[:max_len] if limpo else "sem_titulo"


def _criar_driver():
    """Cria Selenium WebDriver com Chrome headless."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=pt-BR")
    # Configurar download de PDFs
    prefs = {
        "download.default_directory": PARECERES_DIR,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    return driver


# ============================================================
# PARECERES TÉCNICO-CIENTÍFICOS
# ============================================================

def importar_pareceres(conn, driver):
    """Importa todos os pareceres técnico-científicos do e-NatJus."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    log.info("=== Importando Pareceres Técnico-Científicos ===")
    os.makedirs(PARECERES_DIR, exist_ok=True)

    # Registrar importação
    conn.execute("""
        INSERT INTO log_importacao (fonte, data_inicio, status)
        VALUES ('natjus_pareceres', datetime('now'), 'em_andamento')
    """)
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    importados = 0
    duplicados = 0
    erros = 0
    pagina_atual = 1

    try:
        driver.get(URL_PARECERES)
        time.sleep(3)

        while True:
            log.info(f"  Página {pagina_atual}...")

            # Esperar tabela carregar
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
                )
            except Exception:
                log.warning(f"  Tabela não encontrada na página {pagina_atual}")
                break

            # Extrair linhas da tabela
            linhas = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not linhas:
                log.info("  Nenhuma linha encontrada, encerrando.")
                break

            for linha in linhas:
                try:
                    colunas = linha.find_elements(By.TAG_NAME, "td")
                    if len(colunas) < 4:
                        continue

                    natjus_id_text = colunas[0].text.strip()
                    data_pub = colunas[1].text.strip()
                    titulo = colunas[2].text.strip()
                    status = colunas[3].text.strip()

                    if not natjus_id_text:
                        continue

                    try:
                        natjus_id = int(re.sub(r"\D", "", natjus_id_text))
                    except ValueError:
                        log.warning(f"  ID não numérico: {natjus_id_text}")
                        continue

                    # Procurar link de download do PDF
                    arquivo_hash = None
                    try:
                        link = linha.find_element(By.CSS_SELECTOR, "a[href*='arquivo-download']")
                        href = link.get_attribute("href") or ""
                        match = re.search(r"hash=([A-Za-z0-9]+)", href)
                        if match:
                            arquivo_hash = match.group(1)
                    except Exception:
                        # Tenta procurar qualquer link na linha
                        try:
                            links = linha.find_elements(By.TAG_NAME, "a")
                            for lnk in links:
                                href = lnk.get_attribute("href") or ""
                                if "download" in href.lower() or "arquivo" in href.lower():
                                    match = re.search(r"hash=([A-Za-z0-9]+)", href)
                                    if match:
                                        arquivo_hash = match.group(1)
                                        break
                        except Exception:
                            pass

                    # Normalizar data (DD/MM/YYYY → YYYY-MM-DD)
                    data_normalizada = None
                    if data_pub:
                        partes = data_pub.split("/")
                        if len(partes) == 3:
                            data_normalizada = f"{partes[2]}-{partes[1]}-{partes[0]}"

                    # Inserir no banco
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO natjus_pareceres
                            (natjus_id, tipo, data_publicacao, titulo, status, arquivo_hash)
                            VALUES (?, 'parecer', ?, ?, ?, ?)
                        """, (natjus_id, data_normalizada, titulo, status, arquivo_hash))

                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            importados += 1
                            log.info(f"    [+] Parecer {natjus_id}: {titulo[:60]}...")
                        else:
                            duplicados += 1
                    except Exception as e:
                        log.warning(f"    Erro inserindo parecer {natjus_id}: {e}")
                        erros += 1

                except Exception as e:
                    log.warning(f"    Erro processando linha: {e}")
                    erros += 1

            conn.commit()

            # Navegar para próxima página
            if not _proxima_pagina(driver, pagina_atual):
                break
            pagina_atual += 1
            time.sleep(DELAY_ENTRE_REQUESTS)

        # Baixar PDFs dos pareceres que têm hash
        _baixar_pdfs_pareceres(conn, driver)

        # Atualizar log
        conn.execute("""
            UPDATE log_importacao
            SET data_fim=datetime('now'), registros_importados=?,
                registros_duplicados=?, status='concluido'
            WHERE id=?
        """, (importados, duplicados, log_id))
        conn.commit()

    except Exception as e:
        log.error(f"Erro na importação de pareceres: {e}")
        conn.execute("""
            UPDATE log_importacao SET status='erro', erro=? WHERE id=?
        """, (str(e)[:500], log_id))
        conn.commit()

    log.info(f"\n  Pareceres: {importados} importados, {duplicados} duplicados, {erros} erros")
    return importados


def _proxima_pagina(driver, pagina_atual):
    """Tenta navegar para a próxima página. Retorna True se conseguiu."""
    from selenium.webdriver.common.by import By

    try:
        # Procurar links de paginação
        paginacao = driver.find_elements(By.CSS_SELECTOR, "ul.pagination li a, .pagination a, a.page-link, nav a")
        if not paginacao:
            # Tentar outros seletores comuns
            paginacao = driver.find_elements(By.XPATH,
                "//a[contains(@href, 'pagina') or contains(@href, 'page') or contains(@onclick, 'pagina')]"
            )

        prox_pagina = pagina_atual + 1
        for link in paginacao:
            texto = link.text.strip()
            href = link.get_attribute("href") or ""
            onclick = link.get_attribute("onclick") or ""

            # Verificar se é o link da próxima página
            if texto == str(prox_pagina):
                link.click()
                time.sleep(DELAY_ENTRE_REQUESTS)
                return True

            # "Próximo" ou ">" ou ">>"
            if texto in [">", ">>", "›", "Próximo", "Próxima", "Next"]:
                link.click()
                time.sleep(DELAY_ENTRE_REQUESTS)
                return True

        # Tentar botão "next" genérico
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, ".next a, .pagination .next, a[rel='next']")
            next_btn.click()
            time.sleep(DELAY_ENTRE_REQUESTS)
            return True
        except Exception:
            pass

        log.info("  Sem mais páginas disponíveis.")
        return False

    except Exception as e:
        log.warning(f"  Erro na paginação: {e}")
        return False


def _baixar_pdfs_pareceres(conn, driver):
    """Baixa PDFs de todos os pareceres que têm hash mas não têm arquivo local."""
    import urllib.request

    pareceres = conn.execute("""
        SELECT id, natjus_id, titulo, arquivo_hash
        FROM natjus_pareceres
        WHERE tipo='parecer' AND arquivo_hash IS NOT NULL AND arquivo_local IS NULL
    """).fetchall()

    if not pareceres:
        log.info("  Nenhum PDF de parecer para baixar.")
        return

    log.info(f"\n  Baixando {len(pareceres)} PDFs de pareceres...")
    os.makedirs(PARECERES_DIR, exist_ok=True)

    baixados = 0
    for p in pareceres:
        p_id, natjus_id, titulo, arquivo_hash = p["id"], p["natjus_id"], p["titulo"], p["arquivo_hash"]
        nome_arquivo = f"{natjus_id}_{_limpar_nome_arquivo(titulo)}.pdf"
        caminho = os.path.join(PARECERES_DIR, nome_arquivo)

        if os.path.exists(caminho):
            conn.execute("UPDATE natjus_pareceres SET arquivo_local=? WHERE id=?", (caminho, p_id))
            baixados += 1
            continue

        url_pdf = f"{URL_BASE}arquivo-download.php?hash={arquivo_hash}"
        log.info(f"    Baixando parecer {natjus_id}: {nome_arquivo[:60]}...")

        try:
            req = urllib.request.Request(url_pdf, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                conteudo = resp.read()

            if len(conteudo) < 500:
                log.warning(f"    PDF muito pequeno ({len(conteudo)} bytes), pode ser erro")
                continue

            with open(caminho, "wb") as f:
                f.write(conteudo)

            conn.execute("UPDATE natjus_pareceres SET arquivo_local=? WHERE id=?", (caminho, p_id))
            conn.commit()
            baixados += 1
            log.info(f"    OK ({len(conteudo) / 1024:.0f} KB)")

        except Exception as e:
            log.warning(f"    Erro ao baixar parecer {natjus_id}: {e}")

        time.sleep(DELAY_ENTRE_REQUESTS)

    conn.commit()
    log.info(f"  PDFs baixados: {baixados}/{len(pareceres)}")


# ============================================================
# NOTAS TÉCNICAS
# ============================================================

def importar_notas(conn, driver, tema=None, todos_temas=False, limite=100):
    """Importa notas técnicas do e-NatJus por tema/CID."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select

    log.info("=== Importando Notas Técnicas ===")

    temas = {}
    if todos_temas:
        temas = TEMAS_CID
    elif tema:
        if tema not in TEMAS_CID:
            log.error(f"Tema '{tema}' não encontrado. Disponíveis: {list(TEMAS_CID.keys())}")
            return 0
        temas = {tema: TEMAS_CID[tema]}
    else:
        log.error("Especifique --tema ou --todos-temas")
        return 0

    total_importados = 0

    for nome_tema, config in temas.items():
        log.info(f"\n--- Tema: {nome_tema} ---")
        cids = config.get("cids", [])
        textos = config.get("textos", [])

        # Buscar por CID
        for cid in cids:
            importados = _buscar_notas_por_cid(conn, driver, cid, nome_tema, limite)
            total_importados += importados
            time.sleep(DELAY_ENTRE_REQUESTS)

        # Buscar por texto (diagnóstico)
        for texto in textos:
            importados = _buscar_notas_por_texto(conn, driver, texto, nome_tema, limite)
            total_importados += importados
            time.sleep(DELAY_ENTRE_REQUESTS)

    log.info(f"\n=== Total notas importadas: {total_importados} ===")
    return total_importados


def _buscar_notas_por_cid(conn, driver, cid, tema, limite):
    """Busca notas técnicas por código CID."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    log.info(f"  Buscando CID: {cid}...")

    try:
        driver.get(URL_NOTAS)
        time.sleep(3)

        # Limpar e preencher campo de diagnóstico com o CID
        campo_diag = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "txtDescAvaliacaoDiagnosticoSemCID"))
        )
        campo_diag.clear()
        campo_diag.send_keys(cid)

        # Clicar em buscar
        _clicar_buscar(driver)
        time.sleep(3)

        return _extrair_notas_tabela(conn, driver, tema, limite, cid_busca=cid)

    except Exception as e:
        log.error(f"  Erro buscando CID {cid}: {e}")
        return 0


def _buscar_notas_por_texto(conn, driver, texto, tema, limite):
    """Busca notas técnicas por texto (usa campo de tecnologia, visível no formulário)."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    log.info(f"  Buscando texto: '{texto}'...")

    try:
        driver.get(URL_NOTAS)
        time.sleep(3)

        # Campo txtTecnologiaE é visível e aceita texto livre (tecnologia/procedimento)
        campo = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "txtTecnologiaE"))
        )
        campo.clear()
        campo.send_keys(texto)

        # Clicar em buscar
        _clicar_buscar(driver)
        time.sleep(4)

        return _extrair_notas_tabela(conn, driver, tema, limite)

    except Exception as e:
        # Fallback: tentar pelo campo de diagnóstico
        try:
            driver.get(URL_NOTAS)
            time.sleep(3)
            campo_diag = driver.find_element(By.ID, "txtDescAvaliacaoDiagnosticoSemCID")
            campo_diag.clear()
            campo_diag.send_keys(texto)
            _clicar_buscar(driver)
            time.sleep(4)
            return _extrair_notas_tabela(conn, driver, tema, limite)
        except Exception as e2:
            log.error(f"  Erro buscando texto '{texto}': {e2}")
            return 0


def _clicar_buscar(driver):
    """Clica no botão de busca do formulário."""
    from selenium.webdriver.common.by import By

    try:
        # Tentar por diferentes seletores
        for seletor in [
            "input[type='submit']",
            "button[type='submit']",
            "#btnPesquisar",
            "input[value='Pesquisar']",
            "input[value='Buscar']",
            "button.btn-primary",
        ]:
            try:
                btn = driver.find_element(By.CSS_SELECTOR, seletor)
                btn.click()
                return
            except Exception:
                continue

        # Último recurso: qualquer botão com texto de busca
        botoes = driver.find_elements(By.TAG_NAME, "button") + driver.find_elements(By.TAG_NAME, "input")
        for btn in botoes:
            texto = (btn.text or btn.get_attribute("value") or "").lower()
            if "pesquis" in texto or "buscar" in texto:
                btn.click()
                return

        log.warning("  Botão de busca não encontrado, tentando submit do form")
        form = driver.find_element(By.TAG_NAME, "form")
        form.submit()

    except Exception as e:
        log.warning(f"  Erro ao clicar buscar: {e}")


def _proxima_pagina_natjus(driver, pagina_atual):
    """Paginação específica do NatJus (usa links JavaScript com href='#')."""
    from selenium.webdriver.common.by import By

    prox = pagina_atual + 1
    try:
        # O NatJus usa: <a href="#">2</a>, <a href="#">3</a>, <a href="#">»</a>
        links = driver.find_elements(By.CSS_SELECTOR, "a[href='#']")
        for link in links:
            texto = link.text.strip()
            # Tentar pelo número da próxima página
            if texto == str(prox):
                # Guardar referência da tabela antes do clique para detectar mudança
                old_first = None
                try:
                    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr td:first-child")
                    if rows:
                        old_first = rows[0].text.strip()
                except Exception:
                    pass

                link.click()
                time.sleep(3)

                # Esperar a tabela mudar (novo conteúdo)
                for _ in range(5):
                    try:
                        new_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr td:first-child")
                        if new_rows:
                            new_first = new_rows[0].text.strip()
                            if new_first != old_first:
                                return True
                    except Exception:
                        pass
                    time.sleep(1)

                # Mesmo que não detecte mudança, assuma sucesso se a tabela existe
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                return len(rows) > 0

        # Tentar "»" (próxima)
        for link in links:
            if link.text.strip() in ["»", "›", ">", "Próximo"]:
                link.click()
                time.sleep(4)
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                return len(rows) > 0

        return False

    except Exception as e:
        log.warning(f"  Erro na paginação NatJus: {e}")
        return False


def _extrair_notas_tabela(conn, driver, tema, limite, cid_busca=None):
    """Extrai notas técnicas da tabela de resultados."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    importados = 0
    duplicados = 0
    pagina = 1
    paginas_sem_novos = 0
    MAX_PAGINAS_SEM_NOVOS = 3

    # Registrar importação
    conn.execute("""
        INSERT INTO log_importacao (fonte, tribunal, data_inicio, status)
        VALUES ('natjus_notas', ?, datetime('now'), 'em_andamento')
    """, (tema,))
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    try:
        while importados < limite:
            # Esperar tabela com retry
            tabela_ok = False
            for tentativa in range(3):
                try:
                    WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
                    )
                    tabela_ok = True
                    break
                except Exception:
                    if tentativa < 2:
                        time.sleep(3)
                        continue

            if not tabela_ok:
                try:
                    page_text = driver.find_element(By.TAG_NAME, "body").text
                    if "nenhum" in page_text.lower() or "sem resultado" in page_text.lower():
                        log.info("  Nenhum resultado encontrado.")
                    else:
                        log.warning("  Tabela de resultados não carregou.")
                except Exception:
                    pass
                break

            linhas = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            if not linhas:
                break

            novos_na_pagina = 0
            for linha in linhas:
                if importados >= limite:
                    break

                try:
                    colunas = linha.find_elements(By.TAG_NAME, "td")
                    if len(colunas) < 5:
                        continue

                    natjus_id_text = colunas[0].text.strip()
                    data_pub = colunas[1].text.strip()
                    tecnologia = colunas[2].text.strip()

                    cid_texto = ""
                    natjus_origem = ""
                    status = ""

                    if len(colunas) >= 6:
                        cid_texto = colunas[3].text.strip()
                        natjus_origem = colunas[4].text.strip()
                        status = colunas[5].text.strip()
                    elif len(colunas) == 5:
                        cid_texto = colunas[3].text.strip()
                        status = colunas[4].text.strip()

                    if not natjus_id_text:
                        continue

                    try:
                        natjus_id = int(re.sub(r"\D", "", natjus_id_text))
                    except ValueError:
                        continue

                    data_normalizada = None
                    if data_pub:
                        partes = data_pub.split("/")
                        if len(partes) == 3:
                            data_normalizada = f"{partes[2]}-{partes[1]}-{partes[0]}"

                    cid_codigo = None
                    cid_descricao = None
                    if cid_texto:
                        match_cid = re.match(r"([A-Z]\d{2}(?:\.\d{1,2})?)\s*[-–]?\s*(.*)", cid_texto)
                        if match_cid:
                            cid_codigo = match_cid.group(1)
                            cid_descricao = match_cid.group(2).strip() or None
                        else:
                            cid_codigo = cid_busca

                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO natjus_pareceres
                            (natjus_id, tipo, data_publicacao, titulo, cid, cid_descricao,
                             natjus_origem, status, tema)
                            VALUES (?, 'nota_tecnica', ?, ?, ?, ?, ?, ?, ?)
                        """, (natjus_id, data_normalizada, tecnologia, cid_codigo,
                              cid_descricao, natjus_origem, status, tema))

                        if conn.execute("SELECT changes()").fetchone()[0] > 0:
                            importados += 1
                            novos_na_pagina += 1
                        else:
                            duplicados += 1
                    except Exception as e:
                        log.warning(f"    Erro inserindo nota {natjus_id}: {e}")

                except Exception as e:
                    log.warning(f"    Erro processando linha de nota: {e}")

            conn.commit()
            log.info(f"    Página {pagina}: +{novos_na_pagina} notas ({importados} total, {duplicados} dup)")

            if importados >= limite:
                break

            # Detectar páginas sem novos registros (evitar loop infinito em paginação cíclica)
            if novos_na_pagina == 0:
                paginas_sem_novos += 1
                if paginas_sem_novos >= MAX_PAGINAS_SEM_NOVOS:
                    log.info(f"    {MAX_PAGINAS_SEM_NOVOS} páginas sem novos registros, encerrando busca.")
                    break
            else:
                paginas_sem_novos = 0

            # Próxima página — NatJus usa links JavaScript
            if not _proxima_pagina_natjus(driver, pagina):
                break
            pagina += 1
            time.sleep(DELAY_ENTRE_REQUESTS + 2)  # NatJus precisa de mais tempo

    except Exception as e:
        log.error(f"  Erro extraindo notas: {e}")
        conn.execute("""
            UPDATE log_importacao SET status='erro', erro=? WHERE id=?
        """, (str(e)[:500], log_id))
        conn.commit()

    # Atualizar log
    conn.execute("""
        UPDATE log_importacao
        SET data_fim=datetime('now'), registros_importados=?,
            registros_duplicados=?, status='concluido'
        WHERE id=?
    """, (importados, duplicados, log_id))
    conn.commit()

    return importados


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Importar notas e pareceres do e-NatJus (CNJ)")
    parser.add_argument("--pareceres", action="store_true",
                        help="Importar pareceres técnico-científicos (com PDFs)")
    parser.add_argument("--notas", action="store_true",
                        help="Importar notas técnicas (apenas metadados)")
    parser.add_argument("--tema", type=str,
                        help=f"Tema específico: {list(TEMAS_CID.keys())}")
    parser.add_argument("--todos-temas", action="store_true",
                        help="Importar notas de todos os temas")
    parser.add_argument("--limite", type=int, default=100,
                        help="Limite de registros por busca (default: 100)")
    args = parser.parse_args()

    if not args.pareceres and not args.notas:
        parser.error("Especifique --pareceres e/ou --notas")

    # Inicializar banco
    conn = inicializar()
    driver = None

    try:
        driver = _criar_driver()
        log.info("Selenium WebDriver iniciado (headless)")

        if args.pareceres:
            importar_pareceres(conn, driver)

        if args.notas:
            importar_notas(conn, driver, tema=args.tema,
                          todos_temas=args.todos_temas, limite=args.limite)

    except Exception as e:
        log.error(f"Erro fatal: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
                log.info("WebDriver encerrado.")
            except Exception:
                pass
        conn.close()
        log.info("Conexão com banco encerrada.")


if __name__ == "__main__":
    main()

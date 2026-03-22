"""
scripts/importar_tjsp.py -- Importa ementas de jurisprudencia publica do TJSP via e-SAJ.

Usa Selenium headless para acessar o CJSG (Consulta de Jurisprudencia) do TJSP,
buscar por temas de saude suplementar e extrair acordaos com ementa completa.

Estrategia de importacao (em ordem de preferencia):
    1. Selenium + e-SAJ CJSG (ementas completas)
    2. DataJud API (metadados -- fallback sem ementa)

Uso:
    python scripts/importar_tjsp.py --todos-temas --limite 200
    python scripts/importar_tjsp.py --tema "TEA/Autismo" --limite 40
    python scripts/importar_tjsp.py --assunto "plano de saude autismo" --limite 100
    python scripts/importar_tjsp.py --tema "TEA/Autismo" --paginas 5
"""

import argparse
import hashlib
import html as html_mod
import logging
import os
import re
import sys
import time

# Encoding para Windows
sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from banco import inicializar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# =================================================================
# TEMAS DE BUSCA (saude suplementar)
# =================================================================

TEMAS_BUSCA = {
    "TEA/Autismo": '"plano de saude" autismo',
    "Oncologia": '"plano de saude" quimioterapia',
    "Home care": '"plano de saude" "home care"',
    "Cobertura de medicamento": '"plano de saude" medicamento cobertura negativa',
    "Cobertura de procedimento": '"plano de saude" procedimento cobertura negativa',
    "Reajuste/mensalidade": '"plano de saude" reajuste mensalidade',
    "Rescisao/cancelamento": '"plano de saude" rescisao cancelamento contrato',
    "Transplante": '"plano de saude" transplante',
    "Saude mental": '"plano de saude" psiquiatria "internacao psiquiatrica"',
    "Urgencia/emergencia": '"plano de saude" urgencia emergencia cobertura',
    "Reembolso": '"plano de saude" reembolso',
    "Protese/ortese": '"plano de saude" protese ortese implante',
    "Cirurgia": '"plano de saude" cirurgia bariatrica',
    "Rede credenciada": '"plano de saude" credenciado descredenciamento',
    "Carencia": '"plano de saude" carencia contratual',
    "Dano moral": '"plano de saude" "dano moral" indenizacao',
    "Rol da ANS": '"plano de saude" "rol de procedimentos" ANS taxatividade',
}

# URL base do CJSG
BASE_URL = "https://esaj.tjsp.jus.br/cjsg"
RESULTADOS_POR_PAGINA = 20


# =================================================================
# SELENIUM - DRIVER
# =================================================================

def _criar_driver():
    """Cria Chrome headless configurado para e-SAJ."""
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
    driver.set_page_load_timeout(45)
    return driver


# =================================================================
# SELENIUM - BUSCA E PAGINACAO
# =================================================================

def _executar_busca(driver, termo_busca):
    """Acessa o CJSG, preenche o termo e clica em pesquisar.
    Retorna o total de resultados encontrados ou 0 em caso de erro.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    log.info(f"  Acessando formulario e-SAJ...")
    driver.get(f"{BASE_URL}/consultaCompleta.do")
    time.sleep(3)

    # Preencher campo de busca
    campo = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.ID, "iddados.buscaInteiroTeor"))
    )
    campo.clear()
    campo.send_keys(termo_busca)

    # Marcar apenas acordaos (checkbox valor "A")
    try:
        cb = driver.find_element(
            By.CSS_SELECTOR, "input[value='A'][name='tipoDecisaoSelecionados']"
        )
        if not cb.is_selected():
            cb.click()
    except Exception:
        pass

    # Pesquisar
    btn = driver.find_element(By.ID, "pbSubmit")
    btn.click()

    # Aguardar resultados
    time.sleep(4)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.ementaClass2, .ementaClass"))
        )
    except Exception:
        log.warning("  Timeout aguardando resultados. Verificando pagina...")

    # Extrair total de resultados
    html = driver.page_source
    m = re.search(r"de\s+([\d.]+)\s*$", html[html.find("Resultados"):html.find("Resultados") + 200] if "Resultados" in html else "", re.MULTILINE)
    if m:
        total = int(m.group(1).replace(".", ""))
        log.info(f"  Total de resultados: {total:,}")
        return total

    # Alternativa: campo hidden
    m2 = re.search(r'totalResultadoAba[^"]*"\s*[^>]*value="(\d+)"', html)
    if m2:
        total = int(m2.group(1))
        log.info(f"  Total de resultados: {total:,}")
        return total

    log.warning("  Nao foi possivel determinar total de resultados")
    return 0


def _ir_proxima_pagina(driver, pagina_atual):
    """Clica no link da proxima pagina. Retorna True se conseguiu."""
    from selenium.webdriver.common.by import By

    try:
        # Botao ">" (proxima pagina) no paginador superior
        next_btn = driver.find_element(
            By.CSS_SELECTOR, 'div.trocaDePagina a[title="Pr\u00f3xima p\u00e1gina"]'
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
        time.sleep(0.5)
        next_btn.click()
        time.sleep(3)

        # Confirmar que mudou de pagina
        html = driver.page_source
        current = re.search(r'class="paginaAtual">\s*(\d+)', html)
        if current and int(current.group(1)) == pagina_atual + 1:
            return True

        # Fallback: clicar pelo numero da pagina
        page_link = driver.find_element(By.CSS_SELECTOR, f'div.trocaDePagina a[name="A{pagina_atual + 1}"]')
        page_link.click()
        time.sleep(3)
        return True

    except Exception as e:
        log.debug(f"  Erro na paginacao: {e}")
        return False


# =================================================================
# PARSING DO HTML
# =================================================================

def _parsear_resultados(html):
    """Extrai acordaos do HTML renderizado pelo Selenium.

    Cada resultado no e-SAJ tem:
    - Numero CNJ em <a class="esajLinkLogin downloadEmenta">
    - Ementa em <div class="mensagemSemFormatacao"> (texto plano, sem formatacao)
    - Metadados em <tr class="ementaClass2"> (Classe, Relator, Comarca, Orgao, Datas)
    - Ementa formatada em <div align="justify"><strong>Ementa: </strong>...</div>
    """
    acordaos = []
    vistos = set()

    # Estrategia: dividir por blocos de resultado usando o padrao de numeracao
    # Cada resultado comeca com <td class="ementaClass"><strong>N&nbsp;-</strong>
    blocos = re.split(r'<td[^>]*class="ementaClass"[^>]*>\s*<strong>\s*\d+', html)

    for bloco in blocos[1:]:  # Pular o que vem antes do primeiro resultado
        ac = _parsear_bloco(bloco)
        if ac and ac["numero_processo"] not in vistos:
            vistos.add(ac["numero_processo"])
            acordaos.append(ac)

    return acordaos


def _parsear_bloco(bloco):
    """Parseia um bloco individual de resultado."""
    # Numero do processo (CNJ)
    proc_match = re.search(r"(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})", bloco)
    if not proc_match:
        return None
    numero = proc_match.group(1)

    # Ementa: texto plano dentro de <div class="mensagemSemFormatacao">
    ementa = ""
    em_match = re.search(
        r'class="mensagemSemFormatacao">\s*(.*?)\s*</div>', bloco, re.DOTALL
    )
    if em_match:
        ementa = _limpar_html(em_match.group(1))

    # Se nao achou no div hidden, tentar na ementa formatada
    if not ementa:
        em_match2 = re.search(
            r'<strong>Ementa:\s*</strong>(.*?)(?:</div>|</td>)', bloco, re.DOTALL
        )
        if em_match2:
            ementa = _limpar_html(em_match2.group(1))

    # Metadados: extrair de <tr class="ementaClass2">
    classe = _extrair_meta(bloco, r"Classe/Assunto:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    relator = _extrair_meta(bloco, r"Relator\(a\):\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    comarca = _extrair_meta(bloco, r"Comarca:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    orgao = _extrair_meta(bloco, r"[OÓ]rg[aã]o\s+julgador:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    data_julg = _extrair_meta(bloco, r"Data do julgamento:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")
    data_pub = _extrair_meta(bloco, r"Data de publica[cç][aã]o:\s*</strong>\s*(.*?)\s*(?:</td>|<br)", "")

    # Limpar valores
    classe = _limpar_html(classe).strip()
    relator = _limpar_html(relator).strip()
    comarca = _limpar_html(comarca).strip()
    orgao = _limpar_html(orgao).strip()
    data_julg = _normalizar_data(_limpar_html(data_julg).strip())
    data_pub = _normalizar_data(_limpar_html(data_pub).strip())

    if not ementa and not classe:
        return None

    return {
        "numero_processo": numero,
        "classe_processual": classe,
        "relator": relator,
        "comarca": comarca,
        "orgao_julgador": orgao,
        "data_julgamento": data_julg,
        "data_publicacao": data_pub,
        "ementa": ementa.strip()[:10000],  # Limite de seguranca
    }


def _extrair_meta(bloco, pattern, default=""):
    """Extrai metadado do HTML usando regex."""
    m = re.search(pattern, bloco, re.DOTALL | re.IGNORECASE)
    return m.group(1) if m else default


def _limpar_html(texto):
    """Remove tags HTML e normaliza espacos."""
    if not texto:
        return ""
    texto = html_mod.unescape(texto)
    texto = re.sub(r"<br\s*/?>", "\n", texto)
    texto = re.sub(r"<[^>]+>", " ", texto)
    texto = re.sub(r"&nbsp;", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def _normalizar_data(data_str):
    """DD/MM/YYYY -> YYYY-MM-DD."""
    if not data_str:
        return ""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", data_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return data_str


# =================================================================
# CLASSIFICACAO DE TEMA
# =================================================================

def _classificar_tema(ementa):
    """Classifica o tema do acordao com base na ementa."""
    if not ementa:
        return None

    ementa_up = ementa.upper()

    mapa = {
        "TEA/Autismo": ["AUTIS", "TEA ", "ESPECTRO AUTISTA", "TERAPIA ABA", "TRANSTORNO DO ESPECTRO"],
        "Oncologia": ["QUIMIOTERAPIA", "RADIOTERAPIA", "ONCOLOG", "CANCER", "NEOPLASIA", "CÂNCER"],
        "Home care": ["HOME CARE", "INTERNACAO DOMICILIAR", "INTERNAÇÃO DOMICILIAR", "ATENDIMENTO DOMICILIAR"],
        "Cobertura de medicamento": ["MEDICAMENTO", "FORNECIMENTO DE MEDICAMENTO", "OFF LABEL", "OFF-LABEL"],
        "Reajuste/mensalidade": ["REAJUSTE", "MENSALIDADE", "FAIXA ETARIA", "FAIXA ETÁRIA", "SINISTRALIDADE"],
        "Rescisao/cancelamento": ["RESCISAO", "RESCISÃO", "CANCELAMENTO DO PLANO", "CANCELAMENTO DO CONTRATO"],
        "Transplante": ["TRANSPLANTE"],
        "Saude mental": ["PSIQUIATR", "DEPENDENCIA QUIMICA", "DEPENDÊNCIA QUÍMICA", "INTERNAÇÃO COMPULSÓRIA"],
        "Urgencia/emergencia": ["URGENCIA", "URGÊNCIA", "EMERGENCIA", "EMERGÊNCIA", "PRONTO SOCORRO"],
        "Reembolso": ["REEMBOLSO"],
        "Protese/ortese": ["PROTESE", "PRÓTESE", "ORTESE", "ÓRTESE", "IMPLANTE"],
        "Cirurgia": ["CIRURGIA BARIÁTRICA", "CIRURGIA BARIATRICA", "GASTROPLASTIA"],
        "Rede credenciada": ["CREDENCIADO", "DESCREDENCIAMENTO", "REDE REFERENCIADA"],
        "Carencia": ["CARENCIA", "CARÊNCIA"],
        "Dano moral": ["DANO MORAL", "DANOS MORAIS"],
        "Rol da ANS": ["ROL DA ANS", "ROL DE PROCEDIMENTOS", "TAXATIVIDADE DO ROL"],
    }

    for tema, termos in mapa.items():
        if any(t in ementa_up for t in termos):
            return tema

    if "PLANO DE SAUDE" in ementa_up or "PLANO DE SAÚDE" in ementa_up:
        if "COBERTURA" in ementa_up and "PROCEDIMENTO" in ementa_up:
            return "Cobertura de procedimento"
        if "COBERTURA" in ementa_up:
            return "Cobertura de procedimento"

    return None


# =================================================================
# IMPORTACAO VIA SELENIUM
# =================================================================

def importar_selenium(tema, termo_busca, limite=200, max_paginas=10, conn=None):
    """Importa ementas de um tema via Selenium + e-SAJ CJSG.

    Args:
        tema: Nome do tema (para classificacao)
        termo_busca: Termos de busca para o campo inteiro teor
        limite: Maximo de acordaos a importar
        max_paginas: Maximo de paginas a percorrer
        conn: Conexao SQLite (opcional)

    Returns:
        Tupla (importados, duplicados)
    """
    log.info(f"\n{'='*60}")
    log.info(f"  TEMA: {tema}")
    log.info(f"  Busca: {termo_busca}")
    log.info(f"  Limite: {limite} | Max paginas: {max_paginas}")
    log.info(f"{'='*60}")

    close_conn = False
    if conn is None:
        conn = inicializar()
        close_conn = True

    importados = 0
    duplicados = 0
    driver = None

    try:
        driver = _criar_driver()
        total_resultados = _executar_busca(driver, termo_busca)

        if total_resultados == 0:
            # Pode ser que nao conseguiu ler o total mas tem resultados
            html_check = driver.page_source
            if "ementaClass" not in html_check:
                log.warning("  Nenhum resultado encontrado.")
                return 0, 0

        pagina = 1
        tentativas_vazias = 0

        while pagina <= max_paginas and importados < limite:
            log.info(f"  Pagina {pagina}...")
            html = driver.page_source

            acordaos = _parsear_resultados(html)

            if not acordaos:
                tentativas_vazias += 1
                if tentativas_vazias >= 2:
                    log.info("  Sem mais resultados. Encerrando.")
                    break
                log.warning(f"  Pagina {pagina} sem resultados parseados. Tentando novamente...")
                time.sleep(3)
                pagina += 1
                if pagina <= max_paginas:
                    if not _ir_proxima_pagina(driver, pagina - 1):
                        break
                continue

            tentativas_vazias = 0
            novos_pagina = 0

            for ac in acordaos:
                if importados >= limite:
                    break

                # Hash para deduplicacao
                texto_hash = ac.get("ementa", "") or ac["numero_processo"]
                hash_em = hashlib.md5(texto_hash.encode("utf-8")).hexdigest()

                # Classificar tema pela ementa
                tema_class = _classificar_tema(ac.get("ementa", "")) or tema

                try:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO acordaos
                        (numero_processo, tribunal, orgao_julgador, relator,
                         data_julgamento, data_publicacao, classe_processual,
                         ementa, tema, fonte, hash_ementa)
                        VALUES (?, 'TJSP', ?, ?, ?, ?, ?, ?, ?, 'tjsp_esaj', ?)
                        """,
                        (
                            ac["numero_processo"],
                            ac.get("orgao_julgador", ""),
                            ac.get("relator", ""),
                            ac.get("data_julgamento", ""),
                            ac.get("data_publicacao", ""),
                            ac.get("classe_processual", ""),
                            ac.get("ementa", ""),
                            tema_class,
                            hash_em,
                        ),
                    )

                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        importados += 1
                        novos_pagina += 1
                    else:
                        duplicados += 1
                except Exception as e:
                    log.warning(f"  Erro inserindo {ac['numero_processo']}: {e}")
                    duplicados += 1

            conn.commit()
            log.info(
                f"  Pagina {pagina}: {len(acordaos)} encontrados, "
                f"{novos_pagina} novos, {importados} total importados"
            )

            # Proxima pagina
            pagina += 1
            if pagina <= max_paginas and importados < limite:
                if not _ir_proxima_pagina(driver, pagina - 1):
                    log.info("  Sem mais paginas disponiveis.")
                    break
                time.sleep(2)  # Rate limiting entre paginas

    except Exception as e:
        log.error(f"  Erro Selenium: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        if close_conn:
            conn.close()

    log.info(f"  Resultado {tema}: {importados} importados, {duplicados} duplicados")
    return importados, duplicados


# =================================================================
# IMPORTACAO EM LOTE (todos os temas)
# =================================================================

def importar_todos_temas(limite_por_tema=200, max_paginas=10):
    """Importa ementas de todos os temas via Selenium."""
    conn = inicializar()

    # Registrar no log de importacao
    conn.execute(
        """INSERT INTO log_importacao (fonte, tribunal, data_inicio, status)
           VALUES ('tjsp_esaj', 'TJSP', datetime('now'), 'em_andamento')"""
    )
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    total_imp = 0
    total_dup = 0

    try:
        for i, (tema, termo) in enumerate(TEMAS_BUSCA.items(), 1):
            log.info(f"\n[{i}/{len(TEMAS_BUSCA)}] Processando tema: {tema}")
            try:
                imp, dup = importar_selenium(
                    tema, termo,
                    limite=limite_por_tema,
                    max_paginas=max_paginas,
                    conn=conn,
                )
                total_imp += imp
                total_dup += dup
            except Exception as e:
                log.error(f"Erro no tema {tema}: {e}")

            # Rate limiting entre temas
            if i < len(TEMAS_BUSCA):
                log.info("  Aguardando 5s antes do proximo tema...")
                time.sleep(5)

        conn.execute(
            """UPDATE log_importacao
               SET data_fim=datetime('now'), registros_importados=?,
                   registros_duplicados=?, status='concluido'
               WHERE id=?""",
            (total_imp, total_dup, log_id),
        )
        conn.commit()

    except Exception as e:
        conn.execute(
            "UPDATE log_importacao SET status='erro', erro=? WHERE id=?",
            (str(e)[:500], log_id),
        )
        conn.commit()
    finally:
        conn.close()

    log.info(f"\n{'='*60}")
    log.info(f"  IMPORTACAO TJSP e-SAJ CONCLUIDA")
    log.info(f"  Total importados: {total_imp}")
    log.info(f"  Total duplicados: {total_dup}")
    log.info(f"{'='*60}")
    return total_imp, total_dup


# =================================================================
# FALLBACK DATAJUD
# =================================================================

def importar_datajud_tjsp(limite=500, conn=None):
    """Importa processos TJSP via DataJud (fallback sem Selenium)."""
    log.info("Usando DataJud como fonte para TJSP (sem ementas)...")
    log.info("Para ementas completas, use Selenium: --todos-temas ou --tema")

    from scripts.importar_datajud import importar_tribunal
    return importar_tribunal("TJSP", limite=limite, conn=conn)


# =================================================================
# CLI
# =================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Importar jurisprudencia do TJSP (e-SAJ + DataJud)"
    )
    parser.add_argument(
        "--todos-temas", action="store_true",
        help="Importar todos os temas de saude suplementar"
    )
    parser.add_argument(
        "--tema", type=str,
        help=f"Tema especifico. Opcoes: {', '.join(TEMAS_BUSCA.keys())}"
    )
    parser.add_argument(
        "--assunto", type=str,
        help="Termo de busca livre (ex: '\"plano de saude\" cirurgia')"
    )
    parser.add_argument(
        "--limite", type=int, default=200,
        help="Limite de acordaos por tema (default: 200)"
    )
    parser.add_argument(
        "--paginas", type=int, default=10,
        help="Max paginas por tema (default: 10, cada pagina = 20 resultados)"
    )
    parser.add_argument(
        "--datajud", action="store_true",
        help="Usar DataJud como fonte (fallback sem ementa)"
    )
    args = parser.parse_args()

    if args.datajud:
        importar_datajud_tjsp(limite=args.limite)
        return

    # Verificar Selenium
    try:
        from selenium import webdriver
    except ImportError:
        log.error("Selenium nao instalado! Use: pip install selenium")
        log.info("Alternativa: python scripts/importar_tjsp.py --datajud")
        return

    if args.todos_temas:
        importar_todos_temas(limite_por_tema=args.limite, max_paginas=args.paginas)

    elif args.tema:
        if args.tema in TEMAS_BUSCA:
            importar_selenium(
                args.tema, TEMAS_BUSCA[args.tema],
                limite=args.limite, max_paginas=args.paginas
            )
        else:
            log.error(f"Tema desconhecido: {args.tema}")
            log.info(f"Temas disponiveis: {', '.join(TEMAS_BUSCA.keys())}")

    elif args.assunto:
        importar_selenium(
            "Busca livre", args.assunto,
            limite=args.limite, max_paginas=args.paginas
        )

    else:
        parser.print_help()
        print("\nExemplos:")
        print('  python scripts/importar_tjsp.py --tema "TEA/Autismo" --limite 40')
        print("  python scripts/importar_tjsp.py --todos-temas --limite 200")
        print('  python scripts/importar_tjsp.py --assunto "plano de saude cirurgia"')


if __name__ == "__main__":
    main()

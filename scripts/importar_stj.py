"""
scripts/importar_stj.py — Importa acórdãos do STJ via Portal de Dados Abertos.

Fonte: https://dadosabertos.web.stj.jus.br/
Datasets:
    1) Íntegras de Decisões Terminativas e Acórdãos do Diário da Justiça
    2) Espelhos de Acórdãos (por turma/seção)

Estrutura dos dados (Íntegras):
    - metadados{DATA}.json: Array JSON com metadados (processo, ministro, teor, assuntos)
    - {DATA}.zip: ZIPs com TXTs do inteiro teor (SeqDocumento.txt)

Estrutura dos dados (Espelhos):
    - {DATA}.json: Array JSON com espelhos completos (ementa, decisão, legislação, etc.)

Filtra por assuntos CNJ de saúde suplementar (Íntegras) ou palavras-chave na ementa (Espelhos).

Uso:
    python scripts/importar_stj.py --listar              # Listar períodos disponíveis
    python scripts/importar_stj.py --importar             # Importar último mês de metadados
    python scripts/importar_stj.py --importar --meses 6   # Últimos 6 meses
    python scripts/importar_stj.py --importar --com-integra  # Baixar também inteiro teor (ZIPs grandes)
    python scripts/importar_stj.py --espelhos              # Importar espelhos 3ª+4ª turma (3 meses)
    python scripts/importar_stj.py --espelhos --turmas todas --meses 6  # Todas as turmas, 6 meses
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta

# Encoding seguro no Windows
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from banco import inicializar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CKAN_BASE = "https://dadosabertos.web.stj.jus.br/api/3/action"
DATASET_ID = "integras-de-decisoes-terminativas-e-acordaos-do-diario-da-justica"

# Códigos de assuntos CNJ — saúde suplementar
# Formato no STJ: "00014.05916.05954." (hierarquia separada por pontos)
# 14 = Direito do Consumidor, 5916 = Contratos de Consumo, 5954 = Planos de Saúde
ASSUNTOS_SAUDE_PREFIXOS = [
    "00014.05916.05954",   # Planos de Saúde (genérico)
    "00014.05916.05943",   # Saúde (genérico consumidor)
    "00014.05853",         # Responsabilidade do Fornecedor (inclui saúde)
    "06233",               # Planos de Saúde (código plano)
    "12222", "12223", "12224", "12225",  # Reajuste, Cobertura, Carência, Tratamento
    "12482", "12486", "12487", "12488", "12489", "12490",  # Saúde Suplementar detalhado
]

# Termos no campo 'processo' que indicam classe processual
CLASSES_MAPA = {
    "REsp": "Recurso Especial",
    "AREsp": "Agravo em Recurso Especial",
    "AgInt": "Agravo Interno",
    "AgRg": "Agravo Regimental",
    "EDcl": "Embargos de Declaração",
    "RMS": "Recurso em Mandado de Segurança",
    "HC": "Habeas Corpus",
    "MC": "Medida Cautelar",
    "Rcl": "Reclamação",
}


# =====================================================================
# Espelhos de Acórdãos — Datasets por turma/seção
# =====================================================================

ESPELHOS_DATASETS = {
    # Direito privado (prioritárias para saúde suplementar)
    "terceira-turma":  "espelhos-de-acordaos-terceira-turma",
    "quarta-turma":    "espelhos-de-acordaos-quarta-turma",
    # Demais turmas e seções
    "primeira-turma":  "espelhos-de-acordaos-primeira-turma",
    "segunda-turma":   "espelhos-de-acordaos-segunda-turma",
    "quinta-turma":    "espelhos-de-acordaos-quinta-turma",
    "sexta-turma":     "espelhos-de-acordaos-sexta-turma",
    "primeira-secao":  "espelhos-de-acordaos-primeira-secao",
    "segunda-secao":   "espelhos-de-acordaos-segunda-secao",
    "terceira-secao":  "espelhos-de-acordaos-terceira-secao",
    "corte-especial":  "espelhos-de-acordaos-corte-especial",
}

TURMAS_PRIVADO = ["terceira-turma", "quarta-turma"]

# Palavras-chave para filtrar espelhos por saúde suplementar
PALAVRAS_SAUDE = [
    # Saúde suplementar geral
    "PLANO DE SAUDE", "PLANO DE SAÚDE", "SAUDE SUPLEMENTAR", "SAÚDE SUPLEMENTAR",
    "OPERADORA DE SAUDE", "OPERADORA DE SAÚDE", "SEGURO SAUDE", "SEGURO SAÚDE",
    "SEGURO-SAUDE", "SEGURO-SAÚDE", "ASSISTENCIA MEDICA", "ASSISTÊNCIA MÉDICA",
    "COBERTURA MEDICA", "COBERTURA MÉDICA", "PLANO HOSPITALAR",
    # ANS / regulação
    "AGENCIA NACIONAL DE SAUDE", "AGÊNCIA NACIONAL DE SAÚDE",
    "ROL DE PROCEDIMENTOS", "ROL DA ANS",
    # Operadoras conhecidas
    "UNIMED", "AMIL", "BRADESCO SAUDE", "BRADESCO SAÚDE", "SULAMERICA",
    "SUL AMERICA", "SUL AMÉRICA", "HAPVIDA", "NOTRE DAME", "NOTREDAME",
    "PREVENT SENIOR", "GOLDEN CROSS", "INTERMÉDICA", "INTERMEDICA",
    "CASSI", "GEAP", "ASSIM SAUDE", "ASSIM SAÚDE",
    # Termos médicos/procedimentais ligados a saúde suplementar
    "HOME CARE", "INTERNACAO DOMICILIAR", "INTERNAÇÃO DOMICILIAR",
    "QUIMIOTERAPIA", "RADIOTERAPIA", "CIRURGIA BARIATRICA", "CIRURGIA BARIÁTRICA",
    "TRATAMENTO ONCOLOGICO", "TRATAMENTO ONCOLÓGICO",
    "TRANSPLANTE", "PROTESE", "PRÓTESE", "ORTESE", "ÓRTESE",
    "TERAPIA ABA", "TRANSTORNO DO ESPECTRO AUTISTA", "TEA",
    "STENT", "MEDICAMENTO DE ALTO CUSTO",
    # Temas jurídicos típicos
    "REAJUSTE POR FAIXA ETARIA", "REAJUSTE POR FAIXA ETÁRIA",
    "COBERTURA CONTRATUAL", "NEGATIVA DE COBERTURA",
    "CARENCIA CONTRATUAL", "CARÊNCIA CONTRATUAL",
    "DESCREDENCIAMENTO", "REEMBOLSO",
    "URGENCIA E EMERGENCIA", "URGÊNCIA E EMERGÊNCIA",
    "DOENCA PREEXISTENTE", "DOENÇA PREEXISTENTE",
    "DANO MORAL", "OBRIGACAO DE FAZER", "OBRIGAÇÃO DE FAZER",
    "TUTELA DE URGENCIA", "TUTELA DE URGÊNCIA",
]

# Classificação de temas (14 categorias conforme MEMORY)
TEMAS_ESPELHOS = {
    "TEA/Autismo": [
        "AUTIS", "TEA", "TERAPIA ABA", "ESPECTRO AUTISTA", "ACOMPANHANTE TERAPEUTICO",
        "ACOMPANHANTE TERAPÊUTICO", "FONOAUDIOLOG",
    ],
    "Cobertura de procedimento": [
        "COBERTURA", "ROL DE PROCEDIMENTO", "ROL DA ANS", "PROCEDIMENTO",
        "NEGATIVA DE COBERTURA", "OBRIGACAO DE FAZER", "OBRIGAÇÃO DE FAZER",
        "FORNECIMENTO DE",
    ],
    "Oncologia": [
        "QUIMIOTERAPIA", "ONCOLOG", "CANCER", "CÂNCER", "RADIOTERAPIA",
        "NEOPLASIA", "TUMOR",
    ],
    "Home care": [
        "HOME CARE", "DOMICILIAR", "INTERNACAO DOMICILIAR", "INTERNAÇÃO DOMICILIAR",
    ],
    "Cobertura de medicamento": [
        "MEDICAMENTO", "FARMACO", "FÁRMACO", "OFF LABEL", "OFF-LABEL",
    ],
    "Reajuste/mensalidade": [
        "REAJUSTE", "MENSALIDADE", "FAIXA ETARIA", "FAIXA ETÁRIA",
        "SINISTRALIDADE", "COPARTICIPACAO", "COPARTICIPAÇÃO",
    ],
    "Rescisao/cancelamento": [
        "RESCISAO", "RESCISÃO", "CANCELAMENTO", "RESILICAO", "RESILIÇÃO",
        "INADIMPLENCIA", "INADIMPLÊNCIA",
    ],
    "Contratual": [
        "CONTRATUAL", "CLAUSULA", "CLÁUSULA", "CONTRATO",
    ],
    "Urgencia/emergencia": [
        "URGENCIA", "URGÊNCIA", "EMERGENCIA", "EMERGÊNCIA",
    ],
    "Rede credenciada": [
        "CREDENCIAD", "DESCREDENCIAMENTO", "REEMBOLSO", "REDE REFERENCIADA",
    ],
    "Transplante": [
        "TRANSPLANT",
    ],
    "Protese/ortese": [
        "PROTESE", "PRÓTESE", "ORTESE", "ÓRTESE", "STENT", "IMPLANTE",
    ],
    "Cirurgia": [
        "CIRURGIA", "BARIATRICA", "BARIÁTRICA", "CIRURGICO", "CIRÚRGICO",
    ],
    "Dano moral": [
        "DANO MORAL", "DANOS MORAIS", "INDENIZA",
    ],
}


def _eh_saude_ementa(ementa):
    """Verifica se a ementa contém termos de saúde suplementar."""
    if not ementa:
        return False
    texto = ementa.upper()
    return any(p in texto for p in PALAVRAS_SAUDE)


def _classificar_tema_espelho(ementa):
    """Classifica tema de um espelho baseado na ementa."""
    if not ementa:
        return None
    texto = ementa.upper()
    for tema, termos in TEMAS_ESPELHOS.items():
        if any(t in texto for t in termos):
            return tema
    return "Cobertura de procedimento"  # Default para saúde suplementar genérica


def _classificar_resultado_espelho(ementa, decisao, classe):
    """
    Tenta classificar resultado sob perspectiva da operadora.

    Lógica simplificada:
    - No STJ, geralmente a operadora é recorrente (REsp, AREsp)
    - "NEGOU PROVIMENTO" ao recurso da operadora = desfavoravel
    - "DEU PROVIMENTO" ao recurso da operadora = favoravel
    - "PARCIAL PROVIMENTO" = parcial

    Nota: isso é uma heurística. A classificação final deve ser revisada.
    """
    if not decisao and not ementa:
        return None

    texto = f"{decisao or ''} {ementa or ''}".upper()

    # Termos que indicam resultado
    negou = any(t in texto for t in [
        "NEGOU PROVIMENTO", "NEGAR PROVIMENTO", "NEGARAM PROVIMENTO",
        "DESPROVIMENTO", "RECURSO DESPROVIDO", "IMPROVIMENTO",
        "NAO PROVIMENTO", "NÃO PROVIMENTO", "RECURSO NAO PROVIDO",
        "RECURSO NÃO PROVIDO",
    ])
    deu = any(t in texto for t in [
        "DEU PROVIMENTO", "DAR PROVIMENTO", "DERAM PROVIMENTO",
        "RECURSO PROVIDO",
    ])
    parcial = any(t in texto for t in [
        "PARCIAL PROVIMENTO", "PARCIALMENTE PROVIDO", "EM PARTE",
        "PROVIMENTO PARCIAL",
    ])

    # Em embargos de declaração, o resultado é menos claro
    if classe and "EMBARGO" in classe.upper():
        return None

    # Parcial tem prioridade (pode aparecer junto com "provimento")
    if parcial:
        return "parcial"

    # No STJ em saúde suplementar, o recurso é geralmente da operadora
    # "negou provimento" = manteve decisão do TJSP = geralmente desfavorável p/ operadora
    if negou and not deu:
        return "desfavoravel"
    if deu and not negou:
        return "favoravel"

    return None


def _parse_data_espelho(data_str):
    """
    Converte datas dos espelhos para YYYY-MM-DD.
    Formatos: '20251215' ou 'DJEN       DATA:18/12/2025'
    """
    if not data_str:
        return None

    data_str = str(data_str).strip()

    # Formato YYYYMMDD
    if re.match(r'^\d{8}$', data_str):
        try:
            return f"{data_str[:4]}-{data_str[4:6]}-{data_str[6:8]}"
        except:
            return data_str

    # Formato "DJEN       DATA:18/12/2025"
    m = re.search(r'DATA:\s*(\d{2})/(\d{2})/(\d{4})', data_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Formato DD/MM/YYYY genérico
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', data_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return data_str


def _normalizar_legislacao(ref_leg):
    """Normaliza o campo referenciasLegislativas (pode ser string ou lista)."""
    if not ref_leg:
        return None
    if isinstance(ref_leg, list):
        return json.dumps(ref_leg, ensure_ascii=False)
    return str(ref_leg).strip() if str(ref_leg).strip() else None


# =====================================================================
# Funções auxiliares genéricas
# =====================================================================

def fetch_json(url, timeout=60):
    """Fetch JSON de uma URL."""
    req = urllib.request.Request(url, headers={"User-Agent": "ModuloJurisprudencia/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        log.error(f"HTTP {e.code}: {url[:100]}")
        return None
    except Exception as e:
        log.error(f"Erro: {e}")
        return None


def fetch_raw(url, timeout=120):
    """Fetch bytes de uma URL."""
    req = urllib.request.Request(url, headers={"User-Agent": "ModuloJurisprudencia/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        log.error(f"Erro download: {e}")
        return None


def listar_periodos():
    """Lista períodos disponíveis no dataset."""
    log.info("Consultando períodos disponíveis no STJ Dados Abertos...")

    data = fetch_json(f"{CKAN_BASE}/package_show?id={DATASET_ID}")
    if not data or not data.get("success"):
        log.error("Falha ao acessar dataset")
        return []

    resources = data["result"]["resources"]
    json_resources = [r for r in resources if r.get("format", "").upper() == "JSON"]

    periodos = []
    for r in json_resources:
        name = r.get("name", "")
        # Extrair período do nome (metadados202203, metadados20260319, etc.)
        m = re.search(r'metadados(\d{6,8})', name)
        if m:
            periodo = m.group(1)
            size_mb = (r.get("size", 0) or 0) / 1024 / 1024
            periodos.append({
                "periodo": periodo,
                "url": r["url"],
                "size_mb": size_mb,
                "name": name,
            })

    periodos.sort(key=lambda x: x["periodo"], reverse=True)

    log.info(f"\n=== {len(periodos)} períodos de metadados ===")
    for p in periodos[:20]:
        log.info(f"  {p['periodo']} — {p['size_mb']:.1f} MB — {p['name']}")

    return periodos


def _eh_saude(assuntos_str):
    """Verifica se os assuntos indicam saúde suplementar."""
    if not assuntos_str:
        return False

    for prefixo in ASSUNTOS_SAUDE_PREFIXOS:
        if prefixo in str(assuntos_str):
            return True

    return False


def _classificar_tema_teor(teor, processo):
    """Classificação básica de tema pelo teor/processo."""
    if not teor and not processo:
        return None

    texto = f"{teor or ''} {processo or ''}".upper()

    mapa = {
        "TEA/Autismo": ["AUTIS", "TEA", "ABA"],
        "Oncologia": ["QUIMIOTERAPIA", "ONCOLOG", "CANCER"],
        "Home care": ["HOME CARE", "DOMICILIAR"],
        "Cobertura de medicamento": ["MEDICAMENTO"],
        "Reajuste/mensalidade": ["REAJUSTE"],
        "Rescisao/cancelamento": ["RESCISAO", "CANCELAMENTO"],
    }

    for tema, termos in mapa.items():
        if any(t in texto for t in termos):
            return tema

    return None


def _extrair_classe(processo_str):
    """Extrai classe processual da string de processo do STJ."""
    if not processo_str:
        return "", ""

    # "AREsp 2935515" → classe "Agravo em Recurso Especial", numero extraído
    for sigla, nome in CLASSES_MAPA.items():
        if processo_str.strip().startswith(sigla):
            return nome, processo_str.strip()

    return "", processo_str.strip()


def importar_periodo(periodo_info, conn=None):
    """Importa metadados de um período do STJ."""
    url = periodo_info["url"]
    periodo = periodo_info["periodo"]
    size = periodo_info.get("size_mb", 0)

    log.info(f"\n--- Importando STJ período {periodo} ({size:.1f} MB) ---")

    close_conn = False
    if conn is None:
        conn = inicializar()
        close_conn = True

    importados = 0
    duplicados = 0
    filtrados = 0
    total = 0

    try:
        # Baixar metadados JSON
        log.info(f"  Baixando {url}...")
        raw = fetch_raw(url, timeout=180)
        if not raw:
            log.error("  Falha no download")
            return 0, 0

        # Decodificar
        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                content = raw.decode(enc)
                break
            except:
                continue
        else:
            log.error("  Falha na decodificação")
            return 0, 0

        log.info(f"  Parseando JSON ({len(content)} chars)...")
        records = json.loads(content)
        total = len(records)
        log.info(f"  {total} registros no período")

        batch = []
        for rec in records:
            # Filtrar por assuntos de saúde
            assuntos = rec.get("assuntos", "")
            if not _eh_saude(assuntos):
                filtrados += 1
                continue

            # Extrair campos
            seq = rec.get("SeqDocumento", "")
            data_pub = rec.get("dataPublicacao", "")
            tipo = rec.get("tipoDocumento", "")
            num_registro = rec.get("numeroRegistro", "")
            processo = rec.get("processo", "")
            data_receb = rec.get("dataRecebimento", "")
            data_dist = rec.get("dataDistribuição", rec.get("dataDistribuicao", ""))
            ministro = rec.get("NM_MINISTRO", rec.get("ministro", ""))
            recurso = rec.get("recurso", "")
            teor = rec.get("teor", "")
            desc_mono = rec.get("descricaoMonocratica", "")

            classe, proc_formatado = _extrair_classe(processo)

            # Montar número de processo CNJ (se disponível no registro)
            # O STJ usa "AREsp 2935515", não CNJ. Guardar como está.
            numero_processo = proc_formatado or num_registro

            # Ementa: usar teor + descrição monocrática como resumo
            ementa = ""
            if teor:
                ementa = f"Teor: {teor}"
            if desc_mono:
                ementa += f"\n{desc_mono}" if ementa else desc_mono

            # Hash para dedup
            hash_em = hashlib.md5(
                f"{numero_processo}_{data_pub}_{tipo}".encode("utf-8")
            ).hexdigest()

            # Tema
            tema = _classificar_tema_teor(teor, processo)

            batch.append((
                numero_processo, "STJ", "", ministro,
                data_pub, data_pub, classe or tipo,
                ementa, None, tema, "stj_dados_abertos",
                None, hash_em, str(assuntos),
                str(seq),  # Guardar SeqDocumento para baixar inteiro teor depois
            ))

            if len(batch) >= 500:
                _inserir_batch(conn, batch)
                ins = conn.execute("SELECT changes()").fetchone()[0]
                importados += ins
                duplicados += len(batch) - ins
                batch = []
                log.info(f"  {importados} importados, {duplicados} dup, "
                         f"{filtrados}/{total} filtrados...")

        if batch:
            _inserir_batch(conn, batch)
            ins = conn.execute("SELECT changes()").fetchone()[0]
            importados += ins
            duplicados += len(batch) - ins

        conn.commit()

    except Exception as e:
        log.error(f"  Erro: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if close_conn:
            conn.close()

    log.info(f"  Período {periodo}: {importados} importados, {duplicados} dup, "
             f"{filtrados} não-saúde de {total} total")
    return importados, duplicados


def _inserir_batch(conn, batch):
    """Insere lote de acórdãos."""
    conn.executemany("""
        INSERT OR IGNORE INTO acordaos
        (numero_processo, tribunal, orgao_julgador, relator,
         data_julgamento, data_publicacao, classe_processual,
         ementa, inteiro_teor, tema, fonte,
         fonte_url, hash_ementa, assuntos_cnj)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9],
           b[10], b[14], b[12], b[13]) for b in batch])


def importar_stj(meses=1, com_integra=False):
    """Importa metadados do STJ dos últimos N meses."""
    periodos = listar_periodos()
    if not periodos:
        return False

    conn = inicializar()

    # Registrar importação
    conn.execute("""
        INSERT INTO log_importacao (fonte, tribunal, data_inicio, status)
        VALUES ('stj_dados_abertos', 'STJ', datetime('now'), 'em_andamento')
    """)
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    total_imp = 0
    total_dup = 0

    try:
        # Selecionar períodos (mais recentes primeiro)
        # Cada mês pode ter múltiplos arquivos (diários: 20260319, 20260318...)
        # ou mensais (202203, 202204...)
        periodos_sel = periodos[:meses * 25]  # ~25 arquivos por mês (dias úteis)

        log.info(f"\nImportando {len(periodos_sel)} períodos...")

        for p in periodos_sel:
            try:
                imp, dup = importar_periodo(p, conn=conn)
                total_imp += imp
                total_dup += dup
            except Exception as e:
                log.error(f"Erro no período {p['periodo']}: {e}")
            time.sleep(1)

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

    log.info(f"\n=== STJ Dados Abertos ===")
    log.info(f"  Total importados: {total_imp}")
    log.info(f"  Total duplicados: {total_dup}")
    return True


# =====================================================================
# Espelhos de Acórdãos — Importação
# =====================================================================

def _listar_recursos_espelhos(dataset_id, meses=3):
    """Lista recursos JSON dos espelhos de acórdãos, filtrando por meses recentes."""
    log.info(f"  Consultando dataset: {dataset_id}")

    data = fetch_json(f"{CKAN_BASE}/package_show?id={dataset_id}")
    if not data or not data.get("success"):
        log.error(f"  Falha ao acessar dataset {dataset_id}")
        return []

    resources = data["result"]["resources"]

    # Filtrar apenas JSONs mensais (YYYYMMDD.json)
    json_resources = []
    for r in resources:
        fmt = (r.get("format", "") or "").upper()
        name = r.get("name", "")
        if fmt != "JSON":
            continue
        # Extrair data do nome do recurso (20260228.json, etc.)
        m = re.search(r'(\d{8})\.json', name)
        if not m:
            # Tentar só pelo nome numérico
            m = re.search(r'^(\d{8})$', name.replace('.json', ''))
        if not m:
            continue
        data_str = m.group(1)
        json_resources.append({
            "data": data_str,
            "url": r["url"],
            "name": name,
            "size_mb": (r.get("size", 0) or 0) / 1024 / 1024,
        })

    json_resources.sort(key=lambda x: x["data"], reverse=True)

    # Filtrar pelos últimos N meses
    if meses > 0:
        cutoff = datetime.now() - timedelta(days=meses * 31)
        cutoff_str = cutoff.strftime("%Y%m%d")
        json_resources = [r for r in json_resources if r["data"] >= cutoff_str]

    log.info(f"  {len(json_resources)} recursos JSON nos últimos {meses} meses")
    return json_resources


def _inserir_batch_espelhos(conn, batch):
    """Insere lote de espelhos na tabela acordaos."""
    conn.executemany("""
        INSERT OR IGNORE INTO acordaos
        (numero_processo, tribunal, orgao_julgador, relator,
         data_julgamento, data_publicacao, classe_processual,
         ementa, inteiro_teor, tema, resultado_operadora,
         legislacao_citada, fonte, fonte_url, hash_ementa)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, batch)


def importar_espelhos(turmas="privado", meses=3):
    """
    Importa espelhos de acórdãos do STJ.

    Args:
        turmas: "privado" (3ª+4ª turma) ou "todas"
        meses: Últimos N meses de dados
    """
    log.info(f"\n{'='*60}")
    log.info(f"IMPORTANDO ESPELHOS DE ACÓRDÃOS DO STJ")
    log.info(f"Turmas: {turmas} | Últimos {meses} meses")
    log.info(f"{'='*60}")

    # Selecionar datasets
    if turmas == "privado":
        datasets = {k: v for k, v in ESPELHOS_DATASETS.items() if k in TURMAS_PRIVADO}
    else:
        datasets = ESPELHOS_DATASETS

    conn = inicializar()

    # Registrar importação
    conn.execute("""
        INSERT INTO log_importacao (fonte, tribunal, data_inicio, status)
        VALUES ('stj_espelhos', 'STJ', datetime('now'), 'em_andamento')
    """)
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    total_registros = 0
    total_saude = 0
    total_importados = 0
    total_duplicados = 0
    stats_turma = {}
    stats_tema = {}
    stats_ministro = {}
    stats_resultado = {}

    try:
        for turma_key, dataset_id in datasets.items():
            log.info(f"\n--- {turma_key.upper().replace('-', ' ')} ---")
            turma_imp = 0
            turma_dup = 0
            turma_saude = 0

            recursos = _listar_recursos_espelhos(dataset_id, meses=meses)
            if not recursos:
                log.warning(f"  Nenhum recurso encontrado para {turma_key}")
                continue

            for recurso in recursos:
                url = recurso["url"]
                data_ref = recurso["data"]
                log.info(f"  Baixando {recurso['name']} ({recurso['size_mb']:.1f} MB)...")

                raw = fetch_raw(url, timeout=300)
                if not raw:
                    log.error(f"  Falha no download de {recurso['name']}")
                    continue

                # Decodificar (tentar utf-8 primeiro, depois latin-1)
                content = None
                for enc in ["utf-8", "latin-1", "cp1252"]:
                    try:
                        content = raw.decode(enc)
                        break
                    except:
                        continue

                if not content:
                    log.error(f"  Falha na decodificacao de {recurso['name']}")
                    continue

                try:
                    records = json.loads(content)
                except json.JSONDecodeError as e:
                    log.error(f"  JSON invalido em {recurso['name']}: {e}")
                    continue

                if not isinstance(records, list):
                    log.warning(f"  Formato inesperado em {recurso['name']}: {type(records)}")
                    continue

                total_registros += len(records)
                log.info(f"  {len(records)} registros em {recurso['name']}")

                batch = []
                for rec in records:
                    ementa = rec.get("ementa", "") or ""
                    termos_aux = rec.get("termosAuxiliares", "") or ""
                    info_compl = rec.get("informacoesComplementares", "") or ""
                    tema_campo = rec.get("tema", "") or ""

                    # Filtrar por saúde suplementar (ementa + campos auxiliares)
                    texto_busca = f"{ementa} {termos_aux} {info_compl} {tema_campo}"
                    if not _eh_saude_ementa(texto_busca):
                        continue

                    turma_saude += 1

                    # Extrair campos
                    numero = str(rec.get("numeroProcesso", "")).strip()
                    num_registro = str(rec.get("numeroRegistro", "")).strip()
                    sigla_classe = rec.get("siglaClasse", "") or ""
                    desc_classe = rec.get("descricaoClasse", "") or ""
                    orgao = rec.get("nomeOrgaoJulgador", "") or ""
                    ministro = rec.get("ministroRelator", "") or ""
                    decisao = rec.get("decisao", "") or ""
                    jurisp_citada = rec.get("jurisprudenciaCitada", "") or ""
                    notas = rec.get("notas", "") or ""
                    tese_juridica = rec.get("teseJuridica", "") or ""
                    ref_leg = rec.get("referenciasLegislativas", "")
                    data_decisao = _parse_data_espelho(rec.get("dataDecisao", ""))
                    data_pub = _parse_data_espelho(rec.get("dataPublicacao", ""))

                    # Classe processual formatada
                    classe_fmt = desc_classe if desc_classe else sigla_classe

                    # Número do processo para o banco
                    # Usar número registro se disponível (mais completo)
                    numero_proc = numero or num_registro

                    # Hash para deduplicação (baseado na ementa)
                    hash_em = hashlib.md5(
                        ementa.strip().encode("utf-8", errors="replace")
                    ).hexdigest()

                    # Classificar tema
                    tema = _classificar_tema_espelho(ementa)

                    # Classificar resultado
                    resultado = _classificar_resultado_espelho(ementa, decisao, classe_fmt)

                    # Montar inteiro_teor com decisão + jurisprudência + notas + tese
                    inteiro_teor_parts = []
                    if decisao:
                        inteiro_teor_parts.append(f"DECISAO: {decisao}")
                    if jurisp_citada:
                        inteiro_teor_parts.append(f"JURISPRUDENCIA CITADA: {jurisp_citada}")
                    if tese_juridica:
                        inteiro_teor_parts.append(f"TESE JURIDICA: {tese_juridica}")
                    if notas:
                        inteiro_teor_parts.append(f"NOTAS: {notas}")
                    if termos_aux:
                        inteiro_teor_parts.append(f"TERMOS AUXILIARES: {termos_aux}")
                    inteiro_teor = "\n\n".join(inteiro_teor_parts) if inteiro_teor_parts else None

                    # Legislação citada
                    legislacao = _normalizar_legislacao(ref_leg)

                    # URL do processo no STJ
                    fonte_url_proc = (
                        f"https://processo.stj.jus.br/processo/pesquisa/"
                        f"?tipoPesquisa=tipoPesquisaNumeroRegistro"
                        f"&termo={num_registro}" if num_registro else None
                    )

                    batch.append((
                        numero_proc,       # numero_processo
                        "STJ",             # tribunal
                        orgao,             # orgao_julgador
                        ministro,          # relator
                        data_decisao,      # data_julgamento
                        data_pub,          # data_publicacao
                        classe_fmt,        # classe_processual
                        ementa,            # ementa
                        inteiro_teor,      # inteiro_teor
                        tema,              # tema
                        resultado,         # resultado_operadora
                        legislacao,        # legislacao_citada
                        "stj_espelhos",    # fonte
                        fonte_url_proc,    # fonte_url
                        hash_em,           # hash_ementa
                    ))

                    # Estatísticas
                    stats_tema[tema] = stats_tema.get(tema, 0) + 1
                    if ministro:
                        stats_ministro[ministro] = stats_ministro.get(ministro, 0) + 1
                    if resultado:
                        stats_resultado[resultado] = stats_resultado.get(resultado, 0) + 1

                    # Inserir em lotes
                    if len(batch) >= 500:
                        before = conn.execute("SELECT COUNT(*) FROM acordaos WHERE fonte='stj_espelhos'").fetchone()[0]
                        _inserir_batch_espelhos(conn, batch)
                        conn.commit()
                        after = conn.execute("SELECT COUNT(*) FROM acordaos WHERE fonte='stj_espelhos'").fetchone()[0]
                        ins = after - before
                        dup = len(batch) - ins
                        turma_imp += ins
                        turma_dup += dup
                        batch = []
                        log.info(f"    Lote parcial: +{ins} novos, {dup} dup")

                # Inserir resto do lote
                if batch:
                    before = conn.execute("SELECT COUNT(*) FROM acordaos WHERE fonte='stj_espelhos'").fetchone()[0]
                    _inserir_batch_espelhos(conn, batch)
                    conn.commit()
                    after = conn.execute("SELECT COUNT(*) FROM acordaos WHERE fonte='stj_espelhos'").fetchone()[0]
                    ins = after - before
                    dup = len(batch) - ins
                    turma_imp += ins
                    turma_dup += dup
                    batch = []

                time.sleep(0.5)  # Gentil com o servidor

            total_saude += turma_saude
            total_importados += turma_imp
            total_duplicados += turma_dup
            stats_turma[turma_key] = {"importados": turma_imp, "duplicados": turma_dup, "saude": turma_saude}

            log.info(f"  {turma_key}: {turma_saude} de saude, "
                     f"{turma_imp} importados, {turma_dup} duplicados")

        # Atualizar log
        conn.execute("""
            UPDATE log_importacao
            SET data_fim=datetime('now'), registros_importados=?,
                registros_duplicados=?, status='concluido'
            WHERE id=?
        """, (total_importados, total_duplicados, log_id))
        conn.commit()

    except Exception as e:
        log.error(f"Erro na importacao: {e}")
        import traceback
        traceback.print_exc()
        conn.execute("UPDATE log_importacao SET status='erro', erro=? WHERE id=?",
                     (str(e), log_id))
        conn.commit()
    finally:
        conn.close()

    # Exibir estatísticas
    log.info(f"\n{'='*60}")
    log.info(f"ESTATISTICAS DA IMPORTACAO DE ESPELHOS")
    log.info(f"{'='*60}")
    log.info(f"  Total de registros lidos:     {total_registros}")
    log.info(f"  Filtrados como saude:         {total_saude}")
    log.info(f"  Novos importados:             {total_importados}")
    log.info(f"  Duplicados ignorados:         {total_duplicados}")

    log.info(f"\n  --- Por turma ---")
    for turma, s in stats_turma.items():
        log.info(f"  {turma:25s}: {s['saude']:4d} saude, {s['importados']:4d} novos, {s['duplicados']:4d} dup")

    if stats_tema:
        log.info(f"\n  --- Por tema ---")
        for tema, qtd in sorted(stats_tema.items(), key=lambda x: -x[1]):
            log.info(f"  {tema:35s}: {qtd:4d}")

    if stats_ministro:
        log.info(f"\n  --- Por ministro (top 15) ---")
        top_min = sorted(stats_ministro.items(), key=lambda x: -x[1])[:15]
        for min_nome, qtd in top_min:
            log.info(f"  {min_nome:35s}: {qtd:4d}")

    if stats_resultado:
        log.info(f"\n  --- Por resultado (perspectiva operadora) ---")
        for res, qtd in sorted(stats_resultado.items(), key=lambda x: -x[1]):
            log.info(f"  {res:20s}: {qtd:4d}")

    return {
        "total_registros": total_registros,
        "total_saude": total_saude,
        "importados": total_importados,
        "duplicados": total_duplicados,
        "por_turma": stats_turma,
        "por_tema": stats_tema,
        "por_ministro": stats_ministro,
        "por_resultado": stats_resultado,
    }


def main():
    parser = argparse.ArgumentParser(description="Importar acórdãos STJ (Dados Abertos)")
    parser.add_argument("--listar", action="store_true", help="Listar períodos disponíveis")
    parser.add_argument("--importar", action="store_true", help="Importar metadados (íntegras)")
    parser.add_argument("--espelhos", action="store_true",
                        help="Importar espelhos de acórdãos (por turma)")
    parser.add_argument("--turmas", choices=["privado", "todas"], default="privado",
                        help="Turmas: privado=3a+4a (default), todas=todas turmas/seções")
    parser.add_argument("--meses", type=int, default=3, help="Últimos N meses (default: 3)")
    parser.add_argument("--com-integra", action="store_true",
                        help="Baixar inteiro teor (ZIPs grandes!)")
    args = parser.parse_args()

    if args.espelhos:
        importar_espelhos(turmas=args.turmas, meses=args.meses)
    elif args.listar:
        listar_periodos()
    elif args.importar:
        importar_stj(meses=args.meses, com_integra=args.com_integra)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

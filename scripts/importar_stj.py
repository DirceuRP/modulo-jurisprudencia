"""
scripts/importar_stj.py — Importa acórdãos do STJ via Portal de Dados Abertos.

Fonte: https://dadosabertos.web.stj.jus.br/
Dataset: Íntegras de Decisões Terminativas e Acórdãos do Diário da Justiça

Estrutura dos dados:
    - metadados{DATA}.json: Array JSON com metadados (processo, ministro, teor, assuntos)
    - {DATA}.zip: ZIPs com TXTs do inteiro teor (SeqDocumento.txt)

Filtra por assuntos CNJ de saúde suplementar.

Uso:
    python scripts/importar_stj.py --listar              # Listar períodos disponíveis
    python scripts/importar_stj.py --importar             # Importar último mês de metadados
    python scripts/importar_stj.py --importar --meses 6   # Últimos 6 meses
    python scripts/importar_stj.py --importar --com-integra  # Baixar também inteiro teor (ZIPs grandes)
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


def main():
    parser = argparse.ArgumentParser(description="Importar acórdãos STJ (Dados Abertos)")
    parser.add_argument("--listar", action="store_true", help="Listar períodos disponíveis")
    parser.add_argument("--importar", action="store_true", help="Importar metadados")
    parser.add_argument("--meses", type=int, default=1, help="Últimos N meses (default: 1)")
    parser.add_argument("--com-integra", action="store_true",
                        help="Baixar inteiro teor (ZIPs grandes!)")
    args = parser.parse_args()

    if args.listar:
        listar_periodos()
    elif args.importar:
        importar_stj(meses=args.meses, com_integra=args.com_integra)
    else:
        listar_periodos()


if __name__ == "__main__":
    main()

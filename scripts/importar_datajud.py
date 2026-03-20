"""
scripts/importar_datajud.py — Importa metadados processuais via API pública do DataJud (CNJ).

API: https://api-publica.datajud.cnj.jus.br/api_publica_{tribunal}/_search
Autenticação: Header 'Authorization: APIKey cDZHYzlZa0JadVREZDR4N3VQaVY6MjI0ZDQ4NjctMjNiZS00Y2VlLTk3ZjUtY2RkYjRjYzRhNDk3'

IMPORTANTE: A API DataJud fornece METADADOS (classes, assuntos, movimentações, partes),
NÃO o texto completo de acórdãos. Use para enriquecer dados e encontrar processos relevantes.

Uso:
    python scripts/importar_datajud.py --tribunal TJSP --limite 500
    python scripts/importar_datajud.py --tribunal STJ --assunto 6233
    python scripts/importar_datajud.py --todos-tribunais --limite 100
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from banco import inicializar

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# API DataJud
API_BASE = "https://api-publica.datajud.cnj.jus.br"
API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="

# Tribunais relevantes para saúde suplementar
TRIBUNAIS = [
    "TJSP", "TJRJ", "TJMG", "TJRS", "TJPR", "TJSC", "TJBA",
    "TJPE", "TJCE", "TJGO", "TJDF", "TJES", "TJPA", "TJMA",
    "TJMT", "TJMS", "TJAL", "TJSE", "TJRN", "TJPB", "TJPI",
    "TJAM", "TJRO", "TJTO", "TJAC", "TJAP", "TJRR",
    "STJ",
]

# Códigos de assunto CNJ para saúde suplementar
ASSUNTOS_SAUDE = [
    6233,   # Planos de Saúde
    12222,  # Reajuste de Mensalidade
    12225,  # Tratamento Médico-Hospitalar e/ou Fornecimento de Medicamentos
    12223,  # Cobertura
    12224,  # Carência
    12482,  # Saúde Suplementar
    12486,  # Plano de Saúde - Cobertura
    12490,  # Plano de Saúde - Reajuste
    12487,  # Plano de Saúde - Exclusão de Cobertura
    12488,  # Plano de Saúde - Rescisão do Contrato
    12489,  # Plano de Saúde - Carência
]


def datajud_search(tribunal, query_body, size=100):
    """Executa busca na API DataJud."""
    endpoint = f"{API_BASE}/api_publica_{tribunal.lower()}/_search"

    body = json.dumps(query_body).encode("utf-8")

    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "Authorization": f"APIKey {API_KEY}",
            "Content-Type": "application/json",
            "User-Agent": "ModuloJurisprudencia/1.0",
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", errors="replace")[:500]
        log.error(f"HTTP {e.code} ({tribunal}): {body_err}")
        return None
    except Exception as e:
        log.error(f"Erro API DataJud ({tribunal}): {e}")
        return None


def importar_tribunal(tribunal, assuntos=None, limite=500, conn=None):
    """Importa processos de saúde suplementar de um tribunal via DataJud."""
    log.info(f"\n--- Importando {tribunal} ---")

    assuntos_filtro = assuntos or ASSUNTOS_SAUDE
    close_conn = False
    if conn is None:
        conn = inicializar()
        close_conn = True

    importados = 0
    duplicados = 0
    pagina = 0
    PAGE_SIZE = 100

    try:
        while importados + duplicados < limite:
            # Query Elasticsearch
            query = {
                "size": min(PAGE_SIZE, limite - importados - duplicados),
                "from": pagina * PAGE_SIZE,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "assuntos.codigo": assuntos_filtro
                                }
                            }
                        ]
                    }
                },
                "sort": [{"dataAjuizamento": {"order": "desc"}}],
                "_source": [
                    "numeroProcesso", "classe", "assuntos", "orgaoJulgador",
                    "dataAjuizamento", "movimentos", "grau", "tribunal",
                    "formato.nome"
                ]
            }

            result = datajud_search(tribunal, query)

            if not result:
                log.warning(f"  Sem resposta para {tribunal}")
                break

            hits = result.get("hits", {}).get("hits", [])
            total = result.get("hits", {}).get("total", {})
            total_val = total.get("value", 0) if isinstance(total, dict) else total

            if pagina == 0:
                log.info(f"  Total disponível: {total_val} processos")

            if not hits:
                break

            for hit in hits:
                src = hit.get("_source", {})
                numero = src.get("numeroProcesso", "")
                classe = src.get("classe", {})
                classe_nome = classe.get("nome", "") if isinstance(classe, dict) else str(classe)
                orgao = src.get("orgaoJulgador", {})
                orgao_nome = orgao.get("nome", "") if isinstance(orgao, dict) else str(orgao)
                data_aj = src.get("dataAjuizamento", "")
                assuntos_proc = src.get("assuntos", [])
                grau = src.get("grau", "")

                if not numero:
                    continue

                # Normalizar data
                if data_aj and "T" in str(data_aj):
                    data_aj = str(data_aj).split("T")[0]

                # Extrair assuntos como JSON
                assuntos_json = json.dumps([
                    {"codigo": a.get("codigo", ""), "nome": a.get("nome", "")}
                    for a in assuntos_proc
                ] if isinstance(assuntos_proc, list) else [], ensure_ascii=False)

                # Extrair última movimentação relevante (decisão/julgamento)
                ementa_mov = ""
                data_julg = ""
                movimentos = src.get("movimentos", [])
                if isinstance(movimentos, list):
                    for mov in movimentos:
                        nome_mov = (mov.get("nome", "") or "").upper()
                        if any(k in nome_mov for k in ["JULGAMENTO", "ACORDAO", "ACÓRDÃO", "DECISÃO", "DECISAO"]):
                            data_julg = str(mov.get("dataHora", "")).split("T")[0]
                            # Complementos podem ter texto
                            comps = mov.get("complementosTabelados", [])
                            if isinstance(comps, list):
                                for c in comps:
                                    desc = c.get("descricao", "")
                                    if desc:
                                        ementa_mov += desc + ". "
                            break

                # Classificar tema pelos assuntos
                tema = _classificar_por_assuntos(assuntos_proc)

                # Hash para dedup
                hash_em = hashlib.md5(
                    f"{numero}_{tribunal}_{classe_nome}".encode("utf-8")
                ).hexdigest()

                try:
                    conn.execute("""
                        INSERT OR IGNORE INTO acordaos
                        (numero_processo, tribunal, orgao_julgador, relator,
                         data_julgamento, classe_processual, ementa, tema,
                         fonte, hash_ementa, assuntos_cnj)
                        VALUES (?, ?, ?, '', ?, ?, ?, ?, 'datajud', ?, ?)
                    """, (numero, tribunal, orgao_nome, data_julg or data_aj,
                          classe_nome, ementa_mov or None, tema,
                          hash_em, assuntos_json))

                    if conn.execute("SELECT changes()").fetchone()[0] > 0:
                        importados += 1
                    else:
                        duplicados += 1

                except Exception as e:
                    log.warning(f"  Erro inserindo {numero}: {e}")
                    duplicados += 1

            conn.commit()
            pagina += 1

            log.info(f"  Página {pagina}: {importados} importados, {duplicados} duplicados")

            # Rate limit
            time.sleep(1)

            if len(hits) < PAGE_SIZE:
                break

    finally:
        if close_conn:
            conn.close()

    log.info(f"  {tribunal}: {importados} importados, {duplicados} duplicados")
    return importados, duplicados


def _classificar_por_assuntos(assuntos):
    """Classifica tema baseado nos assuntos CNJ."""
    if not isinstance(assuntos, list):
        return None

    codigos = set()
    nomes = []
    for a in assuntos:
        if isinstance(a, dict):
            cod = a.get("codigo")
            nome = a.get("nome", "")
            if cod:
                codigos.add(int(cod))
            nomes.append(nome.upper())

    # Mapeamento código → tema
    if 12222 in codigos or 12490 in codigos:
        return "Reajuste/mensalidade"
    if 12224 in codigos or 12489 in codigos:
        return "Cobertura contratual"
    if 12488 in codigos:
        return "Rescisao/cancelamento"
    if 12487 in codigos:
        return "Cobertura de procedimento"
    if 12486 in codigos or 12223 in codigos:
        return "Cobertura de procedimento"
    if 12225 in codigos:
        return "Cobertura de medicamento"

    # Por nome
    nomes_str = " ".join(nomes)
    if "AUTIS" in nomes_str or "TEA" in nomes_str:
        return "TEA/Autismo"
    if "MEDICAMENTO" in nomes_str:
        return "Cobertura de medicamento"
    if "REAJUSTE" in nomes_str:
        return "Reajuste/mensalidade"

    return None


def importar_todos(limite_por_tribunal=100):
    """Importa de todos os tribunais relevantes."""
    conn = inicializar()

    # Registrar importação
    conn.execute("""
        INSERT INTO log_importacao (fonte, data_inicio, status)
        VALUES ('datajud', datetime('now'), 'em_andamento')
    """)
    conn.commit()
    log_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    total_imp = 0
    total_dup = 0

    try:
        for tribunal in TRIBUNAIS:
            try:
                imp, dup = importar_tribunal(tribunal, limite=limite_por_tribunal, conn=conn)
                total_imp += imp
                total_dup += dup
            except Exception as e:
                log.error(f"Erro no tribunal {tribunal}: {e}")
            time.sleep(2)  # Pausa entre tribunais

        conn.execute("""
            UPDATE log_importacao
            SET data_fim=datetime('now'), registros_importados=?,
                registros_duplicados=?, status='concluido'
            WHERE id=?
        """, (total_imp, total_dup, log_id))
        conn.commit()

    except Exception as e:
        conn.execute("""
            UPDATE log_importacao SET status='erro', erro=? WHERE id=?
        """, (str(e), log_id))
        conn.commit()
    finally:
        conn.close()

    log.info(f"\n=== DataJud — Todos os tribunais ===")
    log.info(f"  Total importados: {total_imp}")
    log.info(f"  Total duplicados: {total_dup}")


def main():
    parser = argparse.ArgumentParser(description="Importar processos via API DataJud")
    parser.add_argument("--tribunal", help="Tribunal específico (ex: TJSP, STJ)")
    parser.add_argument("--todos-tribunais", action="store_true", help="Importar todos")
    parser.add_argument("--assunto", type=int, help="Código de assunto CNJ específico")
    parser.add_argument("--limite", type=int, default=500, help="Limite de registros (default: 500)")
    args = parser.parse_args()

    if args.todos_tribunais:
        importar_todos(limite_por_tribunal=args.limite)
    elif args.tribunal:
        assuntos = [args.assunto] if args.assunto else None
        importar_tribunal(args.tribunal.upper(), assuntos=assuntos, limite=args.limite)
    else:
        # Padrão: TJSP + STJ
        log.info("Importando TJSP e STJ (padrão)...")
        conn = inicializar()
        try:
            importar_tribunal("TJSP", limite=args.limite, conn=conn)
            importar_tribunal("STJ", limite=args.limite, conn=conn)
        finally:
            conn.close()


if __name__ == "__main__":
    main()

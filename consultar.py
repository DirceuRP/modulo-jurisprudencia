"""
modulo-jurisprudencia/consultar.py — API de consulta à base de jurisprudência.

Uso:
    from consultar import JurisprudenciaConsulta

    juris = JurisprudenciaConsulta()
    resultados = juris.buscar("plano de saude autismo")
    citacao = juris.gerar_citacao(id_acordao)
"""

import json
import logging
import os
import sqlite3

log = logging.getLogger(__name__)

DB_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "ModuloJurisprudencia", "dados")
DB_PATH = os.path.join(DB_DIR, "jurisprudencia.db")

# Temas reconhecidos (mesmos 14 do projeto principal)
TEMAS = [
    "TEA/Autismo", "Home care", "Oncologia", "Cobertura de medicamento",
    "Cobertura de procedimento", "Cobertura contratual", "Reajuste/mensalidade",
    "Rescisao/cancelamento", "Transplante", "Saude mental", "Cirurgia",
    "Urgencia/emergencia", "Reembolso", "Ortopedia/protese",
]


class JurisprudenciaConsulta:
    """Consultas à base de jurisprudência pública."""

    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ==========================================
    # BUSCA POR TEXTO (FTS5)
    # ==========================================

    def buscar(self, termo, tribunal=None, limite=50):
        """
        Busca full-text em ementas e inteiro teor.

        Args:
            termo: palavras-chave (ex: "plano saude autismo cobertura")
            tribunal: filtro opcional (TJSP, STJ, etc.)
            limite: máximo de resultados

        Returns:
            lista de acórdãos encontrados, ordenados por relevância
        """
        conn = self._conn()
        try:
            # FTS5 query
            query = """
                SELECT a.id, a.numero_processo, a.tribunal, a.orgao_julgador,
                       a.relator, a.data_julgamento, a.classe_processual,
                       a.ementa, a.tema, a.resultado_operadora,
                       rank
                FROM acordaos_fts
                JOIN acordaos a ON a.id = acordaos_fts.rowid
                WHERE acordaos_fts MATCH ?
            """
            params = [termo]

            if tribunal:
                query += " AND a.tribunal = ?"
                params.append(tribunal.upper())

            query += " ORDER BY rank LIMIT ?"
            params.append(limite)

            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            log.error(f"Erro na busca FTS: {e}")
            # Fallback para LIKE
            return self._buscar_like(conn, termo, tribunal, limite)
        finally:
            conn.close()

    def _buscar_like(self, conn, termo, tribunal, limite):
        """Busca por LIKE quando FTS falha."""
        where = "UPPER(ementa) LIKE ?"
        params = [f"%{termo.upper()}%"]

        if tribunal:
            where += " AND tribunal = ?"
            params.append(tribunal.upper())

        rows = conn.execute(f"""
            SELECT id, numero_processo, tribunal, orgao_julgador,
                   relator, data_julgamento, classe_processual,
                   ementa, tema, resultado_operadora
            FROM acordaos WHERE {where}
            ORDER BY data_julgamento DESC
            LIMIT ?
        """, params + [limite]).fetchall()

        return [dict(r) for r in rows]

    # ==========================================
    # BUSCA POR TEMA
    # ==========================================

    def buscar_por_tema(self, tema, tribunal=None, resultado=None, limite=50):
        """
        Busca acórdãos por tema classificado.

        Args:
            tema: um dos 14 temas (ex: "TEA/Autismo")
            tribunal: filtro opcional
            resultado: filtro por resultado_operadora (favoravel/desfavoravel/parcial)
            limite: máximo de resultados

        Returns:
            lista de acórdãos
        """
        conn = self._conn()
        try:
            where = "tema = ?"
            params = [tema]

            if tribunal:
                where += " AND tribunal = ?"
                params.append(tribunal.upper())

            if resultado:
                where += " AND resultado_operadora = ?"
                params.append(resultado)

            rows = conn.execute(f"""
                SELECT id, numero_processo, tribunal, orgao_julgador,
                       relator, data_julgamento, classe_processual,
                       ementa, tema, resultado_operadora
                FROM acordaos WHERE {where}
                ORDER BY data_julgamento DESC
                LIMIT ?
            """, params + [limite]).fetchall()

            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ==========================================
    # BUSCA POR RELATOR / ÓRGÃO
    # ==========================================

    def buscar_por_relator(self, relator, tema=None, limite=50):
        """Busca acórdãos de um relator específico."""
        conn = self._conn()
        try:
            where = "UPPER(relator) LIKE ?"
            params = [f"%{relator.upper()}%"]

            if tema:
                where += " AND tema = ?"
                params.append(tema)

            rows = conn.execute(f"""
                SELECT id, numero_processo, tribunal, orgao_julgador,
                       relator, data_julgamento, classe_processual,
                       ementa, tema, resultado_operadora
                FROM acordaos WHERE {where}
                ORDER BY data_julgamento DESC
                LIMIT ?
            """, params + [limite]).fetchall()

            return [dict(r) for r in rows]
        finally:
            conn.close()

    def tendencia_orgao(self, orgao_julgador, tema=None):
        """
        Calcula a tendência de decisão de um órgão julgador.

        Returns:
            dict com total, favoravel, desfavoravel, parcial, taxa_favoravel
        """
        conn = self._conn()
        try:
            where = "UPPER(orgao_julgador) LIKE ?"
            params = [f"%{orgao_julgador.upper()}%"]

            if tema:
                where += " AND tema = ?"
                params.append(tema)

            rows = conn.execute(f"""
                SELECT resultado_operadora, COUNT(*) as qtd
                FROM acordaos WHERE {where}
                GROUP BY resultado_operadora
            """, params).fetchall()

            resultado = {"total": 0, "favoravel": 0, "desfavoravel": 0, "parcial": 0}
            for r in rows:
                res = r["resultado_operadora"] or "indefinido"
                qtd = r["qtd"]
                resultado["total"] += qtd
                if res in resultado:
                    resultado[res] = qtd

            if resultado["total"] > 0:
                resultado["taxa_favoravel"] = round(
                    resultado["favoravel"] / resultado["total"] * 100, 1
                )
            else:
                resultado["taxa_favoravel"] = 0.0

            return resultado
        finally:
            conn.close()

    def tendencia_relator(self, relator, tema=None):
        """
        Calcula a tendência de um relator específico.

        Returns:
            dict com total, favoravel, desfavoravel, parcial, taxa_favoravel
        """
        return self.tendencia_orgao(relator, tema)  # Mesma lógica, campo diferente

    # ==========================================
    # GERAÇÃO DE CITAÇÃO
    # ==========================================

    def gerar_citacao(self, acordao_id_or_dict, formato="petição"):
        """
        Gera citação formatada de um acórdão para uso em petições.

        Args:
            acordao_id_or_dict: ID do acórdão na base OU dict retornado por buscar()
            formato: "petição" (padrão ABNT jurídico) ou "texto"

        Returns:
            string formatada pronta para colar na petição
        """
        # Aceitar tanto ID quanto dict
        if isinstance(acordao_id_or_dict, dict):
            row = acordao_id_or_dict
        else:
            conn = self._conn()
            try:
                row = conn.execute("""
                    SELECT numero_processo, tribunal, orgao_julgador, relator,
                           data_julgamento, classe_processual, ementa
                    FROM acordaos WHERE id = ?
                """, (acordao_id_or_dict,)).fetchone()
            finally:
                conn.close()

            if not row:
                return None
            row = dict(row)

        if not row:
            return None

        proc = row.get("numero_processo", "")
        trib = row.get("tribunal", "")
        orgao = row.get("orgao_julgador") or ""
        relator = row.get("relator") or ""
        data = row.get("data_julgamento") or ""
        classe = row.get("classe_processual") or ""
        ementa = row.get("ementa") or ""

        # Formatar data
        if data and len(data) >= 10:
            partes = data.split("-")
            if len(partes) == 3:
                data_fmt = f"{partes[2]}/{partes[1]}/{partes[0]}"
            else:
                data_fmt = data
        else:
            data_fmt = data

        if formato == "petição":
            # Formato ABNT jurídico
            citacao = f'{trib}. {classe} nº {proc}. '
            if relator:
                citacao += f'Rel. {relator}. '
            if orgao:
                citacao += f'{orgao}. '
            if data_fmt:
                citacao += f'Julgado em {data_fmt}.'

            if ementa:
                # Primeira frase da ementa como trecho
                primeira = ementa.split(".")[0] + "."
                if len(primeira) < 500:
                    citacao += f'\n\nEmenta: "{primeira}"'

            return citacao
        else:
            # Formato texto simples
            return f"{classe} {proc} ({trib}, {orgao}, Rel. {relator}, j. {data_fmt})"

    def gerar_citacoes_tema(self, tema, tribunal=None, resultado="favoravel", limite=5):
        """
        Gera múltiplas citações sobre um tema para usar em petição.

        Args:
            tema: tema jurídico
            tribunal: filtro opcional
            resultado: tipo de resultado desejado
            limite: quantas citações

        Returns:
            lista de citações formatadas
        """
        acordaos = self.buscar_por_tema(tema, tribunal, resultado, limite)
        citacoes = []
        for a in acordaos:
            cit = self.gerar_citacao(a["id"])
            if cit:
                citacoes.append(cit)
        return citacoes

    # ==========================================
    # ESTATÍSTICAS
    # ==========================================

    def stats(self):
        """Retorna estatísticas gerais da base."""
        conn = self._conn()
        try:
            stats = {}
            stats["total_acordaos"] = conn.execute(
                "SELECT COUNT(*) FROM acordaos").fetchone()[0]
            stats["com_inteiro_teor"] = conn.execute(
                "SELECT COUNT(*) FROM acordaos WHERE inteiro_teor IS NOT NULL AND inteiro_teor != ''").fetchone()[0]

            # Por tribunal
            rows = conn.execute("""
                SELECT tribunal, COUNT(*) as qtd
                FROM acordaos GROUP BY tribunal ORDER BY qtd DESC
            """).fetchall()
            stats["por_tribunal"] = {r["tribunal"]: r["qtd"] for r in rows}

            # Por tema
            rows = conn.execute("""
                SELECT tema, COUNT(*) as qtd
                FROM acordaos WHERE tema IS NOT NULL
                GROUP BY tema ORDER BY qtd DESC
            """).fetchall()
            stats["por_tema"] = {r["tema"]: r["qtd"] for r in rows}

            # Por resultado
            rows = conn.execute("""
                SELECT resultado_operadora, COUNT(*) as qtd
                FROM acordaos WHERE resultado_operadora IS NOT NULL
                GROUP BY resultado_operadora ORDER BY qtd DESC
            """).fetchall()
            stats["por_resultado"] = {r["resultado_operadora"]: r["qtd"] for r in rows}

            # Por fonte
            rows = conn.execute("""
                SELECT fonte, COUNT(*) as qtd
                FROM acordaos GROUP BY fonte ORDER BY qtd DESC
            """).fetchall()
            stats["por_fonte"] = {r["fonte"]: r["qtd"] for r in rows}

            return stats
        finally:
            conn.close()


# Teste rápido
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    juris = JurisprudenciaConsulta()

    print("=== Estatísticas ===")
    s = juris.stats()
    for k, v in s.items():
        print(f"  {k}: {v}")

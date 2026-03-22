"""
modulo-jurisprudencia/banco.py — Schema SQLite para base de jurisprudência.

Banco: %LOCALAPPDATA%/ModuloJurisprudencia/dados/jurisprudencia.db
"""

import os
import sqlite3
import logging

log = logging.getLogger(__name__)

DB_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "ModuloJurisprudencia", "dados")
DB_PATH = os.path.join(DB_DIR, "jurisprudencia.db")


def inicializar(db_path=None):
    """Cria o banco e as tabelas se não existirem."""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.executescript("""
        -- Acórdãos e decisões de tribunais
        CREATE TABLE IF NOT EXISTS acordaos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_processo TEXT NOT NULL,
            tribunal TEXT NOT NULL,          -- TJSP, STJ, TJRJ, etc.
            orgao_julgador TEXT,             -- 7ª Câmara de Direito Privado, 3ª Turma, etc.
            relator TEXT,                    -- Nome do relator
            data_julgamento TEXT,            -- YYYY-MM-DD
            data_publicacao TEXT,            -- YYYY-MM-DD
            classe_processual TEXT,          -- Apelação Cível, Agravo de Instrumento, REsp, etc.
            ementa TEXT,                     -- Texto da ementa
            inteiro_teor TEXT,               -- Texto completo (quando disponível)
            tema TEXT,                       -- Tema principal (das 14 categorias)
            subtema TEXT,                    -- Subtema específico
            resultado_operadora TEXT,        -- favoravel, desfavoravel, parcial (perspectiva operadora)
            assuntos_cnj TEXT,              -- JSON array de códigos/nomes CNJ
            legislacao_citada TEXT,          -- JSON array de normas citadas
            fonte TEXT NOT NULL,             -- stj_dados_abertos, datajud, tjsp_esaj, manual
            fonte_url TEXT,                  -- URL de origem
            hash_ementa TEXT,               -- Para deduplicação
            importado_em TEXT DEFAULT (datetime('now')),

            UNIQUE(numero_processo, tribunal, classe_processual, data_julgamento)
        );

        -- Teses jurídicas extraídas dos acórdãos
        CREATE TABLE IF NOT EXISTS teses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            acordao_id INTEGER NOT NULL,
            tese TEXT NOT NULL,              -- Texto da tese
            tipo TEXT,                       -- favoravel_operadora, desfavoravel_operadora, neutra
            categoria TEXT,                  -- Das 14 categorias de defesa
            acolhida INTEGER,               -- 1=sim, 0=não

            FOREIGN KEY (acordao_id) REFERENCES acordaos(id) ON DELETE CASCADE
        );

        -- Índice de assuntos CNJ para busca rápida
        CREATE TABLE IF NOT EXISTS assuntos_cnj (
            codigo INTEGER PRIMARY KEY,
            nome TEXT NOT NULL,
            pai_codigo INTEGER,
            area TEXT                        -- saude_suplementar, consumidor, etc.
        );

        -- Controle de importações
        CREATE TABLE IF NOT EXISTS log_importacao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fonte TEXT NOT NULL,
            tribunal TEXT,
            data_inicio TEXT,
            data_fim TEXT,
            registros_importados INTEGER DEFAULT 0,
            registros_duplicados INTEGER DEFAULT 0,
            status TEXT DEFAULT 'em_andamento', -- em_andamento, concluido, erro
            erro TEXT,
            criado_em TEXT DEFAULT (datetime('now'))
        );

        -- Índices
        CREATE INDEX IF NOT EXISTS idx_acordaos_tribunal ON acordaos(tribunal);
        CREATE INDEX IF NOT EXISTS idx_acordaos_tema ON acordaos(tema);
        CREATE INDEX IF NOT EXISTS idx_acordaos_resultado ON acordaos(resultado_operadora);
        CREATE INDEX IF NOT EXISTS idx_acordaos_orgao ON acordaos(orgao_julgador);
        CREATE INDEX IF NOT EXISTS idx_acordaos_relator ON acordaos(relator);
        CREATE INDEX IF NOT EXISTS idx_acordaos_data ON acordaos(data_julgamento);
        CREATE INDEX IF NOT EXISTS idx_acordaos_processo ON acordaos(numero_processo);
        CREATE INDEX IF NOT EXISTS idx_acordaos_hash ON acordaos(hash_ementa);
        CREATE INDEX IF NOT EXISTS idx_acordaos_fonte ON acordaos(fonte);
        CREATE INDEX IF NOT EXISTS idx_teses_acordao ON teses(acordao_id);
        CREATE INDEX IF NOT EXISTS idx_teses_categoria ON teses(categoria);

        -- Pareceres e Notas Técnicas do e-NatJus (CNJ)
        CREATE TABLE IF NOT EXISTS natjus_pareceres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            natjus_id INTEGER,              -- ID no sistema e-NatJus
            tipo TEXT NOT NULL,             -- 'parecer' ou 'nota_tecnica'
            data_publicacao TEXT,
            titulo TEXT,                    -- Intervenção/tecnologia estudada
            cid TEXT,                       -- Código CID (F84, C50, etc.)
            cid_descricao TEXT,             -- Descrição do CID
            tipo_tecnologia TEXT,           -- Medicamento, Procedimento, Produto
            natjus_origem TEXT,             -- UF ou Nacional
            status TEXT,                    -- Finalizado, Em andamento
            conclusao TEXT,                 -- Favorável, Desfavorável, etc.
            arquivo_hash TEXT,              -- Hash do PDF para download
            arquivo_local TEXT,             -- Caminho local do PDF baixado
            tema TEXT,                      -- Classificado nos 14 temas
            importado_em TEXT DEFAULT (datetime('now')),
            UNIQUE(natjus_id, tipo)
        );

        CREATE INDEX IF NOT EXISTS idx_natjus_cid ON natjus_pareceres(cid);
        CREATE INDEX IF NOT EXISTS idx_natjus_tema ON natjus_pareceres(tema);
        CREATE INDEX IF NOT EXISTS idx_natjus_tipo ON natjus_pareceres(tipo);

        -- FTS (Full-Text Search) para buscas em ementas
        CREATE VIRTUAL TABLE IF NOT EXISTS acordaos_fts USING fts5(
            ementa,
            inteiro_teor,
            content='acordaos',
            content_rowid='id'
        );

        -- Triggers para manter FTS sincronizado
        CREATE TRIGGER IF NOT EXISTS acordaos_ai AFTER INSERT ON acordaos BEGIN
            INSERT INTO acordaos_fts(rowid, ementa, inteiro_teor)
            VALUES (new.id, new.ementa, new.inteiro_teor);
        END;

        CREATE TRIGGER IF NOT EXISTS acordaos_ad AFTER DELETE ON acordaos BEGIN
            INSERT INTO acordaos_fts(acordaos_fts, rowid, ementa, inteiro_teor)
            VALUES ('delete', old.id, old.ementa, old.inteiro_teor);
        END;

        CREATE TRIGGER IF NOT EXISTS acordaos_au AFTER UPDATE ON acordaos BEGIN
            INSERT INTO acordaos_fts(acordaos_fts, rowid, ementa, inteiro_teor)
            VALUES ('delete', old.id, old.ementa, old.inteiro_teor);
            INSERT INTO acordaos_fts(rowid, ementa, inteiro_teor)
            VALUES (new.id, new.ementa, new.inteiro_teor);
        END;
    """)

    conn.commit()
    log.info(f"Banco jurisprudência inicializado: {path}")
    return conn


def get_db(db_path=None):
    """Retorna conexão ao banco existente."""
    path = db_path or DB_PATH
    if not os.path.exists(path):
        return inicializar(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

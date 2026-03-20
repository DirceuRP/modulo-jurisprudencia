# modulo-jurisprudencia

Base de acordaos publicos para consulta e citacao em peticoes de saude suplementar.

## Fontes de dados

- **STJ Dados Abertos**: integras de acordaos e decisoes (portal CKAN)
- **DataJud API (CNJ)**: metadados processuais (classes, assuntos, movimentacoes)
- **TJSP e-SAJ**: ementas de jurisprudencia publica (scraping respeitoso)

## Instalacao

```bash
# Sem dependencias externas — usa apenas stdlib Python
python scripts/importar_datajud.py --tribunal TJSP --limite 100
```

## Uso rapido

```python
from consultar import JurisprudenciaConsulta

juris = JurisprudenciaConsulta()

# Busca full-text em ementas
juris.buscar("plano de saude autismo cobertura")

# Busca por tema e tribunal
juris.buscar_por_tema("TEA/Autismo", tribunal="TJSP")

# Tendencia de um orgao julgador
juris.tendencia_orgao("7a Camara de Direito Privado", tema="TEA/Autismo")

# Gerar citacao para peticao
juris.gerar_citacao(id_acordao=42)

# Multiplas citacoes sobre um tema
juris.gerar_citacoes_tema("TEA/Autismo", tribunal="TJSP", resultado="favoravel")
```

## Importacao de dados

```bash
# 1. DataJud API (metadados — mais rapido)
python scripts/importar_datajud.py --tribunal TJSP --limite 500
python scripts/importar_datajud.py --todos-tribunais --limite 100

# 2. TJSP e-SAJ (ementas de acordaos)
python scripts/importar_tjsp.py --todos-temas --limite 200
python scripts/importar_tjsp.py --assunto "plano de saude autismo"

# 3. STJ Dados Abertos (integras de acordaos)
python scripts/importar_stj.py --explorar
python scripts/importar_stj.py --importar --limite 100
```

## Temas cobertos

1. TEA/Autismo
2. Oncologia
3. Home care
4. Cobertura de medicamento
5. Cobertura de procedimento
6. Cobertura contratual
7. Reajuste/mensalidade
8. Rescisao/cancelamento
9. Transplante
10. Saude mental
11. Urgencia/emergencia
12. Reembolso
13. Ortopedia/protese
14. Cirurgia

## Estrutura

```
modulo-jurisprudencia/
├── __init__.py                    # Documentacao do modulo
├── banco.py                       # Schema SQLite (acordaos, teses, FTS5)
├── consultar.py                   # API de consulta (JurisprudenciaConsulta)
├── scripts/
│   ├── importar_datajud.py        # DataJud API (metadados CNJ)
│   ├── importar_tjsp.py           # TJSP e-SAJ (ementas publicas)
│   └── importar_stj.py            # STJ Dados Abertos (integras)
└── dados/                         # Arquivos temporarios (nao versionados)
```

## Banco de dados

- Local: `%LOCALAPPDATA%/ModuloJurisprudencia/dados/jurisprudencia.db`
- Tabela `acordaos`: numero_processo, tribunal, orgao, relator, ementa, inteiro_teor, tema
- Tabela `teses`: teses extraidas dos acordaos (favoravel/desfavoravel operadora)
- FTS5: busca full-text em ementas e inteiro teor
- Log de importacoes para controle

## Fontes

- DataJud: https://datajud-wiki.cnj.jus.br/
- TJSP: https://esaj.tjsp.jus.br/cjsg/consultaCompleta.do
- STJ: https://dadosabertos.web.stj.jus.br/

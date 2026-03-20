"""
modulo-jurisprudencia — Base de acórdãos públicos para consulta e citação em petições.

Fontes:
    - STJ Portal de Dados Abertos (íntegras de acórdãos e decisões)
    - DataJud API (metadados processuais: classes, assuntos, movimentações)
    - TJSP e-SAJ (ementas de jurisprudência pública)

Uso:
    from consultar import JurisprudenciaConsulta

    juris = JurisprudenciaConsulta()
    juris.buscar("plano de saude autismo cobertura")
    juris.buscar_por_tema("TEA/Autismo", tribunal="TJSP")
    juris.gerar_citacao(id_acordao)
"""

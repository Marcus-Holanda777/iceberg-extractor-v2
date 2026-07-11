"""
Modulo de excecoes para o pacote iceberg_extractor_v2.

Este arquivo define as classes de erro personalizadas utilizadas pelo
leitor Iceberg e pela integracao com Delta Lake. O objetivo e centralizar
os tipos de falha esperados para facilitar o tratamento de erros em todo
o codigo e permitir uma captura consistente de excecoes especificas.

Classes:
    IcebergReaderError: excecao base para erros do leitor Iceberg.
    LineageBrokenError: lancada quando a linhagem de snapshots esta corrompida ou quebrada.
    CheckpointExpiredError: lancada quando o ID do checkpoint nao existe mais nos metadados historicos.
    UnsupportedFeatureError: lancada quando o formato encontra recursos nao suportados.
    DeltaStorageError: erro generico de persistencia Delta Lake.
    DeltaTableResolutionError: erro ao resolver ou acessar a tabela Delta de destino.
    DeltaStorageWriteError: erro ao gravar um DataFrame no Delta Lake.
    DeltaMutationError: erro ao aplicar mutacoes incrementais no Delta Lake.

Este modulo deve ser importado por outras partes do pacote para lancar e
capturar excecoes especificas, mantendo o codigo mais legivel e separado
entre falhas de leitura Iceberg e falhas de persistencia Delta.
"""


class IcebergReaderError(Exception):
    """Excecao base para o pacote do leitor Iceberg."""

    pass


class LineageBrokenError(IcebergReaderError):
    """Lancada quando a linhagem de snapshots esta corrompida ou quebrada."""

    pass


class CheckpointExpiredError(IcebergReaderError):
    """Lancada quando o ID do checkpoint nao existe mais nos metadados historicos."""

    pass


class UnsupportedFeatureError(IcebergReaderError):
    """Lancada quando o formato encontra recursos nao suportados (ex: Equality Deletes)."""

    pass


class DeltaStorageError(IcebergReaderError):
    """Erro generico de persistencia Delta Lake."""

    pass


class DeltaTableResolutionError(DeltaStorageError):
    """Erro ao resolver ou acessar a tabela Delta de destino."""

    pass


class DeltaStorageWriteError(DeltaStorageError):
    """Erro ao gravar um DataFrame no Delta Lake."""

    pass


class DeltaMutationError(DeltaStorageError):
    """Erro ao aplicar mutacoes incrementais no Delta Lake."""

    pass

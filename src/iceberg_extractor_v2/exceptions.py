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

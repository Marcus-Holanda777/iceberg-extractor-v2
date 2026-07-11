"""
Modulo responsavel por abstrair a leitura e descoberta de metadados de tabelas Apache Iceberg.

Este modulo fornece classes e funcoes para localizar o ultimo arquivo de metadata
Iceberg no diretorio de metadados de uma tabela e para representar esses arquivos
com informacoes de caminho e timestamp de modificacao.

Ele e util para cenarios de leitura incremental, pois identifica o snapshot
atual e os manifestos associados a partir da estrutura interna de metadados
do Iceberg. O processo de descoberta e capaz de trabalhar em ambientes com
adbutils do Databricks e, quando necessario, recorre ao Hadoop FileSystem do Spark.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Set

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from .exceptions import CheckpointExpiredError, LineageBrokenError

logger = logging.getLogger("iceberg_incremental_reader.catalog")


@dataclass
class IcebergMetadataFile:
    """Representa um arquivo de metadados do Iceberg com seu caminho e timestamp de modificacao."""

    path: str
    modification_time: int


class IcebergCatalog:
    """Camada de abstracao para leitura e navegacao de metadados do Apache Iceberg V2.

    Fornece descoberta do ultimo arquivo de metadata, leitura do JSON de metadados
    em DataFrame Spark, extracao de informacoes de snapshots e schemas, e filtragem
    de arquivos ativos em snapshots especificos.
    """

    def __init__(self, spark: SparkSession, table_directory_path: str) -> None:
        self.spark = spark
        self.table_path = table_directory_path.rstrip("/")
        self.metadata_dir = f"{self.table_path}/metadata"

        logger.info(f"Escaneando diretorio de metadados: {self.metadata_dir}")
        self.metadata_path = self._discover_latest_metadata()
        logger.info(f"Ultimo arquivo de metadados identificado: {self.metadata_path}")

    def _is_path_based(self, path: str) -> bool:
        """Verifica se o caminho fornecido e baseado em S3.

        Aceita prefixos comuns de buckets S3 usados pelo Spark e Databricks.

        Args:
            path (str): O caminho a ser verificado.

        Returns:
            bool: True se o caminho for baseado em S3, False caso contrario.
        """
        return path.startswith(("s3://", "s3a://", "s3n://"))

    def _discover_latest_metadata(self) -> str:
        """Descobre o arquivo de metadados Iceberg mais recente no diretório de metadata.

        A funcao verifica se o caminho de metadata e um caminho S3 valido e, em seguida,
        tenta listar os arquivos usando o dbutils do Databricks. Se o dbutils nao
        estiver disponivel no ambiente, a funcao alterna para o Hadoop FileSystem
        do Spark para listar os arquivos no diretorio.

        O arquivo retornado e o mais recente entre os arquivos que terminam com
        ".metadata.json", ordenado pelo tempo de modificacao. O tempo de modificacao
        e extraido do status do arquivo ou do FileSystem hadoop.

        Returns:
            str: Caminho completo do arquivo de metadados mais recente.

        Raises:
            ValueError: Se o caminho fornecido nao for um bucket S3 valido.
            FileNotFoundError: Se nenhum arquivo de metadados for encontrado.
        """
        if not self._is_path_based(self.metadata_dir):
            raise ValueError(
                f"O caminho fornecido '{self.metadata_dir}' nao aponta para um bucket S3 valido."
            )

        metadata_files: list[IcebergMetadataFile] = []

        try:
            from databricks.sdk import WorkspaceClient

            wsc = WorkspaceClient()
            status_list = wsc.dbutils.fs.ls(self.metadata_dir)

            if not status_list:
                raise FileNotFoundError(
                    f"Nenhum arquivo de metadados encontrado em (s3): {self.metadata_dir}"
                )

            metadata_files = sorted(
                map(
                    lambda _: IcebergMetadataFile(_.path, _.modificationTime),
                    filter(
                        lambda _: _.name.endswith(".metadata.json"),
                        status_list,
                    ),
                ),
                key=lambda _: _.modification_time,
                reverse=True,
            )

        except ImportError:
            logger.info(
                "Ambiente sem dbutils detectado. Tentando acessar o S3 via Hadoop FileSystem."
            )
            logger.info(
                f"Tentando listar arquivos no endereco {self.metadata_dir} via Hadoop FileSystem."
            )

            sc = self.spark.sparkContext
            conf = sc._jsc.hadoopConfiguration()
            Path_class = sc._gateway.jvm.org.apache.hadoop.fs.Path
            path_object = Path_class(self.metadata_dir)
            fs = path_object.getFileSystem(conf)

            status_list = fs.listStatus(path_object)
            if not status_list:
                raise FileNotFoundError(
                    f"Nenhum arquivo de metadados encontrado em (s3): {self.metadata_dir}"
                )

            metadata_files = sorted(
                map(
                    lambda status: IcebergMetadataFile(
                        status.getPath().toString(), status.getModificationTime()
                    ),
                    filter(
                        lambda status: (
                            status.getPath().toString().endswith(".metadata.json")
                        ),
                        status_list,
                    ),
                ),
                key=lambda meta: meta.modification_time,
                reverse=True,
            )
        finally:
            if not metadata_files:
                raise FileNotFoundError(
                    f"Nenhum arquivo com a extensao '.metadata.json' foi localizado em {self.metadata_dir}"
                )

            return metadata_files[0].path

    def load_raw_metadata(self) -> DataFrame:
        """Carrega o JSON de metadados atual.

        Le o arquivo de metadados Iceberg mais recente identificado no
        diretorio de metadata e retorna um DataFrame Spark com o conteúdo JSON.
        O DataFrame resultante contem os campos estruturados do metadado Iceberg,
        como current-snapshot-id, current-schema-id, snapshots, schemas e outros
        objetos de catalogo.

        Returns:
            DataFrame: DataFrame contendo o JSON de metadados lido.
        """
        return self.spark.read.option("multiline", "true").json(self.metadata_path)

    def extract_meta_info(self, df_meta_raw: DataFrame) -> dict:
        """Extrai informacoes essenciais dos metadados do snapshot atual.

        Le o DataFrame de metadados brutos do Iceberg e identifica o snapshot atual
        por meio do campo `current-snapshot-id`. Retorna o ID do snapshot atual,
        o caminho para a lista de manifestos associada e o ID do schema atual. Este
        metodo espera que exista um snapshot cujo snapshot-id coincida com o
        current-snapshot-id e que contenha o campo manifest-list.

        Args:
            df_meta_raw (DataFrame): DataFrame Spark contendo o JSON de metadados
                do Iceberg carregado.

        Returns:
            dict: Dicionario com as chaves:
                - current_snapshot_id: ID do snapshot atual como inteiro.
                - manifest_list_path: Caminho do arquivo manifest list.
                - current_schema_id: ID do schema atual como inteiro.
        """
        meta_info = (
            df_meta_raw.select(
                F.col("current-snapshot-id").alias("current_snapshot_id"),
                F.col("current-schema-id").alias("current_schema_id"),
                F.explode("snapshots").alias("snap"),
            )
            .filter(F.col("current-snapshot-id") == F.col("snap.snapshot-id"))
            .select(
                F.col("current_snapshot_id"),
                F.col("snap.manifest-list").alias("manifest_list_path"),
                F.col("current_schema_id"),
            )
            .first()
        )
        return {
            "current_snapshot_id": int(meta_info["current_snapshot_id"]),
            "manifest_list_path": meta_info["manifest_list_path"],
            "current_schema_id": int(meta_info["current_schema_id"]),
        }

    def get_schema_columns(self, df_meta_raw: DataFrame, schema_id: int) -> List[str]:
        """Mapeia as colunas do schema oficial do Iceberg.

        Extrai os nomes de todas as colunas de um schema especifico da tabela Iceberg,
        a partir do dataframe de metadados bruto.

        Args:
            df_meta_raw: DataFrame contendo os metadados brutos da tabela Iceberg.
            schema_id: ID do schema a ser consultado como inteiro.

        Returns:
            List[str]: Lista com os nomes de todas as colunas do schema especificado.
        """
        col_rows = (
            df_meta_raw.select(F.explode("schemas").alias("sch"))
            .filter(F.col("sch.schema-id") == F.lit(schema_id))
            .select(F.explode("sch.fields").alias("field"))
            .select(F.col("field.name").alias("column_name"))
            .collect()
        )
        return [row["column_name"] for row in col_rows]

    def build_snapshot_interval(
        self, df_meta: DataFrame, checkpoint_id: int, current_id: int
    ) -> List[int]:
        """Valida a linhagem cronologica entre o checkpoint e o snapshot atual.

        Extrai o historico de snapshots a partir dos metadados e constroi o intervalo
        incremental de snapshots entre o checkpoint e o snapshot atual.

        Args:
            df_meta: DataFrame contendo os metadados brutos da tabela Iceberg.
            checkpoint_id: ID do snapshot de checkpoint que serve como origem do intervalo.
            current_id: ID do snapshot atual de destino.

        Returns:
            List[int]: Lista de ids de snapshots em ordem cronológica do checkpoint ao current_id.

        Raises:
            CheckpointExpiredError: Se o checkpoint nao existir no historico de snapshots e nao for igual ao current_id.
            LineageBrokenError: Se houver ciclo na linhagem ou se o checkpoint nao for ancestral linear de current_id.
        """
        df_snapshots = df_meta.select(F.explode("snapshots").alias("snap")).select(
            F.col("snap.snapshot-id").cast("long").alias("snapshot_id"),
            F.col("snap.parent-snapshot-id").cast("long").alias("parent_id"),
        )

        snapshot_tree = {
            row["snapshot_id"]: row["parent_id"] for row in df_snapshots.collect()
        }

        if checkpoint_id not in snapshot_tree and checkpoint_id != current_id:
            raise CheckpointExpiredError(
                f"O snapshot do checkpoint {checkpoint_id} expirou do historico."
            )

        interval_ids: List[int] = []
        cursor: Optional[int] = current_id
        visited = set()

        while cursor and cursor != checkpoint_id:
            if cursor in visited:
                raise LineageBrokenError(
                    "Ciclo infinito detectado na linhagem de snapshots."
                )
            visited.add(cursor)
            interval_ids.append(cursor)
            cursor = snapshot_tree.get(cursor)

        if cursor != checkpoint_id:
            raise LineageBrokenError(
                f"O checkpoint {checkpoint_id} nao e um ancestral linear de {current_id}."
            )

        list_interval_ids = list(reversed(interval_ids))
        logger.info(f"Snapshots do intervalo incremental: {list_interval_ids}")
        return list_interval_ids

    def get_active_files_at_snapshot(
        self, snapshot_id: int, df_meta_raw: DataFrame
    ) -> Set[str]:
        """Retorna os caminhos de arquivos de dados ativos em um snapshot.

        Percorre o metadado bruto do Iceberg para localizar o snapshot
        especificado, carrega seus manifests e extrai os caminhos dos
        arquivos de dados que estão ativos naquele snapshot.

        Args:
            snapshot_id: ID do snapshot a ser consultado.
            df_meta_raw: DataFrame contendo o metadado bruto do catálogo,
                incluindo a lista de snapshots.

        Returns:
            Conjunto de strings com os caminhos dos arquivos de dados ativos.
        """
        snap_info = (
            df_meta_raw.select(F.explode("snapshots").alias("snap"))
            .filter(F.col("snap.snapshot-id") == F.lit(snapshot_id))
            .select(F.col("snap.manifest-list").alias("manifest_list"))
            .first()
        )

        if not snap_info or not snap_info["manifest_list"]:
            return set()

        df_manifests = (
            self.spark.read.format("avro")
            .load(snap_info["manifest_list"])
            .filter(F.col("content") == 0)
        )
        manifest_paths = [
            row["p"]
            for row in df_manifests.select(F.col("manifest_path").alias("p"))
            .distinct()
            .collect()
        ]

        if not manifest_paths:
            return set()

        df_entries = self.spark.read.format("avro").load(manifest_paths)
        active_paths = [
            row["path"]
            for row in df_entries.filter(
                F.col("status").isin(0, 1) & (F.col("data_file.content") == 0)
            )
            .select(F.col("data_file.file_path").alias("path"))
            .distinct()
            .collect()
        ]

        return set(active_paths)

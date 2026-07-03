import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from .exceptions import (
    CheckpointExpiredError,
    LineageBrokenError,
    UnsupportedFeatureError,
)
from delta.tables import DeltaTable


logger = logging.getLogger("iceberg_incremental_reader")


@dataclass
class IcebergMetadataFile:
    """Representa um arquivo de metadados do Iceberg com seu caminho e timestamp de modificacao."""

    path: str
    modification_time: int


class IcebergIncrementalReader:
    """Orquestrador de leitura incremental e full de tabelas Apache Iceberg V2."""

    def __init__(
        self, spark: SparkSession, table_directory_path: str, target_table: str
    ) -> None:
        """Inicializa o leitor incremental desvendando os metadados dinamicamente.

        Args:
            spark: Instância ativa do SparkSession.
            table_directory_path: Caminho base da tabela de origem no S3
                (ex: 's3://bucket-name/tabela/').
            target_table: Nome qualificado da tabela Iceberg destino.
        """
        self.spark = spark
        self.target_table = target_table
        self.table_path = table_directory_path.rstrip("/")
        self.metadata_dir = f"{self.table_path}/metadata"

        logger.info(f"Escaneando diretorio de metadados: {self.metadata_dir}")
        self.metadata_path = self._discover_latest_metadata()
        logger.info(f"Ultimo arquivo de metadados identificado: {self.metadata_path}")

    def _discover_latest_metadata(self) -> str:
        """Identifica o arquivo de metadados mais recente no diretorio S3 especificado.

        Returns:
            str: Caminho completo do arquivo de metadados mais recente.

        Raises:
            ValueError: Se o caminho fornecido nao for um bucket S3 valido.
            FileNotFoundError: Se nenhum arquivo de metadados for encontrado.

        """
        if not self.metadata_dir.startswith(("s3://", "s3a://", "s3n://")):
            raise ValueError(
                f"O caminho fornecido '{self.metadata_dir}' nao aponta para um bucket S3 valido."
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

        if not metadata_files:
            raise FileNotFoundError(
                f"Nenhum arquivo com a extensao '.metadata.json' foi localizado em {self.metadata_dir}"
            )

        return metadata_files[0].path

    def _build_snapshot_interval(
        self, checkpoint_id: int, current_id: int
    ) -> List[int]:
        """Rastreia a arvore de snapshots do Iceberg do mais recente ao checkpoint."""
        df_meta = self.spark.read.option("multiline", "true").json(self.metadata_path)

        df_snapshots = df_meta.select(F.explode("snapshots").alias("snap")).select(
            F.col("snap.snapshot-id").cast("long").alias("snapshot_id"),
            F.col("snap.parent-snapshot-id").cast("long").alias("parent_id"),
        )

        snapshot_tree = {
            row["snapshot_id"]: row["parent_id"] for row in df_snapshots.collect()
        }

        if checkpoint_id not in snapshot_tree and checkpoint_id != current_id:
            raise CheckpointExpiredError(
                f"O snapshot do checkpoint {checkpoint_id} expirou do histórico."
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
                f"O checkpoint {checkpoint_id} nao é um ancestral linear de {current_id}."
            )

        list_interval_ids = list(reversed(interval_ids))
        logger.info(
            f"Snapshots a serem processados do checkpoint {checkpoint_id} até o atual {current_id}: {list_interval_ids}"
        )

        return list_interval_ids

    def _extract_parquet_paths(
        self, manifest_entries_path: List[str], is_incremental: bool
    ) -> Tuple[List[str], List[str], List[str]]:
        """Analisa os arquivos Avro de manifesto coletando os caminhos fisicos dos Parquets por status."""
        df_entries = self.spark.read.format("avro").load(manifest_entries_path)

        if df_entries.filter(F.col("data_file.content") == 2).count() > 0:
            raise UnsupportedFeatureError(
                "A tabela de origem contem 'Equality Deletes' (content=2)."
            )

        if is_incremental:
            cond_data = (F.col("status") == 1) & (F.col("data_file.content") == 0)
            cond_delete = (F.col("status") == 1) & (F.col("data_file.content") == 1)
            cond_removed = (F.col("status") == 2) & (F.col("data_file.content") == 0)
        else:
            cond_data = (F.col("status").isin(0, 1)) & (F.col("data_file.content") == 0)
            cond_delete = (F.col("status").isin(0, 1)) & (
                F.col("data_file.content") == 1
            )
            cond_removed = F.lit(False)

        def _collect_paths(condition: F.Column) -> List[str]:
            return [
                row["path"]
                for row in df_entries.filter(condition)
                .select(F.col("data_file.file_path").alias("path"))
                .distinct()
                .collect()
            ]

        return (
            _collect_paths(cond_data),
            _collect_paths(cond_delete),
            _collect_paths(cond_removed),
        )

    def sync(self, last_checkpoint_id: Optional[int] = None) -> int:
        """Executa a sincronizacao entre as tabelas de Origem e Destino."""
        df_meta_raw = self.spark.read.option("multiline", "true").json(
            self.metadata_path
        )

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

        avro_file = meta_info["manifest_list_path"]
        current_snapshot_id = int(meta_info["current_snapshot_id"])
        current_schema_id = int(meta_info["current_schema_id"])

        if last_checkpoint_id and current_snapshot_id == last_checkpoint_id:
            logger.info("Nenhuma alteracao detectada na origem Iceberg.")
            return current_snapshot_id

        col_rows = (
            df_meta_raw.select(F.explode("schemas").alias("sch"))
            .filter(F.col("sch.schema-id") == F.lit(current_schema_id))
            .select(F.explode("sch.fields").alias("field"))
            .select(F.col("field.name").alias("column_name"))
            .collect()
        )
        col_final = [row["column_name"] for row in col_rows] + ["_fp", "_rid"]

        is_incremental = last_checkpoint_id is not None
        df_manifests_base = (
            self.spark.read.format("avro")
            .load(avro_file)
            .filter(F.col("content").isin(0, 1))
        )

        if is_incremental:
            logger.info(f"Modo INCREMENTAL. Checkpoint base: {last_checkpoint_id}")
            valid_ids = self._build_snapshot_interval(
                last_checkpoint_id, current_snapshot_id
            )
            df_manifests = df_manifests_base.filter(
                F.col("added_snapshot_id").isin(valid_ids)
            )
        else:
            logger.info("Modo FULL.")
            df_manifests = df_manifests_base

        manifest_paths = [
            row["p"]
            for row in df_manifests.select(F.col("manifest_path").alias("p"))
            .distinct()
            .collect()
        ]

        if not manifest_paths and is_incremental:
            logger.info("Nenhum manifesto modificado encontrado.")
            return current_snapshot_id

        data_paths, delete_paths, removed_paths = self._extract_parquet_paths(
            manifest_paths, is_incremental
        )

        df_final_limpo = None
        if data_paths:
            df_dados_reais = (
                self.spark.read.option("mergeSchema", "true")
                .parquet(*data_paths)
                .withColumn(
                    "_fp",
                    F.regexp_replace(
                        F.col("_metadata.file_path"), r"^s3[a-z0-9]*://", ""
                    ),
                )
                .withColumn("_rid", F.col("_metadata.row_index").cast("long"))
            )

            if delete_paths:
                df_deletes = (
                    self.spark.read.parquet(*delete_paths)
                    .select(
                        F.regexp_replace(
                            F.col("file_path"), r"^s3[a-z0-9]*://", ""
                        ).alias("_fp"),
                        F.col("pos").cast("long").alias("_rid"),
                    )
                    .dropDuplicates(["_fp", "_rid"])
                )
                df_final_limpo = df_dados_reais.join(
                    df_deletes, ["_fp", "_rid"], "left_anti"
                ).select(*col_final)
            else:
                df_final_limpo = df_dados_reais.select(*col_final)

        if not is_incremental:
            if df_final_limpo is not None:
                df_final_limpo.write.format("delta").mode("overwrite").save(
                    self.target_table
                )
                logger.info("Tabela destino inicializada via carga FULL.")
        else:
            df_target = DeltaTable.forPath(self.spark, self.target_table)
            has_mutations = False

            if delete_paths:
                df_del_target = (
                    self.spark.read.parquet(*delete_paths)
                    .select(
                        F.regexp_replace(
                            F.col("file_path"), r"^s3[a-z0-9]*://", ""
                        ).alias("_fp"),
                        F.col("pos").cast("long").alias("_rid"),
                    )
                    .dropDuplicates(["_fp", "_rid"])
                )

                (
                    df_target.alias("target")
                    .merge(
                        df_del_target.alias("source"),
                        "target._fp = source._fp AND target._rid = source._rid",
                    )
                    .whenMatchedDelete()
                    .execute()
                )
                logger.info("Position Deletes sincronizados com sucesso.")
                has_mutations = True

            if removed_paths:
                logger.info(
                    f"Removendo referências de {len(removed_paths)} arquivos otimizados."
                )
                norm_paths = [
                    (
                        p.replace("s3://", "")
                        .replace("s3a://", "")
                        .replace("s3n://", ""),
                    )
                    for p in removed_paths
                ]

                df_del_optimize = self.spark.createDataFrame(norm_paths, ["_fp"])
                (
                    df_target.alias("target")
                    .merge(
                        df_del_optimize.alias("source"),
                        "target._fp = source._fp",
                    )
                    .whenMatchedDelete()
                    .execute()
                )

                logger.info("Registros historicos orfaos limpos do DELTA destino.")
                has_mutations = True

            if df_final_limpo is not None:
                (
                    df_target.alias("target")
                    .merge(
                        df_final_limpo.alias("source"),
                        "target._fp = source._fp AND target._rid = source._rid",
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )

                logger.info("Novos registros anexados com sucesso.")
                has_mutations = True

            if has_mutations:
                logger.info(
                    f"Sincronizacao concluida com sucesso para o snapshot: {current_snapshot_id}"
                )
            else:
                logger.info("Nenhuma modificacao fisica ou logica pendente encontrada.")

        return current_snapshot_id

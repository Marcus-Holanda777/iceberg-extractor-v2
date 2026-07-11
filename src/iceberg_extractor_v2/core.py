"""
Modulo principal de orquestracao para leitura incremental e full de tabelas Apache Iceberg V2 para Delta.

Este modulo implementa a logica central de sincronizacao de dados entre tabelas Apache Iceberg e
um storage Delta. Ele funciona como uma fachada (Facade) que coordena multiplos componentes
desacoplados para realizar operacoes de carga incremental e completa.

Funcionalidades principais:
    - Leitura de metadados brutos de tabelas Iceberg (table metadata)
    - Extracao de informacoes de snapshot (versoes) da tabela
    - Mapeamento dinamico de colunas baseado em schema IDs
    - Deteccao de mudancas entre snapshots (incremental)
    - Processamento de manifestos Avro para localizar arquivos fisicos
    - Extracao de caminhos de dados e deletes posicionais de arquivos Parquet
    - Tratamento de exclusoes logicas via delete files
    - Deteccao de otimizacoes/compactacoes atraves de analise de set difference
    - Sincronizacao inteligente com suporte a checkpoint para continuidade

O pipeline segue estas etapas:
    1. Coleta e validacao de metadados brutos
    2. Mapeamento dinamico de colunas baseado no schema atual
    3. Filtragem de manifestos relevantes (incremental ou full)
    4. Extracao de caminhos fisicos de dados e deletes
    5. Leitura e limpeza de arquivos Parquet
    6. Aplicacao de filtros de delete files
    7. Persistencia das alteracoes via camada de Storage

"""

import logging
from typing import List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from .catalog import IcebergCatalog
from .exceptions import UnsupportedFeatureError
from .storage import DeltaStorageEngine

logger = logging.getLogger("iceberg_incremental_reader.core")


class IcebergIncrementalReaderV2:
    """Orquestrador (Facade) de leitura incremental e full de tabelas Apache Iceberg V2 para Delta."""

    def __init__(
        self, spark: SparkSession, table_directory_path: str, target_table: str
    ) -> None:
        """Inicializa o orquestrador de leitura incremental de tabelas Iceberg.

        Args:
            spark: Sessão SparkSession ativa para operacoes distribuidas.
            table_directory_path: Caminho do diretorio raiz da tabela Iceberg.
            target_table: Nome ou caminho da tabela Delta destino.
        """
        self.spark = spark

        # Componentes desacoplados injetados internamente
        self.catalog = IcebergCatalog(spark, table_directory_path)
        self.storage = DeltaStorageEngine(spark, target_table)

    def _extract_parquet_paths(
        self, manifest_entries_path: List[str], is_incremental: bool
    ) -> Tuple[List[str], List[str]]:
        """Analisa os manifestos Avro coletando caminhos físicos de dados e deletes posicionais.

        Args:
            manifest_entries_path: Lista de caminhos dos arquivos de manifesto Avro.
            is_incremental: Flag indicando se a leitura é incremental ou full.

        Returns:
            Tupla contendo (caminhos_dados, caminhos_deletes) com os arquivos Parquet identificados.

        Raises:
            UnsupportedFeatureError: Quando detectados Equality Deletes (content=2) na tabela.
        """
        df_entries = self.spark.read.format("avro").load(manifest_entries_path)

        if df_entries.filter(F.col("data_file.content") == 2).count() > 0:
            raise UnsupportedFeatureError(
                "A tabela de origem contem 'Equality Deletes' (content=2)."
            )

        if is_incremental:
            cond_data = (F.col("status") == 1) & (F.col("data_file.content") == 0)
            cond_delete = (F.col("status") == 1) & (F.col("data_file.content") == 1)
        else:
            cond_data = (F.col("status").isin(0, 1)) & (F.col("data_file.content") == 0)
            cond_delete = (F.col("status").isin(0, 1)) & (
                F.col("data_file.content") == 1
            )

        def _collect_paths(condition: F.Column) -> List[str]:
            return [
                row["path"]
                for row in df_entries.filter(condition)
                .select(F.col("data_file.file_path").alias("path"))
                .distinct()
                .collect()
            ]

        return _collect_paths(cond_data), _collect_paths(cond_delete)

    def sync(self, last_checkpoint_id: Optional[int] = None) -> int:
        """Executa o pipeline completo de sincronizacao entre Iceberg e Delta.

        Coordena todas as etapas do processo: coleta de metadados, mapeamento dinamico de colunas,
        filtragem de manifestos, extracao de caminhos fisicos, leitura de arquivos Parquet,
        aplicacao de filtros de delete files e persistencia das alteracoes.

        Args:
            last_checkpoint_id: ID do ultimo snapshot processado. Se None, executa full load.
            Se fornecido, executa carga incremental a partir deste checkpoint.

        Returns:
            ID do snapshot atual processado (current_snapshot_id).

        Raises:
            UnsupportedFeatureError: Quando a tabela contem Equality Deletes não suportados.
        """

        df_meta_raw = self.catalog.load_raw_metadata()
        meta_info = self.catalog.extract_meta_info(df_meta_raw)

        current_snapshot_id = meta_info["current_snapshot_id"]
        avro_file = meta_info["manifest_list_path"]
        current_schema_id = meta_info["current_schema_id"]

        if last_checkpoint_id and current_snapshot_id == last_checkpoint_id:
            logger.info("Nenhuma alteracao detectada na origem Iceberg.")
            return current_snapshot_id

        columns = self.catalog.get_schema_columns(df_meta_raw, current_schema_id)
        col_final = columns + ["_fp", "_rid"]

        is_incremental = last_checkpoint_id is not None
        df_manifests_base = (
            self.spark.read.format("avro")
            .load(avro_file)
            .filter(F.col("content").isin(0, 1))
        )

        removed_paths = []
        if is_incremental:
            logger.info(f"Modo INCREMENTAL. Checkpoint base: {last_checkpoint_id}")
            valid_ids = self.catalog.build_snapshot_interval(
                df_meta_raw, last_checkpoint_id, current_snapshot_id
            )
            df_manifests = df_manifests_base.filter(
                F.col("added_snapshot_id").isin(valid_ids)
            )

            logger.info(
                "Mapeando morfologia dos snapshots para detectar compactacoes/exclusoes..."
            )
            files_at_checkpoint = self.catalog.get_active_files_at_snapshot(
                last_checkpoint_id, df_meta_raw
            )
            files_at_current = self.catalog.get_active_files_at_snapshot(
                current_snapshot_id, df_meta_raw
            )

            removed_paths = list(files_at_checkpoint - files_at_current)
            if removed_paths:
                logger.info(
                    f"Set Difference detectou {len(removed_paths)} arquivos removidos (Optimize/Exclusoes)."
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

        if not manifest_paths and not removed_paths and is_incremental:
            logger.info("Nenhuma modificacao pendente encontrada no intervalo.")
            return current_snapshot_id

        data_paths, delete_paths = self._extract_parquet_paths(
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
                self.storage.write_full_load(df_final_limpo)
                logger.info("Tabela destino inicializada via carga FULL.")
        else:
            has_mutations = self.storage.apply_incremental_mutations(
                df_final_limpo, delete_paths, removed_paths
            )
            if has_mutations:
                logger.info(
                    f"Sincronizacao concluida com sucesso para o snapshot: {current_snapshot_id}"
                )
            else:
                logger.info("Nenhuma modificacao fisica ou logica pendente encontrada.")

        return current_snapshot_id

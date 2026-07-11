"""
Modulo de persistencia e mutacoes Delta para o pipeline de extracao.

Este arquivo contem a implementacao de um mecanismo de armazenamento Delta Lake
capaz de realizar cargas completas e aplicar mutacoes incrementais em tabelas
Delta, seja por caminho (S3) ou por nome de tabela registrada.

As responsabilidades incluem:
- Detectar se o destino e um caminho S3 ou uma tabela Delta nomeada.
- Executar gravacoes completas em modo overwrite com merge de esquema.
- Aplicar deletes posicionais e expurgos de arquivos obsoletos.
- Inserir novos registros limpos de forma incremental.

A implementacao depende de PySpark e Delta Lake, usando APIs de DeltaTable
para garantir operações ACID e compatibilidade com workloads de streaming
ou batch.
"""

import logging
from typing import List, Optional

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .exceptions import (
    DeltaMutationError,
    DeltaStorageWriteError,
    DeltaTableResolutionError,
)

logger = logging.getLogger("iceberg_incremental_reader.storage")


class DeltaStorageEngine:
    """Gerenciador de I/O e mutacoes (Merge/Overwrite) para o destino Delta Lake."""

    def __init__(self, spark: SparkSession, target_table: str) -> None:
        """Inicializa a engine de armazenamento Delta Lake.

        Args:
            spark (SparkSession): Sessao Spark ativa usada para leitura e escrita.
            target_table (str): Caminho S3 ou nome da tabela Delta registrada que será o destino.

        A inicializacao determina se o destino e baseado em caminho (S3) ou em tabela
        pelo sufixo do target_table. O logger registra o tipo de destino detectado.
        """
        self.spark = spark
        self.target_table = target_table
        self.is_path_based = self._is_path_based(target_table)
        logger.info(
            f"Destino Delta {'baseado em caminho' if self.is_path_based else 'baseado em tabela'}: {target_table}"
        )

    def _is_path_based(self, path: str) -> bool:
        """Verifica se o destino Delta está baseado em caminho S3.

        Args:
            path (str): O destino Delta passado como string.

        Returns:
            bool: True se o destino for um caminho S3 compatível, False se for um nome de tabela.
        """
        return path.startswith(("s3://", "s3a://", "s3n://"))

    def _get_delta_table(self) -> DeltaTable:
        """Obtem a instancia DeltaTable para o destino configurado.

        Quando o destino e um caminho S3, usa DeltaTable.forPath.
        Quando o destino e uma tabela registrada, usa DeltaTable.forName.

        Returns:
            DeltaTable: instancia DeltaTable referente ao destino final.

        Raises:
            DeltaTableResolutionError: em caso de falha ao obter a instancia DeltaTable.
        """
        try:
            if self.is_path_based:
                dt = DeltaTable.forPath(self.spark, self.target_table)
            else:
                dt = DeltaTable.forName(self.spark, self.target_table)
            logger.debug(
                f"Obtido DeltaTable para {'path' if self.is_path_based else 'table'}: {self.target_table}"
            )
            return dt
        except Exception as exc:
            logger.exception(
                f"Falha ao obter DeltaTable para {self.target_table}: {exc}"
            )
            raise DeltaTableResolutionError(
                f"Falha ao obter DeltaTable para {self.target_table}: {exc}"
            ) from exc

    def write_full_load(self, df: DataFrame) -> None:
        """Grava um DataFrame completo no destino Delta em modo overwrite.

        A escrita e feita com merge de esquema habilitado para garantir compatibilidade
        de colunas entre o DataFrame de origem e o destino Delta. Se o destino for um
        caminho S3, chama save. Se for uma tabela registrada, chama saveAsTable.

        Args:
            df (DataFrame): DataFrame que será gravado como carga completa.

        Raises:
            DeltaStorageWriteError: se ocorrer qualquer erro durante a escrita no destino Delta.
        """
        logger.info(f"Iniciando gravacao full load no destino: {self.target_table}")
        try:
            writer = (
                df.write.format("delta").option("mergeSchema", "true").mode("overwrite")
            )
            if self.is_path_based:
                writer.save(self.target_table)
            else:
                writer.saveAsTable(self.target_table)
            logger.info("Full load concluido com sucesso.")
            logger.debug(
                f"DataFrame escrito com {df.count()} linhas e {len(df.columns)} colunas"
            )
        except Exception as exc:
            logger.exception(
                f"Erro ao executar full load para {self.target_table}: {exc}"
            )
            raise DeltaStorageWriteError(
                f"Erro ao executar full load para {self.target_table}: {exc}"
            ) from exc

    def apply_incremental_mutations(
        self,
        df_final_limpo: Optional[DataFrame],
        delete_paths: List[str],
        removed_paths: List[str],
    ) -> bool:
        """Aplica mutacoes incrementais no destino Delta.

        O processo e dividido em tres fases:
        1) Position Deletes: le arquivos Parquet contendo referencias de arquivo e posicao
           e exclui registros correspondentes na tabela Delta.
        2) Expurgos de otimização: remove registros historicamente órfãos que pertencem a
           arquivos obsoletos listados em removed_paths.
        3) Inserções incrementais: adiciona novos registros limpos ao Delta se fornecidos.

        Args:
            df_final_limpo (Optional[DataFrame]): DataFrame com novos registros limpos a serem inseridos.
            delete_paths (List[str]): Lista de caminhos parquet contendo informações de deletions posicionais.
            removed_paths (List[str]): Lista de caminhos de arquivos obsoletos para expurgo de registros.

        Returns:
            bool: True se alguma mutação foi aplicada (delete, expurgo ou insert), False caso contrario.

        Raises:
            DeltaMutationError: se algum erro ocorrer durante a aplicacao das mutacoes incrementais.

        """

        df_target = self._get_delta_table()
        has_mutations = False

        try:
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

                try:
                    del_count = df_del_target.count()
                except Exception:
                    del_count = None

                logger.debug(f"Position deletes dataframe rows: {del_count}")

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
                    f"Executando expurgo agnostico de registros vinculados a {len(removed_paths)} arquivos obsoletos."
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

                try:
                    opt_count = df_del_optimize.count()
                except Exception:
                    opt_count = None

                logger.debug(
                    f"Optimize-delete dataframe rows (removed_paths): {opt_count}"
                )

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
                try:
                    ins_count = df_final_limpo.count()
                except Exception:
                    ins_count = None

                logger.debug(f"Dataframe de insercao incremental rows: {ins_count}")

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

        except Exception as exc:
            logger.exception(
                f"Erro ao aplicar mutacoes incrementais para {self.target_table}: {exc}"
            )
            raise DeltaMutationError(
                f"Erro ao aplicar mutacoes incrementais para {self.target_table}: {exc}"
            ) from exc

        return has_mutations

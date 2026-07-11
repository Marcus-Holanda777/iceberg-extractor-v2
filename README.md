# Iceberg to Delta Incremental Reader (V2)

> [!WARNING]
> **AVISO DE ESCOPO E USO:** Este projeto foi desenvolvido **estritamente para fins de testes, estudo e desenvolvimento de habilidades pessoais em engenharia de dados**. Ele **NÃO é uma garantia de 100% de sucesso em produção** e deve ser encarado como uma Prova de Conceito (PoC) educacional e experimental.

---

## 💡 Motivação e Contexto Técnico

A inspiração para o desenvolvimento deste motor surgiu de um desafio técnico crítico em ecossistemas modernos de Big Data: **a limitação do Databricks em ler tabelas Apache Iceberg catalogadas no AWS Glue que possuem arquivos de deleção ativos (*Row-Level Deletes*)**.

Em arquiteturas que sofrem mutações constantes (como rotinas de `UPDATE` e `DELETE`), o formato Iceberg V2 gera arquivos secundários de deleção. No entanto, ao tentar consumir essas tabelas de dentro do Databricks **utilizando o recurso de Catálogo Federado (*Lakehouse Federation*) para mapear o catálogo externo do AWS Glue**, essa camada de federação frequentemente falha em aplicar as deleções lógicas, gerando inconsistências silenciosas na leitura dos dados.

---

### A Solução Ideal vs. A Solução Proposta

* **A Melhor Prática (Origem):** Do ponto de vista de governança e arquitetura, a melhor solução definitiva sempre será **executar a otimização e manutenção das tabelas diretamente na origem dentro do ecossistema AWS/Glue** (como agendar rotinas de `OPTIMIZE` e `REWRITE DATA FILES` via Athena ou EMR Serverless). Isso consolida as deleções posicionais de volta nos arquivos Parquet de dados, eliminando o problema na raiz.
* **A Nossa Opção (Este Pacote):** Como alternativa técnica de aprendizado e engenharia reversa, este pacote foi construído como **mais uma opção no arsenal do desenvolvedor**. Ele ignora a camada de abstração do conector, decodificando o estado da tabela diretamente pela árvore de manifestos e arquivos brutos armazenados no S3.

---

## 🚀 Engenharia e Lógica de Funcionamento

O leitor descarta intermediários e inspeciona de forma puramente técnica a infraestrutura imutável do Apache Iceberg no S3, dividindo seu ciclo de vida em três etapas essenciais:

### 1. Descoberta Dinâmica de Metadados

Ao ser instanciado, o construtor executa uma varredura adaptativa para localizar o arquivo de metadados mais recente no diretório `/metadata/` da origem no S3. O motor tenta, prioritariamente, utilizar a API nativa do **Databricks SDK (`WorkspaceClient`)** para listar os arquivos de forma segura e compatível com ambientes Serverless. Caso a execução ocorra em um ambiente local (onde o SDK não está disponível ou lança erro), o sistema aciona um mecanismo de *fallback* automático, instanciando a classe **`FileSystem` do Hadoop** através do gateway JVM do PySpark. A partir dessa listagem, o motor decodifica o arquivo `.metadata.json` mais recente com base no carimbo de modificação física (`modification_time`), extraindo dinamicamente o ponteiro do snapshot ativo (`current-snapshot-id`) e o ID do esquema atual (`current-schema-id`).

### 2. Carga Completa (Modo FULL)

Quando o método `sync()` é acionado sem um checkpoint prévio (`last_checkpoint_id=None`), o motor:

* Varre a lista completa de manifestos ativos pertencentes à fotografia atual da tabela.
* Carrega os arquivos Parquet físicos de dados (`content=0`) cujo estado de criação seja ativo (`status` igual a `0` ou `1`).
* Captura em tempo de execução metadados virtuais injetados pelo leitor de Parquet do Spark: o caminho do arquivo de origem (`_metadata.file_path`) e o índice sequencial exato da linha dentro daquele arquivo físico (`_metadata.row_index`).
* Carrega os arquivos de *Position Deletes* (`content=1`) ativos e aplica uma junção de exclusão do tipo `left_anti`.
* Inicializa a estrutura no Delta destino materializando as colunas ocultas de controle como campos físicos chamados `_fp` (File Path normalizado) e `_rid` (Row Index).

### 3. Sincronização Incremental Blindada (Modo INCREMENTAL)

Ao receber o ID do último snapshot processado, o leitor reconstrói cronologicamente a linhagem linear de snapshots até o estado atual. A mutação no Delta de destino é executada em três passos isolados e sequenciais para garantir atomicidade:

```
[Passo 1: Position Deletes] ──> [Passo 2: Expurgo Set Difference] ──> [Passo 3: Inserts/Mutações]

```

* **Passo 1 (Position Deletes):** Identifica novos arquivos de deleção por posição gerados no intervalo e dispara um `MERGE ... WHEN MATCHED THEN DELETE` no Delta casando as assinaturas `_fp` e `_rid`.
* **Passo 2 (Expurgo de Compactação):** Executa o algoritmo de morfologia detalhado a seguir.
* **Passo 3 (Inserts/Mutações):** Anexa os novos registros inéditos cujo par `_fp` e `_rid` não encontre correspondência na tabela Delta de destino.

---

## 🧠 Algoritmo de Expurgo Agnóstico (*Set Difference*)

### O Desafio do `OPTIMIZE` na Origem

Quando motores analíticos (como o AWS Athena) executam rotinas de compactação de arquivos para mitigar o problema de *small files*, o Iceberg cria novos arquivos Parquet consolidados e marca os antigos como removidos (`status=2`).

Em janelas de execução muito rápidas, ou se processos automáticos de manutenção da tabela executarem a expiração de histórico (*Snapshot Expiration*), os marcadores com `status=2` somem dos manifestos do intervalo. Se o leitor incremental avaliar apenas o intervalo de logs, ele lerá o arquivo novo gerado pelo `OPTIMIZE` como se fossem dados inéditos, gerando **duplicação massiva de registros**, dado que o nome do arquivo (`_fp`) mudou.

### A Solução por Morfologia de Fotografia

Para manter o motor 100% genérico e agnóstico (sem a necessidade de mapear chaves primárias de negócio de forma fixa no código), a versão **V2** implementa o método `_get_active_files_at_snapshot`.

O algoritmo ignora os logs de transição do intervalo e reconstrói matematicamente dois conjuntos distintos: os arquivos de dados válidos na época exata do Checkpoint anterior ($\mathbb{A}$) e os arquivos de dados válidos no Snapshot Atual ($\mathbb{B}$). Aplicando o conceito puramente matemático de **Diferença de Conjuntos** ($\mathbb{A} \setminus \mathbb{B}$), descobrimos cirurgicamente quais arquivos deixaram de existir na foto atual da tabela.

$$\text{Caminhos Removidos} = \text{Arquivos Ativos}_{(\text{Checkpoint})} \setminus \text{Arquivos Ativos}_{(\text{Atual})}$$

No Delta de destino, um `MERGE INTO ... ON target._fp = source._fp WHEN MATCHED THEN DELETE` limpa o terreno apagando todos os registros vinculados aos arquivos obsoletos. Logo em seguida, o Passo 3 insere as linhas do arquivo novo consolidado sem qualquer risco de duplicação física.

---

## 🧪 Caso de Teste e Homologação (Janela Crítica no AWS Athena)

Para testar a resiliência do algoritmo contra a volatilidade extrema de metadados, o pacote foi validado simulando o cenário mais complexo para pipelines incrementais de CDC: uma janela de processamento contendo múltiplas mutações físicas e reestruturações lógicas executadas consecutivamente pela engine do **AWS Athena**.

O fluxo do teste executou a seguinte sequência de comandos na origem:

```
[DELETE] ──> [UPDATE] ──> [OPTIMIZE] ──> [INSERT]

```

* **O Comportamento do Athena:** O `DELETE` e o `UPDATE` geraram arquivos de *Position Deletes*. Na sequência, o `OPTIMIZE` consolidou os dados remanescentes em um novo arquivo Parquet com nome inédito e limpou os metadados antigos do histórico, ocultando os registros com `status=2`. O `INSERT` final adicionou novos dados imediatamente após a compactação.
* **O Resultado na V2:** Ao rodar o sincronismo incremental, a lista tradicional de arquivos removidos via manifesto veio inteiramente vazia da AWS. Contudo, a validação por *Set Difference* capturou a ausência dos arquivos consolidados comparando as duas fotos. O Delta destino expurgou os dados órfãos e absorveu as novas inserções e atualizações com **sucesso absoluto e zero duplicações**.

---

## 📚 Sustentação Teórica (Apache Iceberg Table Spec V2)

As decisões de engenharia aplicadas neste projeto encontram respaldo direto na especificação técnica oficial do **Apache Iceberg Table Spec V2**:

* **Imutabilidade e Isolamento (Snapshots):** De acordo com a especificação, um *Snapshot* representa o estado exato de uma tabela em um momento determinado do tempo. O uso de arquivos de índice estáticos (`manifest-list`) permite uma auditoria determinística de estados, viabilizando o cálculo de diferença de conjuntos de forma segura.
* **Row-Level Deletes por Posição:** A especificação V2 oficializa o tipo de arquivo `content=1` (*Position Deletes*), ditando que cada registro aponta obrigatoriamente para um arquivo de dados (`file_path`) e para a posição absoluta base zero da linha (`pos`). Nossa classe espelha essa regra mapeando de forma idêntica as colunas de sistema do Spark:
* `data_file.file_path` $\rightarrow$ Normalizado na coluna `_fp`.
* `pos` $\rightarrow$ Mapeado na coluna `_rid`.


* **Equality Deletes (`content=2`):** De acordo com a regra do Iceberg, deleções por igualdade exigem a interpretação de valores de dados em tempo de execução correlacionando IDs de campos de negócio. O leitor valida defensivamente e lança `UnsupportedFeatureError` caso os encontre, blindando o pipeline contra perda silenciosa de dados.

---

## 📦 Instalação e Inteligência de Destinos

Como o projeto é estruturado utilizando o gerenciador moderno **`uv`**, você pode empacotar o código localmente gerando um arquivo Wheel (`.whl`) para implantação em clusters locais ou no Databricks.

### 1. Compilar o Pacote

Na pasta raiz do projeto (onde reside o arquivo `pyproject.toml`), execute:

```bash
uv build

```

O pacote compilado estará disponível em `dist/iceberg_incremental_reader_v2-0.2.1-py3-none-any.whl`.

### 2. Detecção Dinâmica de Destino (Híbrida)

A classe `IcebergIncrementalReaderV2` possui uma inteligência interna que avalia o formato da string fornecida no parâmetro `target_table` para gerenciar a persistência de forma transparente:

#### Modo A: Baseado em Caminho (Ideal para Testes Fora do Databricks / S3 Direto)

Se a string iniciar com o protocolo do S3, o leitor entende que deve ignorar o metastore, utilizando o método `.save()` e instanciando o manipulador via `DeltaTable.forPath`.

```python
reader = IcebergIncrementalReaderV2(
    spark=spark,
    table_directory_path="s3://meu-bucket/raw/tabela_iceberg",
    target_table="s3://meu-bucket/analytics/tabela_delta" # <-- Detectado como Caminho (Path)
)

```

#### Modo B: Baseado em Tabela (Ideal para Produção no Databricks / Unity Catalog)

Se for fornecido um identificador SQL simples ou qualificado de três níveis, o motor assume a gravação via `.saveAsTable()` e gerencia o estado através de `DeltaTable.forName`.

```python
reader = IcebergIncrementalReaderV2(
    spark=spark,
    table_directory_path="s3://meu-bucket/raw/tabela_iceberg",
    target_table="hive_metastore.teste.tabela_delta" # <-- Detectado como Catálogo (Name)
)

```

---

## 🛠️ Exemplo de Execução Prática

Antes de rodar, garanta que a evolução automática de esquema esteja ativa na sua sessão para permitir a absorção de mudanças de contrato da origem (*Schema Drift*):

```python
from iceberg_extractor_v2 import IcebergIncrementalReaderV2

# Ativa a evolução automática exigida pelo Delta Lake
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# 1. Instancia o leitor passando o diretório base (o motor descobre o metadado sozinho)
reader = IcebergIncrementalReaderV2(
    spark=spark,
    table_directory_path="s3://seu-bucket-name/tables/nome_tabela/",
    target_table="s3://seu-bucket-name/delta/nome_tabela_delta/"
)

# 2. Executa a sincronização (Se omitido ou None, roda no modo FULL automático)
# O ID retornado deve ser armazenado como o seu ponto de controle (Checkpoint)
proximo_checkpoint = reader.sync(last_checkpoint_id=11111111111111111)

```

---

## 🗂️ Estrutura do Projeto

* **`exceptions.py`**: Exceções customizadas para isolamento de erros de linhagem (`CheckpointExpiredError`, `LineageBrokenError`, `UnsupportedFeatureError`).
* **`pyproject.toml`**: Metadados de compilação gerenciados pelo `uv` utilizando o backend `hatchling`.
* **`core.py`**: Classe mestre contendo a lógica de orquestração para leitura incremental e full, coordenando catalog e storage.
* **`catalog.py`**: Responsável pela descoberta e leitura de metadados Iceberg, snapshots, manifests e cálculo de arquivos ativos por snapshot.
* **`storage.py`**: Implementa a engine de persistência Delta, aplicando cargas completas e mutações incrementais (position deletes, expurgos por arquivos obsoletos e inserts).
* **`exceptions.py`**: Exceções customizadas para isolamento de erros de linhagem (`CheckpointExpiredError`, `LineageBrokenError`, `UnsupportedFeatureError`).
* **`pyproject.toml`**: Metadados de compilação gerenciados pelo `uv` utilizando o backend `hatchling`.

---
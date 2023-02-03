# Projeto
Tratar informações sobre o Coronavírus (COVID-19)

# Objetivo
Desenvolver uma ETL em um novo projeto, que tem como objetivo, tratar informações sobre o Coronavírus (COVID-19).

A ETL deverá consumir informações de uma base de dados fornecida pela Universidade de Johns Hopkins, em formato CSV.

Os dados deverão ser processados utilizando o Apache Spark e a criação do _pipeline_ de dados com orquestração via Apache Airflow, conforme regras de processamento e armazenamento definidas a seguir.

# Desenho do Projeto
## Origem dos dados
Os dados são disponibilizados pela Universidade de Johns Hopkins nesse [link](https://github.com/CSSEGISandData/COVID-19). Neste projeto os dados já estão disponíveis na pasta: **/datalake/raw/covid19**.

|Arquivo|Descrição|
|---|---|
|time_series_covid19_confirmed_global.csv|Contém informações sobre os casos **confirmados** do vírus|
|time_series_covid19_deaths_global.csv|Contém informações sobre as **mortes** confirmadas do vírus|
|time_series_covid19_recovered_global.csv|Contém informações sobre os casos de **recuperação** do vírus|

### Descrição das colunas
Cada arquivo mencionado anteriormente contém as mesmas colunas, seguem as descrições:

|Coluna|Descrição|
|---|---|
|Province/State|Estado ou Província|
|Country/Region|País ou Região|
|Lat|Latitude|
|Long|Longitude|
|Demais colunas|Contém as datas e os valores **acumulados** dos casos confirmados, as datas estão no formato MM/DD/YY|

### Requisitos de Projeto

O ambiente designado para desenvolver a solução foi pre-configurado com o Airfow e o Spark em uma imagem docker. 

Para executar este ambiente será necessário instalar em seu equipamento o Docker Desktop. Veja como na seção [Software necessário](#software-necessário).

### Parametros de Projeto

#### A solução deverá ser desenvolvida utilizando os diretórios deste repositório:

|Diretório|Descrição|
|---|---|
|datalake|Deverá conter os dados brutos e refinados da solução|
|dags|Deverá conter os códigos em Python (PySpark) desenvolvidos juntamente com a DAG do Airflow que será desenvolvida|


**Importante:** Os diretórios citados acima e presentes nesse repositório, foram pre-mapeados na imagem docker (via volumes no docker-compose), desta forma basta desenvolver o código localmente que o mesmo irá refletir diretamente no container do docker em execução.

Os arquivos de dados originais requeridos para esta solução já estão presentes no diretório:
<pre><code>./datalake/raw/covid19</code></pre>

Podendo ser acessados através do seguinte caminho no contâiner:
<pre><code>/home/airflow/datalake/raw/covid19</code></pre>

#### A solução desenvolvida deverá atender os seguintes requisitos:
**Pipeline**:
- Deverá ser desenvolvida uma DAG no Apache Airflow com as seguintes características:
    - Intervalo de execução diário
    -  Minimamente 2 tasks: ingestão >> processamento
    - Utilização de PythonOperator nas tasks

**Processamento de Dados**
- O código da solução deverá ser desenvolvido utilizando o PySpark, podendo ser utilizadas as APIs: RDD, DataFrames ou Spark SQL.

#### A solução devenvolvida deverá conter as seguintes camadas

**Camada Trusted**

A solução deverá ser capaz de processar os dados contidos nos arquivos .CSV da pasta **datalake/raw/covid19**, efetuando uma unificação dos registros em uma única tabela e armazenando o seu resultado no diretório **datalake/trusted**.

A tabela desenvolvida nesta camada deverá atender a seguinte estrutura:
|Coluna|Descrição|Formato|
|---|---|---|
|pais|Deverá conter a descrição do País|string|
|estado|Deverá conter a descrição do Estado ou Província|string|
|latitude|Deverá conter coordenada geográfica de latitude|double|
|longitude|Deverá conter coordenada geográfica de longitude|double|
|data|Deverá conter a data do registro|timestamp|
|quantidade_confirmados|Deverá conter a quantidade de **novos** casos Confirmados na data específica|long|
|quantidade_mortes|Deverá conter a quantidade de **novas** Mortes na data específica|long|
|quantidade_recuperados|Deverá conter a quantidade de **novos** Recuperados na data específica|long|
|ano|Coluna de partitionamento que deverá conter o ano extraído da coluna data|int|
|mes|Coluna de partitionamento que deverá conter o mes extraído da coluna data|int|

**Camada Refined**

A solução deverá ser capaz de processar os dados contidos na tabela anteriormente criada na camada **trusted**, efetuando uma agregação e cálculo das médias móveis dos 3 tipos de casos nos últimos 7 dias, armazenando o seu resultado no diretório **datalake/refined**.

A tabela desenvolvida nesta camada deverá atender a seguinte estrutura:
|Coluna|Descrição|Formato|
|---|---|---|
|pais|Deverá conter a descrição do País|string|
|data|Deverá conter a data do registro|timestamp|
|media_movel_confirmados|Deverá conter a média móvel dos últimos 7 dias de casos Confirmados até data específica|long|
|media_movel_mortes|Deverá conter a média móvel dos últimos 7 dias de Mortes até data específica|long|
|media_movel_recuperados|Deverá conter a média móvel dos últimos 7 dias de Recuperações até data específica|long|
|ano|Coluna de partitionamento que deverá conter o ano extraído da coluna data|int|

**Armazenamento de Dados**
- Os dados nas camadas **trusted** e **refined** devem ser armazenados no formato **PARQUET**
- Os dados nas camadas **trusted** deverão estar particionados pelas colunas **ano e mes**
- Os dados nas camadas **refined** deverão estar particionados somente pela coluna **ano**
- Os dados nas camadas **trusted** e **refined** deverão conter apenas 1 arquivo **PARQUET** em cada Partição.

## Implementação

### Funções

Foram desenvolvidas duas funções para evitar a repetição no código: **unpivot** e **unacc**. Isso foi possível pois as fontes de dados têm o mesmo formato e as mesmas colunas.

A função **unpivot** é responsável por transpor as colunas de Data em linhas como descrito na [Camada Trusted](#camada-trusted). A função **unacc** é responsável pela subtração dos dias consecutivos transformando os dados acumulados em dados diários.

- Função **unpivot**:
    Foi utilizada a expressão com "stack" para transpor as colunas.
    ```
    unpivotExpr = f"stack({n}, {unpivotList}) as (Date, {unpivot_col_name})"

    unpivotDF = dataframe.select(fixed_cols + [f.expr(unpivotExpr)]) \
        .where(f"{unpivot_col_name} is not null")
    ```
    
- Função **unacc**:
    Foram subtraídos os valores de uma lista criada por uma _Window()_ que percorria as linhas seguindo a coluna de data.
    ```
    unw = Window().partitionBy('Province/State', 'Country/Region', 'Lat', 'Long').orderBy('Date').rowsBetween(-1, 0)

    def minus(lista):
        if len(lista) == 2:
            return lista[1] - lista[0]
        else:
            return lista[0]

    minus_udf = f.udf(minus, t.IntegerType())
    dataframe.withColumn(unacc_col, minus_udf(f.collect_list(unacc_col).over(w)).cast(t.LongType()))
    ```

### Pipeline

A pipeline foi dividia em duas etapas: **Raw to Trusted** e **Trusted to Refined**.

#### Raw to Trusted

Nessa etapa da pipeline serão realizados os seguintes processos:
- Extrair dos dados da pasta Raw;
    - Exemplo: 
        ```
        deaths = spark.read.csv(
            "/home/airflow/datalake/raw/covid19/time_series_covid19_deaths_global.csv",
            sep=',',
            inferSchema=True,
            header= True)
        ```

- Transformar dos dados, incluindo:
    - Transpor as colunas de data em linhas com os dados;
        - Exemplo:
            ```
            unpivot_deaths = unpivot(
                dataframe= deaths,
                unpivot_col_name= 'quantidade_mortes')
            ```

    - Calcular os valores diários de morte, recuperados e confirmados.
        - Exemplo:
            ```
            unacc_deaths = unacc_df(
                dataframe= unpivot_deaths,
                unacc_col= 'quantidade_mortes')
            ```

- Unir os conjuntos de dados;
    - Código:
        ```
        drcDF = unacc_deaths.na.fill("desconhecido")\
            .join(unacc_recovered.na.fill("desconhecido"),
                ['Province/State', 'Country/Region', 'Lat', 'Long', 'Date'],
                how= "outer")\
            .join(unacc_confirmed.na.fill("desconhecido"),
                ['Province/State', 'Country/Region', 'Lat', 'Long', 'Date'],
                how= "outer")

        drcDF = drcDF.na.fill(value= 0, subset= ["quantidade_mortes", "quantidade_recuperados", "quantidade_confirmados"])
        ```
- Renomear as colunas finais;
    - Código:
        ```
        drcDF = drcDF.toDF(
            *['estado', 'pais', 'latitude', 'longitude', 'data',
            'quantidade_mortes', 'quantidade_recuperados', 'quantidade_confirmados',
            'ano', 'mes'])
        ```
- Fixar _Data Type_ das colunas finais;
    - Código:
        ```
        fixing_type_col_list = [
            ('pais', t.StringType()),
            ('estado', t.StringType()),
            ('latitude', t.DoubleType()),
            ('longitude', t.DoubleType())]

        for fixing_type_col, col_type in fixing_type_col_list:
            drcDF = drcDF.withColumn(fixing_type_col, f.col(fixing_type_col).cast(col_type))
        ```
- Carregar os dados na pasta _Trusted_.
    - Código:
        ```
        drcDF.coalesce(1).write.parquet(
            path= "/home/airflow/datalake/trusted/",
            mode= 'overwrite',
            partitionBy= ["ano", "mes"])
        ```

#### Trusted to Refined

Nessa etapa serão executados os seguintes passos:
- Extrair dos dados da pasta _Trusted_;
    - Código:
        ```
        trusted = spark.read.parquet(
            "/home/airflow/datalake/trusted/",
            inferSchema=True, header= True)
        ```
- Calcular a média móvel dos últimos 7 dias de morte, recuperados e confirmados;
    - Código:
        ```
        w = Window().partitionBy('estado', 'pais', 'latitude', 'longitude').orderBy('data').rowsBetween(-6, 0)

        trusted = trusted\
        .withColumn('media_movel_mortes', f.avg("quantidade_mortes").over(w).cast(t.DoubleType()))\
        .withColumn('media_movel_recuperados', f.avg("quantidade_recuperados").over(w).cast(t.DoubleType()))\
        .withColumn('media_movel_confirmados', f.avg("quantidade_confirmados").over(w).cast(t.DoubleType()))\
        .select("pais", "data", "media_movel_confirmados", "media_movel_mortes",
            "media_movel_recuperados", "ano")
        ```
- Fixar _Data Type_ das colunas finais;
    - Código:
        ```
        fixing_type_col_list = [
            ('pais', t.StringType()),
            ('data', t.TimestampType()),
            ('ano', t.IntegerType())]

        for fixing_type_col, col_type in fixing_type_col_list:
            trusted = trusted.withColumn(fixing_type_col, f.col(fixing_type_col).cast(col_type))
        ```
- Carregar os dados na pasta _Refined_.
    - Código:
        ```
        trusted.coalesce(1).write.parquet(
            path= "/home/airflow/datalake/refined/",
            mode= 'overwrite',
            partitionBy= ["ano"])
        ```

### Desafios no desenvolvimento

#### Relato do desenvolvimento do projeto:
- **Início:**
Aos observar os dados, verifiquei que o formato na camada Raw teria que ser transposto para obter os dados como era proposto na camada Trusted.
Em minha experiência, julguei que transpor os dados seria mais complexo que gerar os dados diários a partir do acumulado então decidi que o primeiro problema a resolver seria o da transposição.

- **Transposição:**
Pesquisando por funções que executassem o que eu queria, percebi que o Pyspark Dataframe não tinha um método específico que o fizesse. Lendo mais um pouco, encontrei a solução que foi utilizada.
Logo surgiu um novo problema ao escrever a função. O nome da coluna "1/1/20" na verdade era lido como uma função matemática gerando erro no resultado.
Para corrigir isso decidi renomear todas as colunas que tivessem datas. Na prática renomeei todas as colunas após a quarta coluna. Isso poderia gerar um problema, pois se no futuro a ordem das colunas fosse alterada, isso poderia gerar erro no resultado. No entanto, foquei em concluir a pipeline, sabendo que poderia voltar nesse problema depois.
Outro problema que surgiu foi o tempo de execução para renomear todas as colunas.

- **Dados diários:**
Para o cálculo dos dados diários decidi executar o cálculo antes da transposição que já tinha desenvolvido. Desta forma poderia subtrair colunas subsequentes e obter o número de casos novos. Essa lógica também gerou problemas: o tempo de execução e o risco de alteração da ordem das colunas. Esses problemas seriam posteriormente revistos.

- **Funções:**
Observei que as fontes de dados eram similares então decidi criar uma função para não precisar repetir a mesma operação ao longo do código.

- **Unindo os dados:**
Com as colunas montadas para cada fonte de dados, decidi uni-las.
Inicialmente fiz um _**join left**_, mas observei que a quantidade de linhas mudava, o que indicava que alguns dados não era encontrados.
Para corrigir isso, inicialmente substitui os valores nulos nas colunas de localização por "desconhecido", além disso, realizei um _**join outer**_ pois assim não perderia nenhum dado de nenhuma região. Esse _**join**_ geraria alguns valores nulos, mas decidi por substituí-los por zero, já que se tratavam de "casos novos" e a ausência de dados poderia ser entendida como zero casos novos.
Outra coisa observada foi que algumas regiões não estavam presentes em todas as fontes de dados. Para estes casos também foi aplicada a mesma tratativa: substituir os valores nulos por zero. Desta forma, não há perda de dados, mas isso limita o escopo de análise, limitando a menor granularidade ao país.
Por exemplo, o Canadá tem os dados de morte dividos por estado, no entanto, o dado de recuperados é do país inteiro. Desta forma, só podemos analisar os números de mortes e recuperados separadamente, ou, para analisarmos esse números comparativamente, devemos analisar em um nível de País.

- **Média Móvel:**
Por já ter realizado este cálculo, a pesquisa pela solução foi mais simples. Desenvolvida a solução, apliquei nos dados e validei utilizando o Estado de "Hubei" e "Hebei" na China.

- **Retornando ao início:**
Tendo em mente a forma como calculei a Média Móvel decidi alterar meu cálculo dos Dados Diários. Utilizando a função Window e de uma nova função criada, consegui calcular os Dados Diários sem o uso de loops. Além disso, inverti a ordem de execução, executando primeiro a Transposição e depois o cálculo dos Dados Diários. Desta forma, pude alterar o Data Type da coluna "Date" e assim garantir que, independente da ordem inicial das colunas, isso não geraria problema no resultado.
Resolvido esses dois problemas, faltavam os dois primeiros: o tempo de execução da transposição dos dados e garantir que mudanças na ordem das colunas não alteraria os resultados.
Primeiro observei que o que aumentava o tempo de execução era causado pela forma de renomear as colunas. Para isso, encontrei outra forma de renomear que não utiliza loop. Por fim, para garantir que não haveria erro se as colunas mudassem de ordem, adicionei uma compreensão de lista que filtrava corretamente as colunas, impedindo que ocorressem erros causados pela mudança da ordem das colunas.

- **Final:**
O último passo foram pequenas correções solicitados no projeto como:
    - Corrigir nome de colunas;
    - Fixar formato das colunas;
    - Salvar nos caminhos e camadas corretas com o formato e as partições solicitadas;
    - Determinar a execução diária do Airflow;
    - Escrever a documentação.

## Outros

### Software necessário

#### Docker Desktop

O ambiente designado para desenvolver a solução foi pre-configurado com o Airfow e o Spark em uma imagem docker. 

Para executar este ambiente será necessário instalar em seu equipamento o Docker Desktop, disponível para download no seguinte link:

[Download Docker Desktop](https://www.docker.com/products/docker-desktop)

Após instalar o docker será possível subir o ambiente utilizando a seguinte linha de comando na raíz desse repositorio (onde encontra-se o arquivo `docker-compose.yaml`):

<pre><code>docker-compose up</code></pre>

Ao executar o comando acima, pode ser requerido a permissão de acesso aos seguintes diretórios:

- ./datalake
- ./dags
- ./logs
- ./plugins

Se estiver utilizando Windows uma mensagem pop-up como a seguinte pode aparecer multiplas vezes na primeira execução:

![Docker Permission](/_img/docker_permission.png?raw=true "Docker Permission")

Caso ocorra, basta clicar na opção: **Share it**.

#### Apache Airflow

Ao executar o comando __docker-compose__ algumas mensagens de log aparecerão e o ambiente estará pronto para uso quando a saída do log estiver como na seguinte imagem:

![Docker Compose Logs](/_img/docker_compose_log.png?raw=true "Docker Compose Logs")

A interface visual do Apache Airflow estará acessível no seguinte endereço:

http://localhost:8080

E poderá ser acessada através das seguintes credenciais:

- login: **airflow**
- password: **airflow**

Uma dag de exemplo (sample.py) foi disponibilizada neste repositório para auxiliar no início do desenvolvimento do pipeline.

![Airflow Interface](/_img/airflow.png?raw=true "Airflow Interface")

#### Apache Spark
O Apache Spark (versão 3.1.1) está disponível na imagem docker junto com o Airflow, você poderá acessá-lo através do Spark Session no Python, exemplo:
<pre><code>
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("airflow_app") \
    .config('spark.executor.memory', '6g') \
    .config('spark.driver.memory', '6g') \
    .config("spark.driver.maxResultSize", "1048MB") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()
</code></pre>

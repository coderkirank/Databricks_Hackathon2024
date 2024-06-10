# Databricks notebook source
# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch databricks-genai-inference
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "workspace"
DB='adverse_drug_reaction'
SOURCE_TABLE_NAME = "adverse_effect_listing_vector_result"
SOURCE_TABLE_FULLNAME=f"{CATALOG}.{DB}.{SOURCE_TABLE_NAME}"

# COMMAND ----------

# Set up schema/volume/table
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DB}")
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {SOURCE_TABLE_FULLNAME} (
        id STRING,
        text STRING,
        date DATE,
        title STRING
    )
    USING delta 
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
"""
)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

# COMMAND ----------

VS_ENDPOINT_NAME = 'vs_endpoint'

if vsc.list_endpoints().get('endpoints') == None or not VS_ENDPOINT_NAME in [endpoint.get('name') for endpoint in vsc.list_endpoints().get('endpoints')]:
    print(f"Creating new Vector Search endpoint named {VS_ENDPOINT_NAME}")
    vsc.create_endpoint(VS_ENDPOINT_NAME)
else:
    print(f"Endpoint {VS_ENDPOINT_NAME} already exists.")

vsc.wait_for_endpoint(VS_ENDPOINT_NAME, 600)

# COMMAND ----------

VS_INDEX_NAME = 'fm_api_examples_vs_index'
VS_INDEX_FULLNAME = f"{CATALOG}.{DB}.{VS_INDEX_NAME}"

if not VS_INDEX_FULLNAME in [index.get("name") for index in vsc.list_indexes(VS_ENDPOINT_NAME).get('vector_indexes', [])]:
    try:
        # set up an index with managed embeddings
        print("Creating Vector Index...")
        i = vsc.create_delta_sync_index_and_wait(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_FULLNAME,
            source_table_name=SOURCE_TABLE_FULLNAME,
            pipeline_type="TRIGGERED",
            primary_key="id",
            embedding_source_column="text",
            embedding_model_endpoint_name="databricks-bge-large-en"
        )
    except Exception as e:
        if "INTERNAL_ERROR" in str(e):
            # Check if the index exists after the error occurred
            if VS_INDEX_FULLNAME in [index.get("name") for index in vsc.list_indexes(VS_ENDPOINT_NAME).get('vector_indexes', [])]:
                print(f"Index {VS_INDEX_FULLNAME} has been created.")
            else:
                raise e
        else:
            raise e
else:
    print(f"Index {VS_INDEX_FULLNAME} already exists.")

# COMMAND ----------

# Some example texts
from datetime import datetime


smarter_overview = {"text":"""
S.M.A.R.T.E.R. Initiative: Strategic Management for Achieving Results through Efficiency and Resources
Introduction
The S.M.A.R.T.E.R. Initiative, standing for "Strategic Management for Achieving Results through Efficiency and Resources," is a groundbreaking project aimed at revolutionizing the way our organization operates. In today's rapidly changing business landscape, achieving success demands a strategic approach that leverages resources effectively while optimizing efficiency. The S.M.A.R.T.E.R. Initiative is designed to do just that.

Background
As markets evolve and competition intensifies, organizations must adapt to stay relevant and profitable. Traditional methods of operation often become inefficient and costly. The S.M.A.R.T.E.R. Initiative was conceived as a response to this challenge, with the primary goal of enhancing strategic management practices to achieve better results.

Objectives
1. Resource Optimization
One of the key objectives of the S.M.A.R.T.E.R. Initiative is to optimize resource allocation. This involves identifying underutilized resources, streamlining processes, and reallocating resources to areas that contribute most to our strategic goals.

2. Efficiency Improvement
Efficiency is at the core of the S.M.A.R.T.E.R. Initiative. By identifying bottlenecks and improving processes, we aim to reduce operational costs, shorten project timelines, and enhance overall productivity.

3. Strategic Alignment
For any organization to succeed, its activities must be aligned with its strategic objectives. The S.M.A.R.T.E.R. Initiative will ensure that every action and resource allocation is in sync with our long-term strategic goals.

4. Results-driven Approach
The ultimate measure of success is results. The S.M.A.R.T.E.R. Initiative will foster a results-driven culture within our organization, where decisions and actions are guided by their impact on our bottom line and strategic objectives.

Key Components
The S.M.A.R.T.E.R. Initiative comprises several key components:

1. Data Analytics and Insights
Data is the foundation of informed decision-making. We will invest in advanced data analytics tools to gain insights into our operations, customer behavior, and market trends. These insights will guide our resource allocation and strategy.

2. Process Automation
Automation will play a vital role in enhancing efficiency. Routine and repetitive tasks will be automated, freeing up our workforce to focus on more strategic activities.

3. Performance Metrics and KPIs
To ensure that our efforts are aligned with our objectives, we will establish a comprehensive set of Key Performance Indicators (KPIs). Regular monitoring and reporting will provide visibility into our progress.

4. Training and Development
Enhancing our workforce's skills is essential. We will invest in training and development programs to equip our employees with the knowledge and tools needed to excel in their roles.

Implementation Timeline
The S.M.A.R.T.E.R. Initiative will be implemented in phases over the next three years. This phased approach allows for a smooth transition and ensures that each component is integrated effectively into our operations.

Conclusion
The S.M.A.R.T.E.R. Initiative represents a significant step forward for our organization. By strategically managing our resources and optimizing efficiency, we are positioning ourselves for sustained success in a competitive marketplace. This initiative is a testament to our commitment to excellence and our dedication to achieving exceptional results.

As we embark on this journey, we look forward to the transformative impact that the S.M.A.R.T.E.R. Initiative will have on our organization and the benefits it will bring to our employees, customers, and stakeholders.
""", "title": "Project Kickoff", "date": datetime.strptime("2024-01-16", "%Y-%m-%d")}

smarter_kpis = {"text": """S.M.A.R.T.E.R. Initiative: Key Performance Indicators (KPIs)
Introduction
The S.M.A.R.T.E.R. Initiative (Strategic Management for Achieving Results through Efficiency and Resources) is designed to drive excellence within our organization. To measure the success and effectiveness of this initiative, we have established three concrete and measurable Key Performance Indicators (KPIs). This document outlines these KPIs and their associated targets.

Key Performance Indicators (KPIs)
1. Resource Utilization Efficiency (RUE)
Objective: To optimize resource utilization for cost-efficiency.

KPI Definition: RUE will be calculated as (Actual Resource Utilization / Planned Resource Utilization) * 100%.

Target: Achieve a 15% increase in RUE within the first year.

2. Time-to-Decision Reduction (TDR)
Objective: To streamline operational processes and reduce decision-making time.

KPI Definition: TDR will be calculated as (Pre-Initiative Decision Time - Post-Initiative Decision Time) / Pre-Initiative Decision Time.

Target: Achieve a 20% reduction in TDR for critical business decisions.

3. Strategic Goals Achievement (SGA)
Objective: To ensure that organizational activities align with strategic goals.

KPI Definition: SGA will measure the percentage of predefined strategic objectives achieved.

Target: Achieve an 80% Strategic Goals Achievement rate within two years.

Conclusion
These three KPIs, Resource Utilization Efficiency (RUE), Time-to-Decision Reduction (TDR), and Strategic Goals Achievement (SGA), will serve as crucial metrics for evaluating the success of the S.M.A.R.T.E.R. Initiative. By tracking these KPIs and working towards their targets, we aim to drive efficiency, optimize resource utilization, and align our actions with our strategic objectives. This focus on measurable outcomes will guide our efforts towards achieving excellence within our organization.""",
"title": "Project KPIs", "date": datetime.strptime("2024-01-16", "%Y-%m-%d")}

# COMMAND ----------

import re

def chunk_text(text, chunk_size, overlap):
    words = text.split()
    chunks = []
    index = 0

    while index < len(words):
        end = index + chunk_size
        while end < len(words) and not re.match(r'.*[.!?]\s*$', words[end]):
            end += 1
        chunk = ' '.join(words[index:end+1])
        chunks.append(chunk)
        index += chunk_size - overlap

    return chunks

chunks = []

adverse_effect = {"text": """Common adverse effects of Insulin are SWEATING,LIPODYSTROPHY ACQUIRED,PALPITATION,INJECTION SITE INFLAMMATION,HYPERSENSITIVITY,GASTROESOPHAGEAL REFLUX DISEASE,CHEST TIGHTNESS,LIPOATROPHY,MYALGIA,INJECTION SITE REACTION,ERUPTIONS,FAST PULSE,REFRACTION DISORDERS,GASTRITIS,TIREDNESS,SWELLING  AT THE INJECTION SITE,ANAPHYLACTIC REACTIONS,ANGIONEUROTIC OEDEMA,PERIPHERAL NEUROPATHY,PAIN AT THE INJECTION SITE,INJECTION SITE REACTIONS,ABDOMINAL PAIN,VISUAL IMPAIRMENT,SODIUM RETENTION,BRONCHOSPASM,LIPODYSTROPHY AT THE INJECTION SITE,PROLIFERATIVE RETINOPATHY,ITCHING AT THE INJECTION SITE,ANGIOOEDEMA,INJECTION SITE WARMTH,DYSGEUSIA,INJECTION SITE ERYTHEMA,PANCREATITIS,INJECTION SITE ITCHING,ANTI-INSULIN ANTIBODIES,INJECTION SITE PAIN,INJECTION SITE HAEMATOMA,LIPODYSTROPHY,PERIPHERAL NEUROPATHY (PAINFUL NEUROPATHY),DIABETIC KETOACIDOSIS,DIARRHOEA,REDNESS AT THE INJECTION SITE,REDNESS  AT THE INJECTION SITE,NAUSEA,OEDEMA,GENERALISED HYPERSENSITIVITY REACTIONS,ALLERGIC REACTIONS,HYPERGLYCAEMIA,LOCAL HYPERSENSITIVITY REACTIONS,VOMITING,GENERALIZED ALLERGY,ANAPHYLACTIC REACTION,ABDOMINAL DISTENSION,INJECTION SITE URTICARIA,SWELLING OF TONGUE,SKIN REACTIONS,RASH,ALLERGIC DERMATITIS,DIFFICULTY IN BREATHING,URTICARIA,INJECTION SITE HAEMMORRHAGE,ALLERGIC REACTION,DYSPEPSIA,DIFFICULTIES IN BREATHING,ERUCTATION,RETINOPATHY,SHORTNESS OF BREATH,INJECTION SITE HAEMORRHAGE,PRURITUS,BRUISING AT THE INJECTION SITE,CONSTIPATION,DECREASED APPETITE,HYPOGLYCAEMIA,NECROTISING PANCREATITIS,DEHYDRATION,LIPOHYPERTROPHY,FLATULENCE,SYSTEMIC HYPERSENSITIVITY REACTIONS,INJECTION SITE REDNESS,INJECTION SITE HIVES,REDUCTION IN BLOOD PRESSURE,GASTROINTESTINAL UPSET,WHEEZING,INJECTION SITE MASS,INJECTION SITE NODULES,INCREASED HEART RATE,DYSPNOEA,IMMEDIATE TYPE ALLERGIC REACTIONS,INJECTION SITE BRUISING,INJECTION SITE SWELLING,GENERALISED SKIN REACTIONS,INJECTION SITE PRURITUS,SHOCK,GENERALISED SKIN RASH,SWELLING AT THE INJECTION SITE,PERIPHERAL OEDEMA,HYPOTENSION,SWELLING OF LIPS,DIABETIC RETINOPATHY,INJECTION SITE DISCOLOURATION,PAIN AT THE INJECTION SITE ,INFLAMMATION AT THE INJECTION SITE,HIVES AT THE INJECTION SITE,POTENTIALLY ALLERGIC REACTIONS,ITCHING""",
"title": "Drug_Adverse_Reaction", "date": datetime.strptime("2024-01-16", "%Y-%m-%d")}
#documents = [smarter_overview, smarter_kpis]
documents = [adverse_effect]
for document in documents:
    for i, c in enumerate(chunk_text(document["text"], 150, 25)):
        chunk = {}
        chunk["text"] = c
        chunk["title"] = document["title"]
        chunk["date"] = document["date"]
        chunk["id"] = document["title"] + "_" + str(i)

        chunks.append(chunk)


# COMMAND ----------

smarter_kpis

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, DateType

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("title", StringType(), True),
        StructField("date", DateType(), True),
    ]
)

if chunks:
    result_df = spark.createDataFrame(chunks, schema=schema)
    result_df.write.format("delta").mode("append").saveAsTable(
        SOURCE_TABLE_FULLNAME
    )

# COMMAND ----------

VS_INDEX_NAME = 'fm_api_adverse_effect_vs'
VS_INDEX_FULLNAME = f"{CATALOG}.{DB}.{VS_INDEX_NAME}"

index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME,
                      index_name=VS_INDEX_FULLNAME)
index.sync()

# COMMAND ----------

# query
index.similarity_search(columns=["text", "title"],
                        query_text="What are common adverse effects of insulin?",
                        num_results = 3)

# COMMAND ----------

from databricks_genai_inference import ChatSession

chat = ChatSession(model="databricks-meta-llama-3-70b-instruct",
                   system_message="You are a helpful assistant.",
                   max_tokens=128)

# COMMAND ----------

chat.reply("What are common adverse effects of insulin?")
chat.last

# COMMAND ----------

# reset history
chat = ChatSession(model="databricks-meta-llama-3-70b-instruct",
                   system_message="You are a helpful assistant. Answer the user's question based on the provided context.",
                   max_tokens=128)

# get context from vector search
raw_context = index.similarity_search(columns=["text", "title"],
                        query_text="What are common adverse effects of insulin?",
                        num_results = 3)

context_string = "Context:\n\n"

for (i,doc) in enumerate(raw_context.get('result').get('data_array')):
    context_string += f"Retrieved context {i+1}:\n"
    context_string += doc[0]
    context_string += "\n\n"

chat.reply(f"User question: What are common adverse effects of insulin?\n\nContext: {context_string}")
chat.last

# COMMAND ----------



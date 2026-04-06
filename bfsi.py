# %%
import psycopg2 as psy
import pandas as pd
import re
from openai import OpenAI


# %%
# ---------------------------------------------------------
# read the connection file and connect to postgres database
# ---------------------------------------------------------
def ConnectDB(file):
    try:
        ret = []
        data = ''
        fp = open(file, "r")

        while True:
            next_line = fp.readline()
            if not next_line:
                break
            data += next_line.strip()
        fp.close()

        if len(data) <= 0:
            ret.extend(["ERROR", "Unable to read the Input data properly. Filename:" + file])
        else:
            parameters = data.split(";")
            keys = [];
            values = []

            for p in parameters:
                if len(p) > 0:
                    sp = p.split(":")
                    keys.append(sp[0])
                    values.append(sp[1])
            conn_values = dict(zip(keys, values))

            # Establish connection to PostgreSQL
            conn = psy.connect(dbname=conn_values['dbname'], user=conn_values['user'],
                               password=conn_values['password'],
                               host=conn_values['host'], port=conn_values['port'])

            cursor = conn.cursor()

            ret.extend(["SUCCESS", conn, cursor])

    except Exception as e:
        ret.extend(["EXCEPTION", "ConnetDB(). " + str(e)])

    return (ret)


# %%
# =========================================================
# generic function to execute any query and return DataFrame
# =========================================================
def executeQuery(query):
    try:
        data = ''

        cursor.execute(query)
        data = cursor.fetchall()

        cols = [c[0] for c in cursor.description]
        data = pd.DataFrame(data, columns=cols)

    except Exception as e:
        data = "Exception." + str(e)

    return (data)


# %%
# =========================================================
# SearchData: unified search function for all retrieval types
# qtype: 'lex' = lexical, 'meta' = metadata, 'emb' = embedding, 'reg' = regular SQL
# =========================================================
def SearchData(cursor, cond, qtype, limit=5):
    try:
        qtype = qtype.lower().strip()

        if qtype not in ['meta', 'lex', 'emb', 'reg']:
            data = "Invalid Query Type. Valid values are 'lex','meta','emb','reg'"
        else:
            if qtype == "lex":
                query = f'''
                    select * from bfsi_data where case_id in (
                    select case_id from bfsi where content_tsv @@ websearch_to_tsquery('english',
                    '{cond}'))
                    limit {limit} ; '''
            elif qtype == "meta":
                query = f'''
                        select * from bfsi_data where case_id in (
                            select case_id from bfsi where metadata @> '{cond}') limit {limit} ; '''
            elif qtype == "emb":
                response = client.embeddings.create(model='text-embedding-3-small', input=cond)
                txt_embed = response.data[0].embedding
                query = f''' select * from bfsi_data where case_id in (
                            select case_id from bfsi order by embedding <=> '{txt_embed}'::vector
                            limit {limit} ) '''
            else:
                query = cond

            query = re.sub('[\\n]', ' ', query).strip()
            # print(query)

            cursor.execute(query)
            data = cursor.fetchall()

            cols = [c[0] for c in cursor.description]
            data = pd.DataFrame(data, columns=cols)

    except Exception as e:
        data = str(e)
        data = re.sub("\\n", " ", data).strip()
        data = ' '.join(data.split())

    return (data)


# %%
# -----------------------------------------------------------------------------------------------
# Connect to the pgVector database using the credentials
# -----------------------------------------------------------------------------------------------

# import os
# print(os.getcwd())

ret = ConnectDB("pgvector.txt")
if ret[0] == "SUCCESS":
    conn = ret[1]
    cursor = ret[2]
    print("ConnectDB Successfully!")
else:
    print("Error / Exception during ConnectDB")

# print(conn)
# print(cursor)

# %%
# to create the embeddings, we will use OpenAI embeddings (size 1536)
# apikey = "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"
client = OpenAI()

# %%
# =========================================================
# check the data in both tables
# =========================================================

data = executeQuery("select count(1) from bfsi_data")
print("bfsi_data count:", data)

data = executeQuery("select count(1) from bfsi")
print("bfsi count:", data)

data = executeQuery("select * from bfsi_data limit 3")
print(data)

data = executeQuery("select case_id, content from bfsi limit 3")
print(data)

# %%
# =========================================================
# Generate embeddings for each case and update bfsi
# =========================================================

# get all the content from bfsi
data = executeQuery("select case_id, content from bfsi where embedding IS NULL")
print(f"Records to embed: {len(data)}")

# Convert every Content into an embedding; and update table against each case ID
for i in range(len(data)):
    caseid, content = data.loc[i, "case_id"], data.loc[i, "content"]
    # print(caseid)

    response = client.embeddings.create(model="text-embedding-3-small", input=content)
    embedding = response.data[0].embedding  # get the embeddings

    # Convert to PostgreSQL vector format
    embedding_str = "[" + ",".join(map(str, embedding)) + "]"

    # Update database
    cursor.execute("UPDATE bfsi SET embedding = %s WHERE case_id = %s;", (embedding_str, caseid))

    # print progress every 100 records
    if (i + 1) % 100 == 0:
        print(f"  Embedded {i + 1}/{len(data)} records...")

conn.commit()
print("Embeddings generated and stored successfully.")

# %%
# verify embeddings are stored
data = executeQuery("select case_id, embedding from bfsi where embedding is null")
print(f"Records without embedding: {len(data)}")

# %%
# =========================================================
# TEST: Embedding search
# =========================================================

# write a prompt to fetch data from the table using embedding search
prompt = "borrower asked for release of funds but insurance document is pending"
response = client.embeddings.create(model='text-embedding-3-small', input=prompt)
print(response)
# extract only the embeddings
prompt_embed = response.data[0].embedding

# form the query to run the embedding search
qry = f''' select bd.*
        from bfsi bc
        join bfsi_data bd
        on bd.case_id = bc.case_id
        order by bc.embedding <=> '{prompt_embed}'::vector
        limit 5;
        '''
print(qry)

# execute the query
data = executeQuery(qry)
print(data)

# %%

# check the results
data[['case_id', 'product_type', 'workflow_stage', 'case_summary']]

# %%
# #########################################################################
#
# EXECUTE ALL 14 SAMPLE PROMPTS
#
# #########################################################################

print("\n" + "#" * 70)
print("  RUNNING ALL 14 SAMPLE PROMPTS")
print("#" * 70)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 1: Find cases mentioning 'collateral valuation gap' or
#           'manual review' in the summary and show policy references.
# Method: LEXICAL
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 1: Lexical search — collateral valuation gap / manual review")
data = SearchData(cursor, "collateral valuation gap | manual review", "lex", limit=10)
print(data[['case_id', 'policy_reference', 'case_summary']])

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 2: List all cases where notes mention 'duplicate request'
#           and resolution was 'Closed - Duplicate'.
# Method: LEXICAL + SQL (hybrid)
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 2: Lexical + SQL — duplicate request / Closed - Duplicate")
qry = '''
      select bd.case_id, bd.resolution_code, bd.resolution_notes, bd.case_summary
      from bfsi_data bd
               join bfsi bc on bd.case_id = bc.case_id
      where bc.content_tsv @@ websearch_to_tsquery('english' \
          , 'duplicate request')
        and bd.resolution_code = 'Closed - Duplicate'
          limit 10; \
      '''
data = SearchData(cursor, qry, "reg")
print(data)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 3: Show only high-priority Home Loan cases from West region
#           that are still open or pending documents.
# Method: METADATA
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 3: Metadata — high priority Home Loan / West / Open or Pending")
qry = '''
      select bd.*
      from bfsi_data bd
               join bfsi bc on bd.case_id = bc.case_id
      where bc.metadata @> '{"priority":"High","product_type":"Home Loan","region":"West"}'
        and bd.case_status in ('Open', 'Pending Documents') limit 10; \
      '''
data = SearchData(cursor, qry, "reg")
print(data[['case_id', 'priority', 'product_type', 'region', 'case_status', 'case_summary']])

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 4: Retrieve escalated cases handled by Disbursement Control
#           with SLA breach = Yes.
# Method: METADATA
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 4: Metadata — Escalated / Disbursement Control / SLA breach")
data = SearchData(cursor,
                  '{"case_status":"Escalated","business_unit":"Disbursement Control","sla_breach_flag":"Yes"}',
                  "meta", limit=10)
print(data[['case_id', 'business_unit', 'case_status', 'sla_breach_flag', 'case_summary']])

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 5: Find cases semantically similar to: 'borrower asked for
#           release of funds but insurance document is pending'.
# Method: EMBEDDING
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 5: Embedding search — funds release / insurance pending")
data = SearchData(cursor,
                  "borrower asked for release of funds but insurance document is pending",
                  "emb", limit=5)
print(data[['case_id', 'product_type', 'workflow_stage', 'case_summary']])

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 6: Surface cases similar to 'customer disputes EMI amount
#           after rate revision and requests servicing support'.
# Method: EMBEDDING
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 6: Embedding search — EMI dispute / rate revision / servicing")
data = SearchData(cursor,
                  "customer disputes EMI amount after rate revision and requests servicing support",
                  "emb", limit=5)
print(data[['case_id', 'root_cause', 'process_name', 'case_summary']])

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 7: Average actual TAT by process_name and risk_band for Q3 2025.
# Method: SQL (regular)
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 7: SQL — avg TAT by process_name and risk_band for Q3 2025")
qry = '''
      SELECT process_name, \
             risk_band,
             ROUND(AVG(actual_tat_hours)::numeric, 2) AS avg_tat_hours,
             COUNT(*)                                 AS case_count
      FROM bfsi_data
      WHERE created_date >= '2025-07-01' \
        AND created_date <= '2025-09-30'
      GROUP BY process_name, risk_band
      ORDER BY process_name, avg_tat_hours DESC \
      '''
data = SearchData(cursor, qry, "reg", limit=50)
print(data)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 8: Count closed cases by product_type and region where CSAT < 3.0.
# Method: SQL (regular)
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 8: SQL — closed cases by product & region where CSAT < 3.0")
qry = '''
      SELECT product_type, \
             region,
             COUNT(*) AS low_csat_closed_count
      FROM bfsi_data
      WHERE case_status = 'Closed' \
        AND csat_score < 3.0
      GROUP BY product_type, region
      ORDER BY low_csat_closed_count DESC \
      '''
data = SearchData(cursor, qry, "reg", limit=50)
print(data)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 9: Top 10 highest transaction_amount_inr with exception_flag = Yes.
# Method: SQL (regular)
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 9: SQL — top 10 highest txn amount with exception flag")
qry = '''
      SELECT case_id, \
             product_type, \
             region, \
             transaction_amount_inr,
             exception_flag, \
             risk_band, \
             case_summary
      FROM bfsi_data
      WHERE exception_flag = 'Yes'
      ORDER BY transaction_amount_inr DESC LIMIT 10 \
      '''
data = SearchData(cursor, qry, "reg")
print(data[['case_id', 'transaction_amount_inr', 'product_type', 'region']])

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 10: Find severe-risk fraud review cases in North, then rank
#            by semantic similarity to 'suspicious duplicate documentation'.
# Method: METADATA + EMBEDDING (multi-step)
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 10: Metadata (Severe/Fraud Review/North) + Embedding ranking")

# step 1: get the embedding for the semantic query
prompt_10 = "suspicious duplicate documentation"
response = client.embeddings.create(model='text-embedding-3-small', input=prompt_10)
embed_10 = response.data[0].embedding

# step 2: combine metadata filter + embedding sort
qry = f'''
    select bd.case_id, bd.risk_band, bd.process_name, bd.region, bd.case_summary,
           bc.embedding <=> '{embed_10}'::vector as distance
    from bfsi_data bd
    join bfsi bc on bd.case_id = bc.case_id
    where bc.metadata @> '{{"risk_band":"Severe","process_name":"Fraud Review","region":"North"}}'
    order by bc.embedding <=> '{embed_10}'::vector
    limit 10;
'''
data = executeQuery(qry)
print(data)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 11: Complaints about sanction delays for SME Working Capital
#            where actual_tat_hours > sla_hours.
# Method: SQL + EMBEDDING (multi-step)
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 11: SQL (SME + TAT > SLA) + Embedding for 'sanction delays'")

# step 1: get embedding
prompt_11 = "complaints about sanction delays processing time"
response = client.embeddings.create(model='text-embedding-3-small', input=prompt_11)
embed_11 = response.data[0].embedding

# step 2: SQL filter + embedding sort
qry = f'''
    select bd.case_id, bd.product_type, bd.actual_tat_hours, bd.sla_hours, bd.case_summary,
           bc.embedding <=> '{embed_11}'::vector as distance
    from bfsi_data bd
    join bfsi bc on bd.case_id = bc.case_id
    where bd.product_type = 'SME Working Capital'
    and bd.actual_tat_hours > bd.sla_hours
    order by bc.embedding <=> '{embed_11}'::vector
    limit 10;
'''
data = executeQuery(qry)
print(data)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 12: For KYC verification cases, retrieve records similar to
#            'name mismatch in income proof' and return most common
#            root causes.
# Method: METADATA + EMBEDDING + aggregation
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 12: Metadata (KYC) + Embedding for 'name mismatch' + root cause agg")

# step 1: get embedding
prompt_12 = "name mismatch in income proof"
response = client.embeddings.create(model='text-embedding-3-small', input=prompt_12)
embed_12 = response.data[0].embedding

# step 2: metadata filter + embedding sort
qry = f'''
    select bd.case_id, bd.root_cause, bd.case_summary,
           bc.embedding <=> '{embed_12}'::vector as distance
    from bfsi_data bd
    join bfsi bc on bd.case_id = bc.case_id
    where bc.metadata @> '{{"process_name":"KYC Verification"}}'
    order by bc.embedding <=> '{embed_12}'::vector
    limit 20;
'''
data = executeQuery(qry)
print(data[['case_id', 'root_cause', 'distance']].head(10))

# step 3: aggregate root causes
print("\nMost common root causes in KYC 'name mismatch' results:")
root_cause_counts = data['root_cause'].value_counts()
print(root_cause_counts)

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 13: Policy references most frequently associated with escalated
#            cases that mention AML alerts in summary or notes.
# Method: METADATA + LEXICAL + aggregation
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 13: Metadata (Escalated) + Lexical 'AML alerts' + policy agg")
qry = '''
      select bd.case_id, \
             bd.policy_reference, \
             bd.case_summary,
             ts_rank(bc.content_tsv, websearch_to_tsquery('english', 'AML alerts anti money laundering')) as rank
      from bfsi_data bd
               join bfsi bc on bd.case_id = bc.case_id
      where bc.metadata @> '{"case_status":"Escalated"}'
        and bc.content_tsv @@ websearch_to_tsquery('english' \
          , 'AML alerts anti money laundering')
      order by rank desc
          limit 20; \
      '''
data = executeQuery(qry)
print(data[['case_id', 'policy_reference', 'rank']].head(10))

# aggregate policy references
print("\nMost frequent policy references in escalated AML-related cases:")
if len(data) > 0:
    policy_counts = data['policy_reference'].value_counts()
    print(policy_counts)
else:
    print("No matching cases found. AML may not be in the dataset text.")

# %%
# ─────────────────────────────────────────────────────────────────────────
# PROMPT 14: Cases tagged with 'Disbursement Hold' where summary is
#            semantically close to 'release funds blocked due to
#            missing document', grouped by channel.
# Method: METADATA + EMBEDDING + GROUP BY
# ─────────────────────────────────────────────────────────────────────────

print("\n>>> PROMPT 14: Metadata (Disbursement Hold) + Embedding + group by channel")

# step 1: get embedding
prompt_14 = "release funds blocked due to missing document"
response = client.embeddings.create(model='text-embedding-3-small', input=prompt_14)
embed_14 = response.data[0].embedding

# step 2: metadata filter + embedding sort
qry = f'''
    select bd.case_id, bd.channel, bd.case_summary,
           bc.embedding <=> '{embed_14}'::vector as distance
    from bfsi_data bd
    join bfsi bc on bd.case_id = bc.case_id
    where bc.metadata @> '{{"knowledge_article_tag":"Disbursement Hold"}}'
    order by bc.embedding <=> '{embed_14}'::vector
    limit 20;
'''
data = executeQuery(qry)
print(data[['case_id', 'channel', 'distance']].head(10))

# step 3: group by channel
print("\nResults grouped by channel:")
if len(data) > 0:
    channel_groups = data.groupby('channel').agg(
        case_count=('case_id', 'count'),
        avg_distance=('distance', 'mean'),
        case_ids=('case_id', lambda x: ', '.join(x.head(3)))
    ).reset_index()
    print(channel_groups)
else:
    print("No matching cases found.")

# ─────────────────────────────────────────────────────────────────────────
print("\n" + "#" * 70)
print("  ALL 14 SAMPLE PROMPTS EXECUTED SUCCESSFULLY")
print("#" * 70)

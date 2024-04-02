import requests
import boto3
import json
import pandas as pd
import streamlit as st
from datetime import datetime, date
import awswrangler as wr


st.set_page_config(page_title="MKTX-Datalake-Table-Timestamp-Checker", page_icon=":tada", layout="wide")

#Headers
with st.container():
    st.header("Check the status of your tables in the Datalake")
    #st.title("Ask Questions about your ECS and ObjectScale system")

with st.container():
    st.write("---")
    userQuery = st.text_input("What data do you want to check for?")
    #userID = st.text_input("User ID")
    st.write("---")

bedrock = boto3.client('bedrock-runtime' , 'us-east-1')


def parse_xml(xml, tag):
    temp=xml.split(">")
    
    tag_to_extract="</"+tag

    for line in temp:
        if tag_to_extract in line:
            parsed_value=line.replace(tag_to_extract, "")
            return parsed_value

def get_tables(bedrock, user_input):

# Uses the Bedrock Client, the user input, and the document template as part of the prompt

    ##Setup Prompt
    system_prompt = f"""

Human:

You are a Data Analyst assistant
Based on the the user's provided input, determine which database and tables will contain the data the user is asking about
The Database and table pairs should be one of the provided tables. Use the description of the database and table's provided to assist in your selection.
Return the relevant database:table pairs of the user's input in valid json using the format provided as a guide
If more than one database:table pair is relevant, include all relevant pairs in a valid json array



Here are the database and table pairs you should select from (in the form of (Database):(Table) - (description))
<database_tables>
Moma: artists - Contains information about ARTISTS that are currently or have formerly been featured in the "museum of modern arts"
Moma: artworks - Contains information about ARTWORKS that are currently or have formerly been featured in the "museum of modern arts"
N/A: N/A
</database_tables>

<output_format>
{{"database": (Database),"table": (Table)}}
</output_format>

think through each step in the process and write your thoughts in <thinking> xml tags
return the relevant database:table pairs in valid json in <json> xml tags

"""

    prompt = {
        "anthropic_version":"bedrock-2023-05-31",
        "max_tokens":1000,
        "temperature":0.5,
        "system" : system_prompt,
        "messages":[
            {
                "role":"user",
                "content":[
                {
                    "type":"text",
                    "text": "<user_input>" +user_input +"</user_input>"
                }
                ]
            }
        ]
    }

    json_prompt = json.dumps(prompt)

    response = bedrock.invoke_model(body=json_prompt, modelId="anthropic.claude-3-sonnet-20240229-v1:0", accept="application/json", contentType="application/json")

    #modelId = "anthropic.claude-v2"  # change this to use a different version from the model provider if you want to switch 
    #accept = "application/json"
    #contentType = "application/json"
    #Call the Bedrock API
    #response = bedrock.invoke_model(
    #    body=body, modelId=modelId, accept=accept, contentType=contentType
    #)

    #Parse the Response
    response_body = json.loads(response.get('body').read())

    llmOutput=response_body['content'][0]['text']

    thinking = parse_xml(llmOutput, "thinking")
    llm_json = parse_xml(llmOutput, "json")

    #Return the LLM response
    return llm_json, thinking

#method to retreive database table schema in glue. Inputs should be database and table. Outputs should be the schema
def get_table_schema(database, table):
    glue_client = boto3.client('glue')
    response = glue_client.get_table(DatabaseName=database, Name=table)
    temp= response['Table']['StorageDescriptor']['Columns']
    print(temp)

#Adjust this to whatever the timestamp query they want
def question_to_sql(bedrock, user_input, database_name, table_name, table_schema):

# Uses the Bedrock Client, the user input, and the document template as part of the prompt

    ##Setup Prompt
    system_prompt = f"""

Human:

Generate a valid sql query based on the user's question
Use the provided table schemas to understand the table structure
Make sure to use single quotes (') around the string values
Data in the table is case sensitive and your sql query should reflect that
Do not include any other text other than the sql query in the response

Table schema for the table named '{table_name}':
<artists_table_schema>
{table_schema}
</artists_table_schema>

think through each step in the process and write your thoughts in <thinking> xml tags
return the valid sql query in <sql> xml tags

"""

    prompt = {
        "anthropic_version":"bedrock-2023-05-31",
        "max_tokens":1000,
        "temperature":0.5,
        "system" : system_prompt,
        "messages":[
            {
                "role":"user",
                "content":[
                {
                    "type":"text",
                    "text": "<user_question>" +user_input +"</user_question>"
                }
                ]
            }
        ]
    }

    json_prompt = json.dumps(prompt)

    response = bedrock.invoke_model(body=json_prompt, modelId="anthropic.claude-3-sonnet-20240229-v1:0", accept="application/json", contentType="application/json")

    #Parse the Response
    response_body = json.loads(response.get('body').read())

    llmOutput=response_body['content'][0]['text']

    thinking = parse_xml(llmOutput, "thinking")
    sql = parse_xml(llmOutput, "sql")

    #Return the LLM response
    return sql


userQuery = "Pablo Picasso"

table_results = get_tables(bedrock, userQuery)

tables_raw = table_results[0]
thoughts = table_results[1]


print(thoughts)
print("---------------------------")
print(tables_raw)

try:
    tables_json = json.loads(tables_raw)
    print(tables_json)
except:
    tables_json = []
    print("something went wrong")



print("---------------------------")
print(tables_json)


for item in tables_json:
    glue_database =item['database']
    glue_table=item['table']
    glue_schema = get_table_schema(glue_database, glue_table)
    #call llm to generate sql query to get timestamp (apply partition logic here, or in prompt?)

    ###
    

    #Call the sql query
    #query_results = wr.athena.read_sql_query(sql_query, database=glue_database, ctas_approach=False)
    
#validate time stamp
    #If validation fails -> return notification to dev (Glue being the potential)
#What is the expected timestamp
#compare timestamp with expected timestamp 
    #Timestamp == expected timestamp -> healthy
    #timestamp != expected timestamp
        #Validate S3 timestamp
            #If == ?
            #Else ?!

    

#If table is partitioned by time
        #Do x
    #take last weeks data
        #What is the highest value of this column in the past two weeks
#If table is not partitioned by time
        #Do Y
    #take last weeks data#What is the highest column value for the whole table
    #return time
        #this tbales latest update was {timestamp}
    

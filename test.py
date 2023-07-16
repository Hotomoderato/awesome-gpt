from streamlit_chat import message
import os, tempfile, sys
from io import BytesIO
from io import StringIO
import pandas as pd
import tenacity
import openai
import streamlit as st
import tiktoken
import tenacity
from azure.identity import ClientSecretCredential
import configs.constants as const
import configs.deployments as dep
from openai import ChatCompletion

from langchain.agents import create_pandas_dataframe_agent
from langchain.llms.openai import OpenAI
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.chains.summarize import load_summarize_chain
from langchain.document_loaders.csv_loader import CSVLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.text_splitter import CharacterTextSplitter
from langchain.chains.mapreduce import MapReduceChain
from langchain.docstore.document import Document
from langchain.vectorstores import FAISS
from langchain.chat_models import AzureChatOpenAI
from langchain.chains import ConversationalRetrievalChain
from langchain.chains import RetrievalQA
from langchain.memory import ConversationBufferMemory
from langchain.chains.conversational_retrieval.prompts import CONDENSE_QUESTION_PROMPT
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts.prompt import PromptTemplate
from langchain import LLMChain

# authenticate to Azure
credentials = ClientSecretCredential(const.TENANT_ID, const.SERVICE_PRINCIPAL, const.SERVICE_PRINCIPAL_SECRET)
token = credentials.get_token(const.SCOPE_NON_INTERACTIVE)

# access openai account
openai.api_type = "azure_ad"
openai.api_key = token.token
openai.api_base = f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}"
openai.api_version = const.OPENAI_API_VERSION


df = pd.read_csv('/home/titanic.csv')
df        
llm = AzureChatOpenAI(
openai_api_base=f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}",
openai_api_version=const.OPENAI_API_VERSION,
openai_api_key=token.token,
deployment_name=dep.GPT_35_TURBO,
openai_api_type="azure_ad",
temperature=0)
# Create and run the CSV agent with the user's query
agent = create_pandas_dataframe_agent(llm, 
                                      df,
                                      verbose=True, 
                                      return_intermediate_steps=False,
                                      max_execution_time=60)
result = agent({"input": "draw the corr heatmap plot between age and sex"})
result

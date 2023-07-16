import openai
import streamlit as st
from azure.identity import ClientSecretCredential
import configs.constants as const
import configs.deployments as dep
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import StructuredOutputParser, ResponseSchema
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)

import configs.constants as const
import configs.deployments as dep

# authenticate to Azure
credentials = ClientSecretCredential(const.TENANT_ID, const.SERVICE_PRINCIPAL, const.SERVICE_PRINCIPAL_SECRET)
token = credentials.get_token(const.SCOPE_NON_INTERACTIVE)

# access openai account
openai.api_type = "azure_ad"
openai.api_key = token.token
openai.api_base = f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}"
openai.api_version = const.OPENAI_API_VERSION

def translate_results(source_language, target_language, code):
    system_template = """You are a helpful code translator that can convert the fellowing code from {source_language} to {target_language}, do not show {source_language} code in the answer """
    system_message_prompt = SystemMessagePromptTemplate.from_template(system_template)
    human_template = "{format_instructions}\n The following code needs to be translated from {source_language} to {target_language}:{code}\n"
    human_message_prompt = HumanMessagePromptTemplate.from_template(human_template)

    response_schemas = [
        ResponseSchema(name="code", description="the translated code with the target language"),
        ResponseSchema(name="explain", description="the explaination of the code converted")
    ]
    output_parser = StructuredOutputParser.from_response_schemas(response_schemas)
    format_instructions = output_parser.get_format_instructions()
    # run langchain
    llm = AzureChatOpenAI(
        openai_api_base=f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}",
        openai_api_version=const.OPENAI_API_VERSION,
        openai_api_key=token.token,
        deployment_name=dep.GPT_35_TURBO,
        openai_api_type="azure_ad",
        temperature=0)

    chat_prompt = ChatPromptTemplate(
        messages=[
            human_message_prompt,
            system_message_prompt,],
        input_variables=["code", "source_language", "target_language"],
        partial_variables={"format_instructions": format_instructions})
    text = chat_prompt.format_prompt(source_language=source_language, target_language=target_language, code=code)
    #st.write(text.to_messages())
    output = llm(text.to_messages())
    #st.write(output)
    #st.write(output_parser.parse(output.content))
    return output_parser.parse(output.content)


st.set_page_config(
    page_title="Code Translator",
    page_icon="💻"
)
st.header("Code Translator")
st.subheader("Translate code from one language to another")

language_list = ['python','R','Scala', 'PL/SQL', 'pyspark', 'Spark SQL', 'SQL', 'SAS']


with st.form(key='my_form'):
    source_language = st.selectbox('Source Language', options=language_list, index=language_list.index('python'))
    target_language = st.selectbox('Target Language', options=language_list, index=language_list.index('R'))
    code = st.text_area("Enter code to translate")
    submit_button = st.form_submit_button(label='Translate')


if submit_button:

    with st.spinner('Translating code...'):
        results = translate_results(source_language, target_language, code)
        if 'code' in results:
            st.code(results['code'], language=target_language)
        else:
            st.error("No code returned")

        if 'explain' in results:
            st.write(results['explain'])


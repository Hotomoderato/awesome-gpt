import pandas as pd
import tenacity
import openai
import streamlit as st
import tenacity
from azure.identity import ClientSecretCredential
import configs.constants as const
import configs.deployments as dep

from langchain.agents import create_pandas_dataframe_agent
from langchain.chat_models import AzureChatOpenAI


# authenticate to Azure
credentials = ClientSecretCredential(const.TENANT_ID, const.SERVICE_PRINCIPAL, const.SERVICE_PRINCIPAL_SECRET)
token = credentials.get_token(const.SCOPE_NON_INTERACTIVE)

# access openai account
openai.api_type = "azure_ad"
openai.api_key = token.token
openai.api_base = f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}"
openai.api_version = const.OPENAI_API_VERSION

st.set_page_config(page_title="CSV Agent", layout="wide")



def analyze(temperature):
    st.write("# Data Assistant")
    st.sidebar.markdown(
        """
        ### Steps:
        1. Upload PDF File
        2. Perform Q&A
    **Note : File content not stored in any form.**
        """)

    st.sidebar.markdown('''### Upload your CSV file:''')
    uploaded_file = st.sidebar.file_uploader(label='Upload:', type="csv")
    st.sidebar.markdown(
    '''
    ### Example:
    1. Upload
    the [titanic.csv](https://www.kaggle.com/datasets/vinicius150987/titanic3) as example
    2. Ask questions like:
    "How many people survived?"
    ''')
    # example data upload
    # add button to upload defult file

#    if st.sidebar.button("Use Default File", key="default_file"):
#        uploaded_file = '/home/cdsw/titanic.csv'
#        print(uploaded_file)

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        print(df)
        st.write("### Data Preview:")
        st.dataframe(df.head(), hide_index=True, use_container_width=True)
        
        @tenacity.retry(wait=tenacity.wait_fixed(2), stop=tenacity.stop_after_attempt(5))
        def agent_chat(query):
            # Create and run the CSV agent with the user's query
            agent = create_pandas_dataframe_agent(AzureChatOpenAI(
            openai_api_base=f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}",
            openai_api_version=const.OPENAI_API_VERSION,
            openai_api_key=token.token,
            deployment_name=dep.GPT_35_TURBO,
            openai_api_type="azure_ad",
            temperature=temperature), 
            df, 
            verbose=True, 
            return_intermediate_steps=True,
            max_execution_time=60)
            result = agent({"input": query})
#            except:
#                result = "Try asking quantitative questions about structure of csv data!"
            return result

        if 'assistant' not in st.session_state:
            st.session_state['assistant'] = ["Hello ! Ask me anything about Document 🤗"]

        if 'user' not in st.session_state:
            st.session_state['user'] = ["Hey ! 👋"]

        user_input = st.chat_input(placeholder="Use CSV agent for precise information about the structure of your csv file:", key='input')
    
        if user_input:
            output = agent_chat(user_input)
            st.session_state['user'].append(user_input)
            st.session_state['assistant'].append(output["output"])

        if st.session_state['assistant']:
            for i in range(len(st.session_state['assistant'])):
                st.chat_message("user").write(st.session_state["user"][i])
                st.chat_message("assistant").write(st.session_state["assistant"][i])
                    #message(st.session_state["user"][i], is_user=True, key=str(i) + '_user', avatar_style="big-smile")
                    #message(st.session_state["assistant"][i], key=str(i), avatar_style="thumbs")
                    
        if user_input:
            st.markdown("---")
            st.write("### Log:")
            st.write(output)
            st.markdown("---")

# Main App
def main():
    # Execute the home page function
    # MODEL_OPTIONS = ["gpt-3.5-turbo"] #, "gpt-4", "gpt-4-32k"]
    # max_tokens = {"gpt-3.5-turbo":3000}
    # TEMPERATURE_MIN_VALUE = 0.0
    # TEMPERATURE_MAX_VALUE = 1.0
    # TEMPERATURE_DEFAULT_VALUE = 0.0
    # TEMPERATURE_STEP = 0.1
    # temperature = st.sidebar.slider(
    #             label="Temperature",
    #             min_value=TEMPERATURE_MIN_VALUE,
    #             max_value=TEMPERATURE_MAX_VALUE,
    #             value=TEMPERATURE_DEFAULT_VALUE,
    #             step=TEMPERATURE_STEP,)
    analyze(temperature=0)

if __name__ == "__main__":
    main()
from langchain.agents import initialize_agent, AgentType
from langchain.callbacks import StreamlitCallbackHandler
from langchain.tools import DuckDuckGoSearchRun
import streamlit as st
from azure.identity import ClientSecretCredential
import configs.constants as const
import configs.deployments as dep
import openai
from langchain.chat_models import AzureChatOpenAI

# authenticate to Azure
credentials = ClientSecretCredential(const.TENANT_ID, const.SERVICE_PRINCIPAL, const.SERVICE_PRINCIPAL_SECRET)
token = credentials.get_token(const.SCOPE_NON_INTERACTIVE)

# access openai account
openai.api_type = "azure_ad"
openai.api_key = token.token
openai.api_base = f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}"
openai.api_version = const.OPENAI_API_VERSION

st.set_page_config(page_title="LangChain: Chat with search", page_icon="🦜")
st.title("Chat with search")

if "messages" not in st.session_state:
    st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you?"}]

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input(placeholder="Who won the Women's U.S. Open in 2018?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    llm = AzureChatOpenAI(
            openai_api_base=f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}",
            openai_api_version=const.OPENAI_API_VERSION,
            openai_api_key=token.token,
            deployment_name=dep.GPT_35_TURBO,
            openai_api_type="azure_ad",
            streaming=True,)
    
    search_agent = initialize_agent(
        tools=[DuckDuckGoSearchRun(name="Search")],
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        handle_parsing_errors=True,
    )
    with st.chat_message("assistant"):
        st_cb = StreamlitCallbackHandler(st.container(), expand_new_thoughts=False)
        response = search_agent.run(st.session_state.messages, callbacks=[st_cb])
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.write(response)
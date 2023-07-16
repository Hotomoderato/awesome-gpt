import openai
import streamlit as st
import tiktoken
import tenacity
from azure.identity import ClientSecretCredential
import configs.constants as const
import configs.deployments as dep
from openai import ChatCompletion
from langchain.agents import initialize_agent, AgentType
from langchain.callbacks import StreamlitCallbackHandler
from langchain.tools import DuckDuckGoSearchRun
from langchain.chat_models import AzureChatOpenAI

# authenticate to Azure
credentials = ClientSecretCredential(const.TENANT_ID, const.SERVICE_PRINCIPAL, const.SERVICE_PRINCIPAL_SECRET)
token = credentials.get_token(const.SCOPE_NON_INTERACTIVE)

# access openai account
openai.api_type = "azure_ad"
openai.api_key = token.token
openai.api_base = f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}"
openai.api_version = const.OPENAI_API_VERSION

def count_tokens(text):
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    return len(encoding.encode(text))

st.set_page_config(
        page_title="ChatBot",
        page_icon="🤖")
st.header('Your helpful assistant Fullerene 🤖')

# set switch for online and offline
st.sidebar.radio("Select mode", ("Offline", "Online"), index=0, key="mode")

MAX_MEMORY_TOKENS = 3000

if st.session_state.mode == "Offline":

    if "user_messages" not in st.session_state:
        st.session_state.user_messages = []
    if "assistant_messages" not in st.session_state:
        st.session_state.assistant_messages = []
    if "message_history" not in st.session_state:
        st.session_state.message_history = [
            {"role": "system", "content":"You are a helpful assistant named Fullerene. Your personality is lively and you joke about the other person's questions"}]
    

    @tenacity.retry(wait=tenacity.wait_fixed(2), stop=tenacity.stop_after_attempt(5))
    def gpt_call(prompt, placeholder_response):

        response = ChatCompletion.create(
        deployment_id=dep.GPT_35_TURBO, 
        stream=True,
        request_timeout=30,
        messages=st.session_state["message_history"],)
        assistant_response = ""
        for chunk in response:
            if "content" in chunk["choices"][0]["delta"]:
                r_text = chunk["choices"][0]["delta"]["content"]
                assistant_response += r_text
                placeholder_response.chat_message("assistant").markdown(assistant_response, unsafe_allow_html=True)

        return assistant_response

    prompt = st.chat_input("Say something")

    if prompt:
        st.session_state["user_messages"].append(prompt)
        st.session_state["message_history"].append({"role": "user", "content": prompt})
        prompt = st.session_state["user_messages"][-1]

    total_user_messages = len(st.session_state["user_messages"])

    for i in range(total_user_messages):
        st.chat_message("user").write(st.session_state["user_messages"][i])

        if i < total_user_messages - 1:
            st.chat_message("assistant").write(st.session_state["assistant_messages"][i])
        elif i == total_user_messages - 1:
            placeholder_response = st.empty()
            st.session_state["assistant_messages"].append(gpt_call(prompt, placeholder_response))
            st.session_state["message_history"].append({"role": "assistant", "content": st.session_state["assistant_messages"][-1]})

    total_tokens = sum(count_tokens(message["content"]) for message in st.session_state["message_history"])

    while total_tokens > MAX_MEMORY_TOKENS:
        if len(st.session_state["message_history"]) > 2:
            removed_message = st.session_state["message_history"].pop(1)
            total_tokens -= count_tokens(removed_message["content"])
        else:
            break

    st.sidebar.markdown(f"Total tokens: {total_tokens}, Total cost: ${total_tokens/1000 * 0.002:.4f}")


if st.session_state.mode == "Online":
    if "messages" not in st.session_state:
        st.session_state["messages"] = [{"role": "assistant", "content": "How can I help you?"}]

    for msg in st.session_state.messages:
        st.chat_message(msg["role"]).write(msg["content"])

    if prompt := st.chat_input(placeholder="Say something?"):
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

import openai
from azure.identity import ClientSecretCredential

import configs.constants as const
import configs.deployments as dep
import re
from io import BytesIO
from typing import List

# Modules to Import
import openai
import streamlit as st
from langchain import LLMChain
from langchain.chat_models import AzureChatOpenAI
from langchain.agents import AgentExecutor, Tool, ZeroShotAgent
from langchain.chains import RetrievalQA
from langchain.docstore.document import Document
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.memory import ConversationBufferMemory
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores.faiss import FAISS
from langchain.embeddings.openai import OpenAIEmbeddings

from pypdf import PdfReader

# authenticate to Azure
credentials = ClientSecretCredential(const.TENANT_ID, const.SERVICE_PRINCIPAL, const.SERVICE_PRINCIPAL_SECRET)
token = credentials.get_token(const.SCOPE_NON_INTERACTIVE)

# access openai account
openai.api_type = "azure_ad"
openai.api_key = token.token
openai.api_base = f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}"
openai.api_version = const.OPENAI_API_VERSION

# run langchain
llm = AzureChatOpenAI(
    openai_api_base=f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}",
    openai_api_version=const.OPENAI_API_VERSION,
    openai_api_key=token.token,
    deployment_name=dep.GPT_35_TURBO,
    openai_api_type="azure_ad"
)

embeddings = OpenAIEmbeddings(
    openai_api_base=f"{const.OPENAI_API_BASE}/{const.OPENAI_API_TYPE}/{const.OPENAI_ACCOUNT_NAME}",
    openai_api_version=const.OPENAI_API_VERSION,
    openai_api_key=token.token,
    deployment=dep.TEXT_EMBEDDING_ADA_002,
    openai_api_type="azure_ad",
    chunk_size=1)

@st.cache_data
def parse_pdf(file: BytesIO) -> List[str]:
    pdf = PdfReader(file)
    output = []
    for page in pdf.pages:
        text = page.extract_text()
        # Merge hyphenated words
        text = re.sub(r"(\w+)-\n(\w+)", r"\1\2", text)
        # Fix newlines in the middle of sentences
        text = re.sub(r"(?<!\n\s)\n(?!\s\n)", " ", text.strip())
        # Remove multiple newlines
        text = re.sub(r"\n\s*\n", "\n\n", text)
        output.append(text)
    return output


@st.cache_data
def text_to_docs(text: str) -> List[Document]:
    """Converts a string or list of strings to a list of Documents
    with metadata."""
    if isinstance(text, str):
        # Take a single string as one page
        text = [text]
    page_docs = [Document(page_content=page) for page in text]

    # Add page numbers as metadata
    for i, doc in enumerate(page_docs):
        doc.metadata["page"] = i + 1

    # Split pages into chunks
    doc_chunks = []

    for doc in page_docs:
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=4000,
            separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""],
            chunk_overlap=0,)
        chunks = text_splitter.split_text(doc.page_content)
        for i, chunk in enumerate(chunks):
            doc = Document(
                page_content=chunk, metadata={"page": doc.metadata["page"], "chunk": i})
            # Add sources a metadata
            doc.metadata["source"] = f"{doc.metadata['page']}-{doc.metadata['chunk']}"
            doc_chunks.append(doc)
    return doc_chunks


@st.cache_data
def test_embed():
    # Save in a Vector DB
    with st.spinner("It's indexing..."):
        index = FAISS.from_documents(pages, embeddings)
    st.success("Embeddings done.", icon="✅")
    return index

st.title("🤖 PDFBot with Memory 🧠 ")
st.markdown(
    """ 
        ####  🗨️ Chat with your PDF files 📜 with `Conversational Buffer Memory`  
    """)
st.sidebar.markdown(
    """
    ### Steps:
    1. Upload PDF File
    2. Perform Q&A
**Note : File content not stored in any form.**
    """)

uploaded_file = st.file_uploader("**Upload Your PDF File**", type=["pdf"])
if uploaded_file:
    name_of_file = uploaded_file.name
    doc = parse_pdf(uploaded_file)
    pages = text_to_docs(doc)
    
    if pages:
        with st.expander("Show Page Content", expanded=False):
            page_sel = st.number_input(label="Select Page", min_value=1, max_value=len(pages), step=1)
            pages[page_sel - 1]
    
    if embeddings is not None:
        index = test_embed()
        try:
            qa = RetrievalQA.from_chain_type(
                        llm=llm,
                        chain_type="stuff",
                        retriever=index.as_retriever(),)
            tools = [
            Tool(
                name="State of Union QA System",
                func=qa.run,
                description="Useful for when you need to answer questions about the aspects asked. Input may be a partial or fully formed question.",)]
            prefix = """Have a conversation with a human, answering the following questions as best you can based on the context and memory available. 
                            You have access to a single tool and must use it:"""
            suffix = """Begin! {chat_history} Question: {input} {agent_scratchpad}"""

            prompt = ZeroShotAgent.create_prompt(
                        tools,
                        prefix=prefix,
                        suffix=suffix,
                        input_variables=["input", "chat_history", "agent_scratchpad"],)

            if "memory" not in st.session_state:
                        st.session_state.memory = ConversationBufferMemory(memory_key="chat_history")
            llm_chain = LLMChain(llm=llm,prompt=prompt,)
            agent = ZeroShotAgent(llm_chain=llm_chain, tools=tools, verbose=True)
            agent_chain = AgentExecutor.from_agent_and_tools(
                            agent=agent, tools=tools, verbose=True, memory=st.session_state.memory)
            query = st.text_input("**What's on your mind?**",placeholder="Ask me anything from {}".format(name_of_file),)
            if query:
                with st.spinner("Generating Answer to your Query : `{}` ".format(query)):
                    res = agent_chain.run(query)
                    st.info(res, icon="🤖")
            with st.expander("History/Memory"):
                st.session_state.memory
        except:
            st.write("Please upload a file to continue...")


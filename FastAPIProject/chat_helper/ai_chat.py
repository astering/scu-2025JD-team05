# ai_chat.py

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser

# 初始化组件
llm = ChatOpenAI(
    model="deepseek-chat",
    temperature=0.7,
    base_url='https://api.deepseek.com',
    openai_api_key='sk-d51d3f2b8721462aaf35b39ae7accded'  # 替换成你的 key
)

prompt = ChatPromptTemplate.from_template(
    "你是一位风趣的小助手，请用中文回答以下内容：{message}"
)

output_parser = StrOutputParser()

# 构建链
chat_chain = prompt | llm | output_parser


def ai_chat(message: str) -> str:
    """封装调用 LangChain 的聊天函数"""
    return chat_chain.invoke({"message": message})

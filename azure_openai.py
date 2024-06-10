import openai
from openai import AzureOpenAI

from dotenv import load_dotenv,dotenv_values


load_dotenv()
values_env_openai = dotenv_values(".env")

client = AzureOpenAI(
  azure_endpoint = values_env_openai["endpoint"], 
  api_key=values_env_openai["key"],  
  api_version="2023-05-15"
)

def create_prompt(context,query):
    header = "What is Diploblastic and Triploblastic Organisation"
    return context + "\n\n" + query + "\n"


def generate_answer(conversation):
    response = client.chat.completions.create(
    model=values_env_openai["deployment_id_gpt4"],
    messages=conversation,
    temperature=0,
    max_tokens=1000,
    top_p=1,
    frequency_penalty=0,
    presence_penalty=0,
    stop = [' END']
    )
    return (response.choices[0].message.content).strip()
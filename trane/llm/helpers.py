import json
import os
import re

from trane.utils.library_utils import import_or_none

openai = import_or_none("openai")
tiktoken = import_or_none("tiktoken")
anthropic = import_or_none("anthropic")
ipython = import_or_none("IPython")


system_context = (
    "You are an expert data scientist with a specialization in predictive analytics for structured data."
    " Aim for insights reminiscent of the rigor expected from a seasoned data scientist."
    " Focus on the intricacies and complexities of model development, especially those that aren't immediately apparent."
    " Your responses are characterized by clarity, precision, and conciseness."
    " Adherence to the given task's instructions is of utmost importance."
    " Strive to provide the most accurate and relevant insights."
)


def format_prompt(instructions, prompt_context, constraints, problems_formatted):
    prompt = (
        f"{instructions}\n"
        f"## {prompt_context}\n"
        f"## The constraints of your response:\n"
        f"{constraints}\n"
        f"## The list of prediction problems:\n"
        f"{problems_formatted}\n"
        "{{ Insert your response here }}"
    )
    return prompt


def get_max_token_per_problem(problems, model="gpt-3.5-turbo-16k"):
    max_token_per_problem = 0
    for problem in problems:
        encoding = tiktoken.encoding_for_model(model)
        num_tokens = len(encoding.encode(str(problem)))
        max_token_per_problem = max(num_tokens, max_token_per_problem)
    return max_token_per_problem


def analyze(
    problems,
    instructions,
    context,
    model="gpt-3.5-turbo-16k",
    jupyter=False,
):
    prompt_context = f" The context is: {context}"
    constraints = (
        " In your Markdown response, be sure to return the id and original problem text of the prediction problem (in a separate, indented line)."
        " In your Markdown response, explain your reasoning for each prediction problem in 2 sentences (in a separate, indented line)."
    )
    problems_formatted = format_problems(problems)
    prompt = format_prompt(
        instructions,
        prompt_context,
        constraints,
        problems_formatted,
    )
    messages = [
        {"role": "system", "content": system_context},
        {"role": "user", "content": prompt},
    ]
    num_tokens = num_tokens_from_messages(messages, model=model)
    max_token_for_model = get_token_limit(model)
    if num_tokens > max_token_for_model:
        max_token_per_problem = get_max_token_per_problem(problems, model=model)
        max_problems_per_prompt = max_token_for_model // max_token_per_problem
        print(
            "The number of problems exceeds the token limit for the model. Selecting the first {} problems.".format(
                max_problems_per_prompt,
            ),
        )
        problems = problems[0:max_problems_per_prompt]
        problems_formatted = format_problems(problems)
        prompt = format_prompt(
            instructions,
            prompt_context,
            constraints,
            problems_formatted,
        )
    response = openai_gpt(prompt, model)
    if jupyter:
        ipython.display.display(ipython.display.Markdown(response))
    else:
        print(response)

    relevant_ids = extract_problems_from_response(response, model)
    relevant_ids = list(set(relevant_ids))
    relevant_problems = []
    for id_ in relevant_ids:
        relevant_problems.append(problems[int(id_) - 1])
    reasonsings = extract_reasonings_from_response(response)
    for idx, reason in enumerate(reasonsings):
        relevant_problems[idx].set_reasoning(reason)
    relevant_problems = sorted(relevant_problems, key=lambda p: str(p))
    return relevant_problems


def extract_problems_from_response(response, model):
    prompt = (
        f"Extract the IDs in the following text."
        f"## The constraints of your response:\n"
        f" Return your response as JSON only.\n"
        f" Return the IDs in same order as they appear in the text.\n"
        f"## The text:\n"
        f"{response}\n"
        "{{ Insert your response here }}"
    )
    response = openai_gpt(prompt, model)
    if model in ["gpt-3.5-turbo-1106", "gpt-4-1106-preview"]:
        response = re.findall(r"\d+", response)
    else:
        response = json.loads(response).values()
    response = list(flatten(response))
    return response


def extract_reasonings_from_response(text):
    reasonsings = []
    matches = re.findall(r"Reasoning: (.+?)(?:\n\n|$)", text)
    for match in matches:
        reasonsings.append(match)
    return reasonsings


def format_problems(problems: list) -> str:
    formatted = ""
    for idx, problem in enumerate(problems):
        p = str(problem).replace("<", "").replace(">", "")
        formatted += f"ID:{idx + 1} Problem:{p}\n"
    return formatted


def flatten(container):
    for i in container:
        if isinstance(i, (list, tuple)):
            for j in flatten(i):
                yield j
        else:
            yield i


def openai_gpt(prompt: str, model: str, temperature: float = 0.7) -> str:
    client = openai.OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),
    )
    messages = [
        {"role": "system", "content": system_context},
        {"role": "user", "content": prompt},
    ]
    chat_completion = client.chat.completions.create(
        messages=messages,
        model=model,
        temperature=temperature,
    )
    return chat_completion.choices[0].message.content


def get_token_limit(model: str) -> int:
    models = {
        "gpt-3.5-turbo": 4096,
        "gpt-3.5-turbo-16k": 16385,
        "gpt-3.5-turbo-1106": 16385,
        "gpt-3.5-turbo-16k-0613": 16385,
        "gpt-4": 8192,
        "gpt-4-0613": 8192,
        "gpt-4-32k": 32768,
        "gpt-4-32k-0613": 32768,
        "gpt-4-1106-preview": 128000,
    }
    return models.get(model.strip())


def num_tokens_from_messages(messages, model="gpt-3.5-turbo-0613"):
    """Return the number of tokens used by a list of messages."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    if model in {
        "gpt-3.5-turbo-0613",
        "gpt-3.5-turbo-16k-0613",
        "gpt-4-0314",
        "gpt-4-0613",
        "gpt-4-32k-0613",
    }:
        tokens_per_message = 3
        tokens_per_name = 1
    elif model == "gpt-3.5-turbo-0301":
        tokens_per_message = (
            4  # every message follows <|start|>{role/name}\n{content}<|end|>\n
        )
        tokens_per_name = -1  # if there's a name, the role is omitted
    elif "gpt-3.5-turbo" in model:
        print(
            "Warning: gpt-3.5-turbo may update over time. Returning num tokens assuming gpt-3.5-turbo-0613.",
        )
        return num_tokens_from_messages(messages, model="gpt-3.5-turbo-0613")
    elif "gpt-4" in model:
        print(
            "Warning: gpt-4 may update over time. Returning num tokens assuming gpt-4-0613.",
        )
        return num_tokens_from_messages(messages, model="gpt-4-0613")
    else:
        raise NotImplementedError(
            f"""num_tokens_from_messages() is not implemented for model {model}. See https://github.com/openai/openai-python/blob/main/chatml.md for information on how messages are converted to tokens.""",
        )
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
    return num_tokens

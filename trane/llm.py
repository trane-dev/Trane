from trane.utils.library_utils import import_or_none

openai = import_or_none("openai")
tiktoken = import_or_none("tiktoken")


def llm(
    problems: list,
    instructions: str,
    context: str,
    metadata=None,
    model: str = "gpt-3.5-turbo-16k",
) -> str:
    """
    Send instructions to the LLM and get a response.

    Parameters:
    - problems (list) : List of problems (trane.core.Problem).
    - instructions (str) : Instructions for the large language model.
    - context (str): Context for the LLM (describe the dataset).
    - metadata : Metadata for the dataset (trane.metadata). Column descriptions are used with the LLM.
    - model (str) : The model to use. Token limits depend on the model.

    Returns:
    - str: Response from the LLM.
    """

    prompt_context = f" The context is: {context}"
    constraints = (
        "Dive deep into your analyses, ensuring they are thorough and detailed."
        " Aim for insights reminiscent of the rigor expected from a seasoned data scientist."
        " Focus on the intricacies and complexities of model development, especially those that aren't immediately apparent."
        " Do not provide recommendations."
        " Avoid discussing the specifics of the dataset."
        " Do not mention your identity or professional role."
    )
    problems_formatted = format_problems(problems)
    prompt = (
        f"{instructions}\n"
        f"## {prompt_context}\n"
        f"## The constraints of your response:\n"
        f"{constraints}\n"
        f"## The list of prediction problems:\n"
        f"{problems_formatted}\n"
        "{{ Insert your response here }}"
    )
    response = openai_gpt(prompt, model)
    return response


def format_problems(problems: list) -> str:
    formatted = ""
    for idx, problem in enumerate(problems[0:100]):
        formatted += f"{idx + 1}. {str(problem)}\n"
    return formatted


def openai_gpt(prompt: str, model: str, temperature: float = 0.7) -> str:
    """
    Query the OpenAI API with a given prompt and model.

    Parameters:
    - prompt (str): The prompt to send to the LLM.
    - model (str): The model to use.
    - temperature (float): The temperature parameter for the model.

    Returns:
    - str: Response from the LLM.
    """
    system_context = (
        "You are an expert data scientist with a specialization in predictive analytics for structured data."
        " Your responses are characterized by clarity, precision, and conciseness."
        " Adherence to the given task's instructions is of utmost importance."
        " Strive to provide the most accurate and relevant insights."
    )

    messages = [
        {"role": "system", "content": system_context},
        {"role": "user", "content": prompt},
    ]
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )
    return response["choices"][0]["message"]["content"].strip()


def get_token_limit(model: str) -> int:
    """
    Retrieve the token limit for a given model.

    Parameters:
    - model (str): The model to get the token limit for.

    Returns:
    - int: The token limit.
    """
    models = {
        "gpt-3.5-turbo": 4000,
        "gpt-3.5-turbo-16k": 16000,
        "gpt-4": 8000,
        "gpt-4-32k": 32000,
    }

    return models.get(model)


<p align="center">
<img width=50% src="https://github.com/trane-dev/Trane/blob/main/docs/trane-header.png" alt="Trane Logo" />
</p>

<p align="center">
    <a href="https://github.com/trane-dev/Trane/actions/workflows/tests.yaml" target="_blank">
      <img src="https://github.com/trane-dev/Trane/actions/workflows/tests.yaml/badge.svg" alt="Tests Status" />
    </a>
    <a href="https://codecov.io/gh/trane-dev/Trane" target="_blank">
      <img src="https://codecov.io/gh/trane-dev/Trane/branch/main/graph/badge.svg?token=HafAlYGH8F" alt="Code Coverage" />
    </a>
    <a href="https://badge.fury.io/py/Trane" target="_blank">
        <img src="https://badge.fury.io/py/Trane.svg?maxAge=2592000" alt="PyPI Version" />
    </a>
    <a href="https://pepy.tech/project/Trane" target="_blank">
        <img src="https://static.pepy.tech/badge/trane" alt="PyPI Downloads" />
    </a>
</p>

<hr>

**Trane** is a software package that automatically generates problems for temporal datasets and produces labels for supervised learning. Its goal is to streamline the machine learning problem-solving process.

## Install

Install Trane using pip:

```shell
python -m pip install trane
```

## Usage

Here's a quick demonstration of Trane in action:

```python
import trane

data, metadata = trane.load_airbnb()
problem_generator = trane.ProblemGenerator(
  metadata=metadata,
  entity_columns=["location"]
)
problems = problem_generator.generate()

for problem in problems[:5]:
    print(problem)
```

A few of the generated problems:
```
==================================================
Generated 40 total problems
--------------------------------------------------
Classification problems: 5
Regression problems: 35
==================================================
For each <location> predict if there exists a record
For each <location> predict if there exists a record with <location> equal to <str>
For each <location> predict if there exists a record with <location> not equal to <str>
For each <location> predict if there exists a record with <rating> equal to <str>
For each <location> predict if there exists a record with <rating> not equal to <str>
```

With Trane's LLM add-on (`pip install trane[llm]`), we can determine the relevant problems with OpenAI:
```python
from trane.llm import analyze

instructions = "determine 5 most relevant problems about user's booking preferences. Do not include 'predict the first/last X' problems"
context = "Airbnb data listings in major cities, including information about hosts, pricing, location, and room type, along with over 5 million historical reviews."
relevant_problems = analyze(
    problems=problems,
    instructions=instructions,
    context=context,
    model="gpt-3.5-turbo-16k"
)
for problem in relevant_problems:
    print(problem)
    print(f'Reasoning: {problem.get_reasoning()}\n')
```
Output
```text
For each <location> predict if there exists a record
Reasoning: This problem can help identify locations with missing data or locations that have not been booked at all.

For each <location> predict the first <location> in all related records
Reasoning: Predicting the first location in all related records can provide insights into the most frequently booked locations for each city.

For each <location> predict the first <rating> in all related records
Reasoning: Predicting the first rating in all related records can provide insights into the average satisfaction level of guests for each location.

For each <location> predict the last <location> in all related records
Reasoning: Predicting the last location in all related records can provide insights into the most recent bookings for each city.

For each <location> predict the last <rating> in all related records
Reasoning: Predicting the last rating in all related records can provide insights into the recent satisfaction level of guests for each location.
```

## Community

- **Questions or Issues?** Create a [GitHub issue](https://github.com/trane-dev/Trane/issues).
- **Want to Chat?** [Join our Slack community](https://join.slack.com/t/trane-dev/shared_invite/zt-1zglnh25c-ryuQFarw0rVgKHC6ywUOlg).

## Cite Trane

If you find Trane beneficial, consider citing our paper:

Ben Schreck, Kalyan Veeramachaneni. [What Would a Data Scientist Ask? Automatically Formulating and Solving Predictive Problems.](https://dai.lids.mit.edu/wp-content/uploads/2017/10/Trane1.pdf) *IEEE DSAA 2016*, 440-451.

BibTeX entry:

```bibtex
@inproceedings{schreck2016would,
  title={What Would a Data Scientist Ask? Automatically Formulating and Solving Predictive Problems},
  author={Schreck, Benjamin and Veeramachaneni, Kalyan},
  booktitle={Data Science and Advanced Analytics (DSAA), 2016 IEEE International Conference on},
  pages={440--451},
  year={2016},
  organization={IEEE}
}
```

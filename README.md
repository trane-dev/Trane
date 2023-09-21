
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
entity_columns = ["location"]
window_size = "2d"

problem_generator = trane.ProblemGenerator(
    metadata=metadata,
    window_size=window_size,
    entity_columns=entity_columns
)
problems = problem_generator.generate()

print(f'Generated {len(problems)} problems.')
print(problems[108])
print(problems[108].create_target_values(data).head(5))
```

Output:

```
Generated 168 problems.
For each <location> predict the majority <rating> in all related records in the next 2 days.
  location       time  target
0   London 2021-01-01       5
1   London 2021-01-03       4
2   London 2021-01-05       5
3   London 2021-01-07       4
4   London 2021-01-09       5
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

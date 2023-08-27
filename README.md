<p align="center">
<img width=30% src="https://github.com/trane-dev/Trane/blob/new_api/docs/image.jpeg" alt=“Trane_Logo” />
</p>

<p align="center">
    <a href="https://github.com/trane-dev/Trane/actions/workflows/tests.yaml" target="_blank">
      <img src="https://github.com/trane-dev/Trane/actions/workflows/tests.yaml/badge.svg" alt="Tests" />
    </a>
    <a href="https://codecov.io/gh/trane-dev/Trane" >
      <img src="https://codecov.io/gh/trane-dev/Trane/branch/main/graph/badge.svg?token=HafAlYGH8F"/>
    </a>
    <a href="https://badge.fury.io/py/Trane" target="_blank">
        <img src="https://badge.fury.io/py/Trane.svg?maxAge=2592000" alt="PyPI Version" />
    </a>
    <a href="https://pepy.tech/project/Trane" target="_blank">
        <img src="https://static.pepy.tech/badge/tran" alt="PyPI Downloads" />
    </a>
</p>
<hr>

<p align="center">
<i>Automatically formulating machine learning tasks for temporal datasets</i>
</p>

Trane is a software package for automatically generating prediction problems and generating labels for supervised learning. Trane is a system designed to advance the automation of the machine learning problem solving pipeline.

# Install

To install Trane, run the following command:

```shell
python -m pip install trane
```

# Example

Below is an example of using Trane:
```python
import trane

data, metadata = trane.load_airbnb()

entity_col = "location"
window_size = "2d"

problem_generator = trane.ProblemGenerator(
    metadata=metadata,
    window_size=window_size,
)
problems = problem_generator.generate()
print(f'Generated {len(problems)} problems')
print(problems[210])
print(problems[210].create_target_values(data).head(5))
```

```text
Generated 1008 problems
Predict the majority <rating> in all related records in next 2d days
id       time  target
0  720325039 2021-01-01       3
1  720340530 2021-01-01       3
2  720340983 2021-01-01       2
3  720342549 2021-01-01       4
4  720347253 2021-01-01       5
```

# Community
- Need help? Use a [GitHub issue](https://github.com/trane-dev/Trane/issues)
- Prefer chatting? [Join Slack](https://join.slack.com/t/trane-dev/shared_invite/zt-1zglnh25c-ryuQFarw0rVgKHC6ywUOlg)

# Citing Trane
If you use Trane, please consider citing the following paper:

Ben Schreck, Kalyan Veeramachaneni. [What Would a Data Scientist Ask? Automatically Formulating and Solving Predictive Problems.](https://dai.lids.mit.edu/wp-content/uploads/2017/10/Trane1.pdf) *IEEE DSAA 2016*, 440-451

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

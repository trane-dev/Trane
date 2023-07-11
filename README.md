<p align="center">
<img width=30% src="https://dai.lids.mit.edu/wp-content/uploads/2018/06/Trane-logo-300x180.jpg" alt=“Trane_Logo” />
</p>

<p align="center">
    <a href="https://github.com/HDI-Project/Trane/actions/workflows/tests.yaml" target="_blank">
      <img src="https://github.com/HDI-Project/Trane/actions/workflows/tests.yaml/badge.svg" alt="Tests" />
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

<p align="center">
  <a href="https://www.youtube-nocookie.com/embed/TrK5Tm9ic28"><img src="https://img.youtube.com/vi/TrK5Tm9ic28/0.jpg" width="70%" target="_blank" alt="Trane About Video"></a>
</p>

# Install

To install Trane, run the following command:

```shell
python -m pip install trane
```

# Example

Below is an example of using Trane:

```python
import trane

data = trane.datasets.load_covid()
table_meta = trane.datasets.load_covid_metadata()

entity_col = "Country/Region"
window_size = "2d"
minimum_data = "2020-01-22"
maximum_data = "2020-03-29"
cutoff_strategy = trane.CutoffStrategy(
    entity_col=entity_col,
    window_size=window_size,
    minimum_data=minimum_data,
    maximum_data=maximum_data,
)
time_col = "Date"
problem_generator = trane.PredictionProblemGenerator(
    df=data,
    entity_col=entity_col,
    time_col=time_col,
    cutoff_strategy=cutoff_strategy,
    table_meta=table_meta,
)
problems = problem_generator.generate(data, generate_thresholds=True)
```


## Citing Trane
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

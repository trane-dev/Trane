<p align="center">
<img width=40% src="https://dai.lids.mit.edu/wp-content/uploads/2018/06/Trane-logo-300x180.jpg" alt=“Trane_Logo” />
</p>

<p align="center">
<i>Automatically formulating machine learning tasks for temporal datasets"</i>
</p>

[![Build Status](https://travis-ci.org/HDI-Project/Trane.svg?branch=master)](https://travis-ci.org/HDI-Project/Trane)

# Trane
Trane is a software package for automatically generating prediction problems and generating labels for supervised learning. Trane is a system designed to advance the automation of the machine learning problem solving pipeline.

<p align="center">
  <a href="https://www.youtube-nocookie.com/embed/TrK5Tm9ic28"><img src="https://img.youtube.com/vi/TrK5Tm9ic28/0.jpg" width="70%" alt="Trane About Video"></a>
</p>

## Getting Started
There is an [example notebook here](https://github.com/HDI-Project/Trane/tree/master/Examples).

Import and Load Data:

```
import os, warnings
import numpy as np
import pandas as pd
import trane

# load a dataframe
df = pd.read_csv('Example/medical_no_show.csv', parse_dates=['appointment_day', 'scheduled_day']).head(500)

# load the table metadata
meta = trane.TableMeta(open('Example/meta.json').read())

# define a cutoff strategy
cutoff_fn = lambda rows, entity_id: np.datetime64('1980-02-25')
cutoff_strategy = trane.CutoffStrategy(generate_fn=cutoff_fn, description='with a fixed cutoff of 1980-02-25')
```

Define and execute custom prediction problem:

```
# define operations
filter_op = trane.ops.LessFilterOp(column_name='age'); filter_op.set_hyper_parameter(65)
row_op = trane.ops.IdentityRowOp(column_name='no_show')
transformation_op = trane.ops.IdentityTransformationOp(column_name='no_show')
aggregation_op = trane.ops.LastAggregationOp(column_name='no_show')
operations = [filter_op, row_op, transformation_op, aggregation_op]

# create the prediction problem
problem = trane.PredictionProblem(
    operations=operations,
    entity_id_col='appointment_id',
    label_col='no_show',
    table_meta=meta,
    cutoff_strategy=cutoff_strategy)

# Execute the problem
problem.execute(df)
```

You can also automaticaly generate prediction problems:

```
problem_generator = trane.PredictionProblemGenerator(
    table_meta=meta, entity_col='appointment_id', label_col='no_show', filter_col='age')
problems = problem_generator.generate(df)
print(problems[0:5])

# execute the first problem generated
problems[0].execute(df)
```


## Prediction Problems
In data science, people usually have a few records of an entity and want to predict what will happen to that entity in the future. Trane is designed to generate time-related prediction problems. Trane transforms data meta information into lists of relevant prediction problems and cutoff times. Prediction problems are structured in a formal language described in Operations below. Cutoff times are defined as the last time in the data used for training the classifier. Data after the cutoff time is used for evaluating the classifiers accuracy. Cutoff times are necessary to prevent the classifier from training to test data.

### Example
A bank wants to predict whether a customer will make a transaction greater than $100 after 2017. Assume we have all the transaction records for each client from 2016 to 2018. We want to build a machine learning method to solve the prediction problem. Here is the example database.

|User_id|Time|Transaction_id|Amount|
|:--:|:--:|:--:|:--:|
| u1 | 2016 | 1-2015-1 | 10 |
| u1 | 2016 | 1-2015-2 | 200 |
| u2 | 2016 | 2-2015-1 | 50 |
| u2 | 2017 | 1-2016-1 | 10 |
| u1 | 2018 | 1-2017-1 | 1000|
| u1 | 2018 | 1-2017-2 | 20 |
| u2 | 2018 | 2-2017-1 | 10 |

First, we seperate the data by entity. Here the entity is user_id. User u1 for example, has

|User_id|Time|Transaction_id|Amount|
|:--:|:--:|:--:|:--:|
| u1 | 2016 | 1-2015-1 | 10 |
| u1 | 2016 | 1-2015-2 | 200 |
| u1 | 2017 | 1-2016-1 | 10 |
| u1 | 2018 | 1-2017-1 | 1000|
| u1 | 2018 | 1-2017-2 | 20 |

Let's consider a **cutoff time** equal to 2016. The data from before 2016 will be used as training data in the machine learning model. Data after 2016 will be used to evaluate the trained model. Trane outputs a dataframe for each prediction problem.

In pseudocode, problem's would be described as

```
cutoff_fn = lambda rows, entity_id: np.datetime64('2016-01-01')
cutoff_strategy = trane.CutoffStrategy(
  generate_fn=cutoff_fn, description='with a fixed cutoff of 2016-01-01')

operations = [ops.AllFilterOp(), ops.IdentityRowOp(),
         ops.IdentityTransformationOp(), ops.ExistsAggregatoinOp()]

problem = trane.PredictionProblem(
    operations=operations,
    entity_id_col='User_id',
    label_col='Amount',
    table_meta=meta,
    cutoff_strategy=cutoff_strategy)

trane.execute(df)
```

For the above problem, the dataframe would be

|User_id|Label|Cutoff|
|:--:|:--:|:--:|
| u1 | True | 2016-01-01 |
| u2 | False | 2016-01-01 |


This dataframe can then be fed into [FeatureTools](https://github.com/featuretools/featuretools) for feature engineering.

### Prediction Problem Generation
A prediction problem is a sequence of operations applied to data as well as a cutoff time.

In Trane, we generate prediction problems with four operations: Filter Operations, Row Operations, Transformation Operations and Aggregation Operations. Filter operations are applied on the filter\_column. Row, Transformation and Aggregation Operations are applied on the label\_generating\_column.

## Workflow

The workflow of using Trane on a database is as follows:

- Data scientist writes a `meta.json` describing columns and data types in the new database.
- `PredictionProblemGenerator` reads the meta data and a dataframe, and generates a list of possible prediction problems.
- Problems can be edited and saved to file.
- A PredictionProblem's execute method can be called on a dataframe.


## Built-in Operations
- FilterOp
    - IdentityFilterOp
    - GreaterFilterOp
- RowOp
    - IdentityRowOp
    - GreaterRowOp
- TransformationOp
    - IdentityTransformationOp
    - DiffTransformationOp
- AggregationOp
    - FirstAggregationOp
    - CountAggregationOp
    - SumAggregationOp
    - LastAggregationOp
    - LMFAggregationOp

## Unit Testing
We use `pytest` to automatically collecting unit testings and `pytest-cov` to measure the coverage of unit testing. The application code is in `Trane/trane/`. The unit testing code is in `Trane/tests/`. To run all unit testing, change directory to `Trane` and execute

```
> pytest --cov=trane tests
```


## Setup/Install
### Clone from Git
```
> git clone https://github.com/HDI-Project/Trane.git
```
### Run pip install
```
> pip3 install Trane/
```

## History
We started working on Trane in 2015. In its first iteration in 2016, we showed that it is possible to formally specify prediction problems using a language and then also created algorithms to generate prediction problems automatically. With other tools to synthesize features and generate models given a prediction problem - we were able to solve problems end-to-end. You can read our paper [here](https://dai.lids.mit.edu/wp-content/uploads/2017/10/Trane1.pdf). Ben Schreck's [thesis](https://dspace.mit.edu/bitstream/handle/1721.1/105963/965551096-MIT.pdf) goes even further to see if we can learn and filter uninteresting problems.

This repository is a second iteration where we are focusing on usability, apis and showing more use cases and ultimately taking it to real world datasets. The library was rewritten by Alex Nordin and then refactored by Albert Carter.

You can find the related theses here:

* [Towards An Automatic Predictive Question Formulation](https://dspace.mit.edu/bitstream/handle/1721.1/105963/965551096-MIT.pdf?sequence=1)
Benjamin J. Schreck, M.E. thesis, MIT Dept of EECS, June 2016. Advisor: Kalyan Veeramachaneni.
* [End to End Machine Learning Workflow Using
Automation Tools](https://dai.lids.mit.edu/wp-content/uploads/2018/05/Alex_MEng_final.pdf) Alexander Friedrich Nordin, MIT Dept of EECS, June, 2018. Advisor: Kalyan Veeramachaneni.


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

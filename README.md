<p align="center">
<img width=30% src="https://dai.lids.mit.edu/wp-content/uploads/2018/06/Trane-logo-300x180.jpg" alt=“Trane_Logo” />
</p>

<p align="center">
    <a href="https://github.com/HDI-Project/Trane/actions/workflows/tests.yaml" target="_blank">
      <img src="https://github.com/HDI-Project/Trane/actions/workflows/tests.yaml/badge.svg" alt="Tests" />
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
<i>Automatically formulating machine learning tasks for temporal datasets"</i>
</p>

Trane is a software package for automatically generating prediction problems and generating labels for supervised learning. Trane is a system designed to advance the automation of the machine learning problem solving pipeline.

<p align="center">
  <a href="https://www.youtube-nocookie.com/embed/TrK5Tm9ic28"><img src="https://img.youtube.com/vi/TrK5Tm9ic28/0.jpg" width="70%" target="_blank" alt="Trane About Video"></a>
</p>

# Install

Trane is available for Python 3.7, 3.8, 3.9 and 3.10.
To install Trane, run the following command:

```shell
$ python -m pip install Trane
```

## Prediction Problems
In data science, people usually have a few records of an entity and want to predict what will happen to that entity in the future. Trane is designed to generate time-related prediction problems. Trane transforms data meta information into lists of relevant prediction problems and cutoff times. Prediction problems are structured in a formal language described in Operations below. Cutoff times are defined as the last time in the data used for training the classifier. Data after the cutoff time is used for evaluating the classifiers accuracy. Cutoff times are necessary to prevent the classifier from training to test data.

### Example
A bank wants to predict how many transactions over 100$ a customer will make in the next year. Assume we have all the transaction records for each client from 2015 to 2017. We want to build a machine learning method to solve the prediction problem. Here is the example database.

|User_id|Time|Transaction_id|Amount|
|:--:|:--:|:--:|:--:|
| u1 | 2015 | 1-2015-1 | 10 |
| u1 | 2015 | 1-2015-2 | 200 |
| u2 | 2015 | 2-2015-1 | 50 |
| u1 | 2016 | 1-2016-1 | 10 |
| u1 | 2017 | 1-2017-1 | 1000|
| u1 | 2017 | 1-2017-2 | 20 |
| u2 | 2017 | 2-2017-1 | 10 |

First, we seperate the data by entity. Here the entity is user_id. User u1 for example, has

|User_id|Time|Transaction_id|Amount|
|:--:|:--:|:--:|:--:|
| u1 | 2015 | 1-2015-1 | 10 |
| u1 | 2015 | 1-2015-2 | 200 |
| u1 | 2016 | 1-2016-1 | 10 |
| u1 | 2017 | 1-2017-1 | 1000|
| u1 | 2017 | 1-2017-2 | 20 |

Let's consider a **cutoff time** equal to 2016. The data from 2015-2016 will be used as training data in the machine learning model. Data after 2016, that is data from 2016-2017 will be used to evaluate the trained model. Trane outputs a tuple of (entity, cutoff, label) for each prediction problem. A prediction problem is applied to entity data to generate the label. The data from Trane can be fed directly into Feature Tools to perform feature engineering.

### Prediction Problem Generation
As shown in the example, a prediction problem is a sequence of operations applied to data as well as a cutoff time.

In Trane, we generate prediction problems with four operations: Filter Operations, Row Operations, Transformation Operations and Aggregation Operations. Filter operations are applied on the filter\_column. Row, Transformation and Aggregation Operations are applied on the label\_generating\_column.

## Workflow

The workflow of using Trane on a database is as follows:

- Data scientist writes a `meta.json` describing columns and data types in the new database.
- `PredictionProblemGenerator` reads the meta data and generates possible prediction problems. The prediction problems are saved to `problems.json`.
- The data scientist can change parameters to the prediction problems in `problems.json`.
- The `labeler` applies prediction problems in `problems.json` to the database `data.csv`

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

## Quick Usage
We have [tutorial notebooks here](https://github.com/HDI-Project/Trane-Demos/tree/master/IPYNBs).

## History
We started working on Trane in 2015. In its first iteration in 2016, we showed that it is possible to formally specify prediction problems using a language and then also created algorithms to generate prediction problems automatically. With other tools to synthesize features and generate models given a prediction problem - we were able to solve problems end-to-end. You can read our paper [here](https://dai.lids.mit.edu/wp-content/uploads/2017/10/Trane1.pdf). Ben Schreck's [thesis](https://dspace.mit.edu/bitstream/handle/1721.1/105963/965551096-MIT.pdf) goes even further to see if we can learn and filter uninteresting problems.

This repository is a second iteration where we are focusing on usability, apis and showing more use cases and ultimately taking it to real world datasets. Stay tuned for more demos and examples.

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
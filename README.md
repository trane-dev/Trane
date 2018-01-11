# Trane
Trane is a software package for automatically generating prediction problems and generating labels for supervised learning. 

## Prediction Problems
In data science, people usually have a few records of an entity and want to predict what will happen to that entity in the future. 

### Example
A bank may want to predict how many transactions greater than $100 will a client make in the next year. Assume we have all the transaction records for each client from 2015 to 2017. We want to build a machine learning method to do the prediction problem. Here is an example database.

|User_id|Time|Transaction_id|Amount|
|:--:|:--:|:--:|:--:|
| u1 | 2015 | 1-2015-1 | 10 |
| u1 | 2015 | 1-2015-2 | 200 |
| u2 | 2015 | 2-2015-1 | 50 |
| u1 | 2016 | 1-2016-1 | 10 |
| u1 | 2017 | 1-2017-1 | 1000|
| u1 | 2017 | 1-2017-2 | 20 |
| u2 | 2017 | 2-2017-1 | 10 |

We first separate it by entityies Here the entity is user_id. Take user u1 for example, We have

|User_id|Time|Transaction_id|Amount|
|:--:|:--:|:--:|:--:|
| u1 | 2015 | 1-2015-1 | 10 |
| u1 | 2015 | 1-2015-2 | 200 |
| u1 | 2016 | 1-2016-1 | 10 |
| u1 | 2017 | 1-2017-1 | 1000|
| u1 | 2017 | 1-2017-2 | 20 |

We need a **cutoff time** equals 2016. We use data from 2015 to 2016 as features (first 3 records) and we use data in 2017 (last 2 records) to generate labels. We apply a filter operation and an aggregation operation on the last 2 records and we can get a tuple (entity=u1, cutoff=2016, label=1). Similarly, we have (entity=u2, cutoff=2016, label=0). These tuples as well as the training data a fed to automatical machine learning tools such as featuretools to learn a machine learning model. 

### Prediction Problem Generation
As shown in the example, a prediction problem is a sequence of operations applied to a database as well as a cutoff time. Labels are a list of (entity, cutoff, label) tuples. 

In trane, we generate prediction problems with four operations (FilterOp, RowOp, TransformationOp, AggregationOp). FilterOp is applied on filter\_column, RowOp, TransformationOp and AggregationOp are applied on label\_generating\_column

## Workflow

The workflow of using trane on a database is as follow.

- Data scientist write a `meta.json` describing columns and data types in the new database.
- `PredictionProblemGenerator` reads the meta data and generate possible prediction problems and save them in `problems.json`.
- Data scientist can change parameters in `problems.json`.
- The `labeler` applies prediction problems in `problems.json` on a real database `data.csv`


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

## Usage
The directory structure is

```
|-Trane
| |-trane
| | |-...
| |-generate_labels.py
| |-generate_tasks.py
| |-run.sh
|-test_datasets
| |-donations_meta.json
| |-donations_sample.csv
```
Run the example by

```
> bash run.sh
```
- It executes `generate_tasks.py` which generates prediction problems and randomly saves 5 problems to `tasks.json`.
- We can edit thresholds in `tasks.json`. (omitted in `run.sh`, default is 0.)
- Run `generate_labels.py` and print task/desciption/label on screen.

## Unit Testing
We use `pytest` to automatically collecting unit testings and `pytest-cov` to measure the coverage of unit testing. The application code is in `Trane/trane/`. The unit testing code is in `Trane/tests/`. To run all unit testings, change directory to `Trane` and execute

```
> pytest --cov=trane tests
```

## TODO
- Need an easier way to add customize operations. Currently, external plugin operations are not allowed. The bottleneck is we need to maintain a list of operations so that we can save, load, and iterate over operations. It's not easy to add an external operation into operation list. 
- Currently, all operations are in-place operations. The aggregation ops simply take a record, change the value in the column and return. May not be a good design.
- API for setting thresholds. 
- Some NotImplementedError.
- NL system should be independent of Trane. Seems better to generate NL from JSON.

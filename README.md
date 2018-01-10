# Trane
A software package that takes any dataset as input and generates prediction problems relevant to the data. 

# Assumptions
- All filters operate on a column whose type is TYPE\_VALUE. So filter operations are always applied on a TYPE\_VALUE column and don't change the column type.
- Row operations are always applied on a TYPE\_VALUE column and may generate a TYPE\_VALUE or TYPE\_BOOL column.
  - If Row operations generate TYPE\_VALUE, identity and diff transformation operations can be used. The output is TYPE\_VALUE.
  - If Row operations generate TYPE\_BOOL, only identity is applicable. The output is TYPE\_BOOL.
- The first, last, count aggregation operations are applicable on both TYPE\_VALUE and TYPE\_BOOL. The sum and LMF operations only take TYPE_VALUE.
- The NL description system only works with the previous assumptions.

# Requirements
numpy
pandas

# Ops
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

# Usage
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
bash run.sh
```
- It executes `generate_tasks.py` which generates prediction problems and randomly saves 5 problems to `tasks.json`.
- Then use `json.tool` to prettify output as `tasks_pretty.json`.
- We can edit thresholds in `tasks_pretty.json`. (omitted in `run.sh`, default is 0.)
- Run `generate_labels.py` and print task/desciption/label on screen.

# Unit Testing
We use `pytest` to automaticly collecting unit testings and `pytest-cov` to measure the coverage of unit testing. The application code are in `Trane/trane/`. The unit testing code are in `Trane/tests/`. To run all unit testings, change directory to `Trane` and execute

```
> pytest --cov=trane tests
```

# TODO
- Currently, all operations are in-place operations. The aggregation ops simply take a record, change the value in the column and return. May not be a good design.
- API for setting thresholds. 
- Some NotImplementedError.
- NL system should be independent of Trane. Seems better to generate NL from JSON.

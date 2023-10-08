.. changelog:

Changelog
---------
v0.7.0 ()
=========
* Enhancements
    * Add Airbnb Reviews dataset [#161][#161]
    * Remove compose as a dependency [#162][#162]
    * Remove scikit learn as a dependency [#164][#164]
* Fixes

    [#161]: <https://github.com/trane-dev/Trane/pull/161>
    [#162]: <https://github.com/trane-dev/Trane/pull/162>
    [#164]: <https://github.com/trane-dev/Trane/pull/164>

v0.6.0 (August 27, 2023)
========================
* Enhancements
    * Add pyarrow dependency and use pyarrow backed dtypes [#120][#120]
    * Add Airbnb Reviews dataset [#125][#125]
    * Add Store dataset [#131][#131]
    * Add Order By Operation (OrderByOp, along with IdentityOp and TransformationOpBase) [#138][#138]
    * Add First and Last Aggregation Operations [#144][#144]
    * Enable operations to exclude other operations they can be applied with [#147][#147]
    * Add new Problem and Problem Generator class for simplier API [#136][#136]
    * Cleaned up Readme with new Problem class example [#136][#136]
    * Add denormalize function with metadata and data only support [#136][#136]
* Fixes
    * Rename `_execute_operations_on_df` to `target` in executed prediction problem dataframe [#124][#124]
    * Clean up operation description generation [#118][#118]
    * Remove PredictionProblemEvaluator [#118][#118]
    * Remove FeaturetoolsWrapper class [#100][#100]
    * Remove covid19 and youtube datasets [#131][#131]
    * Prevent Aggregation operations besides `FirstAggregationOp` and `LastAggregationOp` to be paired with `OrderByOp` [#147][#147]
    * Prevent `FirstAggregationOp` and `LastAggregationOp` from being paired with `IdentityByOp` (hence only allowing `IdentityByOp`) [#147][#147]
    * Changed Logical Types to ML Types [#136][#136]
    * Removed the ColumnSchema class [#136][#136]
    * Removed the CutoffStrategy class [#136][#136]
    * Make threshold more clear in Problem string output [#136][#136]
    * Add MultiTableMetadata and SingleTableMetadata class [#136][#136]

    [#124]: <https://github.com/trane-dev/Trane/pull/124>
    [#118]: <https://github.com/trane-dev/Trane/pull/118>
    [#120]: <https://github.com/trane-dev/Trane/pull/120>
    [#125]: <https://github.com/trane-dev/Trane/pull/125>
    [#131]: <https://github.com/trane-dev/Trane/pull/131>
    [#138]: <https://github.com/trane-dev/Trane/pull/138>
    [#144]: <https://github.com/trane-dev/Trane/pull/144>
    [#147]: <https://github.com/trane-dev/Trane/pull/147>
    [#136]: <https://github.com/trane-dev/Trane/pull/136>


v0.5.0 (July 27, 2023)
======================
* Enhancements
    * Add ``ExistsAggregationOp`` [#96][#96]
    * Add `get_aggregation_ops` and `get_filter_ops` functions [#98][#98]
    * Enable operations to exclude columns they can be applied to with semantic tags [#108][#108]
* Fixes
    * Fix FeaturetoolsWrapper class and label times [#100][#100]
    * Fix denormalize to support more than 2 tables [#104][#104]
    * Exclude foreign keys from being used to generate prediction problems [#108][#108]
    * Change "index" to "primary_key" [#108][#108]
    * Fixes duplicate prediction problem generation by hashing all possible prediction problems [#108][#108]

    [#96]: <https://github.com/trane-dev/Trane/pull/96>
    [#98]: <https://github.com/trane-dev/Trane/pull/98>
    [#100]: <https://github.com/trane-dev/Trane/pull/100>
    [#104]: <https://github.com/trane-dev/Trane/pull/104>
    [#108]: <https://github.com/trane-dev/Trane/pull/108>

v0.4.0 (July 8, 2023)
=====================
* Enhancements
    * Add detailed walkthroughts in Examples directory [#60][#60]
    * Add code coverage analysis from Codecov [#77][#77]
    * Clean up input and output for operations [#87][#87]
    * Add additional unit tests for _check_operations_valid [#93][#93]
* Fixes
    * Remove TableMeta class and replace with ColumnSchema [#83][#83] [#85][#85]

    [#60]: <https://github.com/HDI-Project/Trane/pull/60>
    [#77]: <https://github.com/HDI-Project/Trane/pull/77>
    [#83]: <https://github.com/HDI-Project/Trane/pull/83>
    [#85]: <https://github.com/HDI-Project/Trane/pull/85>
    [#87]: <https://github.com/HDI-Project/Trane/pull/87>
    [#93]: <https://github.com/HDI-Project/Trane/pull/93>

v0.3.0 (February 24, 2023)
==========================

* Enhancements
    * Add workflow to clear old caches [#57][#57]
* Fixes
    * Update to use new compose argument [#56][#56]
    * Remove py from requirements [#56][#56]

    [#56]: <https://github.com/HDI-Project/Trane/pull/56>
    [#57]: <https://github.com/HDI-Project/Trane/pull/57>
    [#58]: <https://github.com/HDI-Project/Trane/pull/58>

v0.2.0 (January 5, 2023)
========================

* Enhancements
    * Add instructions on how to release trane and changelog updated checker [#45][#45]
    * Use pyproject.toml and add install workflow to GitHub Actions [#39][#39]
    * Add integration tests and got existing unit tests to pass [#41][#41]
    * Add Python 3.11 markers and CI testing [#41][#41]
    * Add lint check with black and ruff [#43][#43]
    * Add tqdm as a req to show progress bar for prediction problem generation [#41][#41]
    * Improve unit tests, add pre-commit, add install commands to Makefile [#42][#42]
    * Add release workflow, move all examples under Examples folder, remove docs folder [#44][#44]
    * Clean up README.md and add badges [#46][#46]
* Fixes
    * Cleaned up Makefile [#41][#41]
    * Modified existing logic to allow unit tests to pass [#41][#41]
    * Do not use cache is automated latest dependency checker is running [#51][#51]
* Changes
    * Remove setup.py, and setup.cfg in favor of pyproject.toml & .flake8 [#39][#39]
    * Removed all requirements-X.txt files and are centralized to pyproject.toml [#39][#39]
    * Removed bumpversion from project release requirements [#39][#39]

    [#39]: <https://github.com/HDI-Project/Trane/pull/39>
    [#41]: <https://github.com/HDI-Project/Trane/pull/41>
    [#42]: <https://github.com/HDI-Project/Trane/pull/42>
    [#43]: <https://github.com/HDI-Project/Trane/pull/43>
    [#44]: <https://github.com/HDI-Project/Trane/pull/44>
    [#45]: <https://github.com/HDI-Project/Trane/pull/45>
    [#46]: <https://github.com/HDI-Project/Trane/pull/46>
    [#51]: <https://github.com/HDI-Project/Trane/pull/51>
    [#52]: <https://github.com/HDI-Project/Trane/pull/52>

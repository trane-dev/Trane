.. changelog:

Changelog
---------
What’s new in 0.3.0 (MONTH, DAY, YEAR)
=====================================

* Enhancements
    * Add workflow to clear old caches [#57][#57]

* Fixes
    * Update to use new compose argument [#56][#56]
* Changes

    [#56]: <https://github.com/HDI-Project/Trane/pull/56>
    [#57]: <https://github.com/HDI-Project/Trane/pull/57>

What’s new in 0.2.0 (January 5, 2023)
=====================================

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

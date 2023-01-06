.. changelog:

Changelog
---------

What’s new in 0.3.0 (MONTH DAY, YEAR)
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
    * Fixes
        * Cleaned up Makefile [#41][#41]
        * Modified existing logic to allow unit tests to pass [#41][#41]
    * Changes
        * Remove setup.py, and setup.cfg in favor of pyproject.toml & .flake8 [#39][#39]
        * Removed all requirements-X.txt files and are centralized to pyproject.toml [#39][#39]
        * Removed bumpversion from project release requirements [#39][#39]

        [#39]: <https://github.com/HDI-Project/Trane/pull/39>
        [#41]: <https://github.com/HDI-Project/Trane/pull/41>
        [#44]: <https://github.com/HDI-Project/Trane/pull/44>
        [#45]: <https://github.com/HDI-Project/Trane/pull/45>

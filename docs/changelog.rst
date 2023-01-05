.. changelog:

Changelog
---------

What’s new in 0.3.0 (MONTH DAY, YEAR)
=====================================

    * Enhancements
        * Add instructions on how to release trane and changelog updated checker (:pr:`45`)
        * Use pyproject.toml and add install workflow to GitHub Actions (:pr:`39`)
        * Add integration tests and got existing unit tests to pass (:pr:`41`)
        * Add Python 3.11 markers and CI testing (:pr:`41`)
        * Add lint check with black and ruff (:pr:`43`)
        * Add tqdm as a req to show progress bar for prediction problem generation (:pr:`41`)
        * Improve unit tests, add pre-commit, add install commands to Makefile (:pr:`42`)
        * Add release workflow, move all examples under Examples folder, remove docs folder (:pr:`44`)
    * Fixes
        * Cleaned up Makefile (:pr:`41`)
        * Modified existing logic to allow unit tests to pass (:pr:`41`)
    * Changes
        * Remove setup.py, and setup.cfg in favor of pyproject.toml & .flake8 (:pr:`39`)
        * Removed all requirements-X.txt files and are centralized to pyproject.toml (:pr:`39`)
        * Removed bumpversion from project release requirements (:pr:`39`)

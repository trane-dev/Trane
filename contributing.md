# Contributing to Trane

:+1::tada: First off, thank you for taking the time to contribute! :tada::+1:

Whether you are a novice or experienced software developer, all contributions and suggestions are welcome!

## 0. Fork repo (optional)
* It helps keep things clean if you fork it first and clone from there.
* Otherwise, just clone directly from the repo

## 1. Clone repo

* Use Git to clone the project and make changes to the codebase. Once you have obtained a copy of the code, you should create a development environment that is separate from your existing Python environment so that you can make and test changes without compromising your own work environment.
* You can run the following steps to clone the code, create a separate virtual environment, and install Trane in editable mode.
* Remember to create a new branch indicating the issue number with a descriptive name

  ```bash
  git clone https://github.com/HDI-Project/Trane.git
  OR (if forked)
  git clone https://github.com/[your github username]/Trane.git
  cd Trane
  python -m venv venv
  source venv/bin/activate
  make installdeps-dev
  git checkout -b issue##-branch_name
  ```

## 2. Implement your Pull Request

* Implement your pull request. If needed, add new tests or update the documentation.
* Before submitting to GitHub, verify the tests run and the code lints properly

  ```bash
  # runs tests
  make tests

  # runs linting
  make lint

  # will fix some common linting issues automatically
  make lint-fix
  ```

* Before you commit, a few lint fixing hooks will run. You can also manually run these.
  ```bash
  # run linting hooks only on changed files
  pre-commit run

  # run linting hooks on all files
  pre-commit run --all-files
  ```

## 3. Submit your Pull Request

* Once your changes are ready to be submitted, make sure to push your changes to GitHub before creating a pull request. Create a pull request, and our continuous integration will run automatically.

* Be sure to include unit tests for your changes; the unit tests you write will also be run as part of the continuous integration.

* Until your pull request is ready for review, please [draft the pull request](https://docs.github.com/en/free-pro-team@latest/github/collaborating-with-issues-and-pull-requests/about-pull-requests#draft-pull-requests) to indicate its not yet ready for review. This signals the team to ignore it and allow you to develop.

* We will review your changes, and you will most likely be asked to make additional changes before it is finally ready to merge. However, once it's reviewed by a maintainer of Trane, passes continuous integration, we will merge it, and you will have successfully contributed to Trane!

## Report issues

When reporting issues please include as much detail as possible about your operating system, Trane version and Python version. Whenever possible, please also include a brief, self-contained code example that demonstrates the problem.

## Code Style Guide

* Keep things simple. Any complexity must be justified in order to pass code review.
* Be aware that while we love fancy Python magic, there's usually a simpler solution which is easier to understand!
* Make PRs as small as possible! Consider breaking your large changes into separate PRs. This will make code review easier, quicker, less bug-prone and more effective.
* In the name of every branch you create, include the associated issue number if applicable.
* If new changes are added to the branch you're basing your changes off of, consider using `git rebase -i base_branch` rather than merging the base branch, to keep history clean.
* Always include a docstring for public methods and classes. Consider including docstrings for private methods too. Our docstring convention is [`sphinx.ext.napoleon`](https://www.sphinx-doc.org/en/master/usage/extensions/napoleon.html).
* Use [PascalCase (upper camel case)](https://en.wikipedia.org/wiki/Camel_case#Variations_and_synonyms) for class names, and [snake_case](https://en.wikipedia.org/wiki/Snake_case) for method and class member names.
* To distinguish private methods and class attributes from public ones, those which are private should be prefixed with an underscore
* Any code which doesn't need to be public should be private. Use `@staticmethod` and `@classmethod` where applicable, to indicate no side effects.
* Only call public methods in unit tests.
* All code must have unit test coverage. Use mocking and monkey-patching when necessary.
* Keep unit tests as fast as possible.
* When you're working with code which uses a random number generator, make sure your unit tests set a random seed.

## GitHub Issue Guide

* Make the title as short and descriptive as possible.
* Make sure the body is concise and gets to the point quickly.
* Check for duplicates before filing.
* For bugs, a good general outline is: problem summary, reproduction steps, symptoms and scope, root cause if known, proposed solution(s), and next steps.
* If the issue writeup or conversation get too long and hard to follow, consider starting a design document.
* Use the appropriate labels to help your issue get triaged quickly.
* Make your issues as actionable as possible. If they track open discussions, consider prefixing the title with "[Discuss]", or refining the issue further before filing.
## adruk_tools
This is a collection of functions developed by the ADR Engineering team for work accross adruk related pieplines

## Setup
### Pre-commit-hooks
1. `pip3 install -r requirements.txt`
2. `pre-commit install` to install the pre-commit hooks ([flake8](https://flake8.pycqa.org/en/latest/), [pydocstyle](http://www.pydocstyle.org/en/stable/) & [darglint](https://github.com/terrencepreilly/darglint)) used in development to ensure coding style consistency.

With pre-commit hooks in place, everything commit you make will be checked against some PEP8 standards. The output message will tell you what needs editing and on which line. Once edits are made, you need to re-add your changes and try and commit again. Repeat until code has passed.

If you want / need to ignore commit hooks run `--no-verify` when commiting e.g. `git commit --no-verify -m "skipping commit hooks"`
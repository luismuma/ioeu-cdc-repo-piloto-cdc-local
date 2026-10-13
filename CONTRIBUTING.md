# Contribution guide
```Copyright (c) MAPFRE DevOps Platform```

- [Introduction](CONTRIBUTING.md#introduction)
- [What can be contributed?](CONTRIBUTING.md#what-can-be-contributed)
- [Informing a problem](CONTRIBUTING.md#informing-a-problem)
- [Improving documentation](CONTRIBUTING.md#improving-documentation)
- [Contributing with code](CONTRIBUTING.md#contributing-with-code)
- [Branch strategy](CONTRIBUTING.md#branch-strategy)
- [Review process](CONTRIBUTING.md#review-process)


## Introduction
First, thanks for taking time to contribute to this project. We have tried to make a stable project, correct errors and add new features continuously. You can help us do more. All types of contributions are valued. See below the different ways of contributing. 

Be sure to read this document before making your contribution. It will make it much easier for us.

## What can be contributed?
There are many ways to contribute, from improving documentation, reporting problems detected or fixing them.


## Informing a problem
The easiest way to contribute is to report an error detected in the "ISSUES" section. Enter a short title and, in the space for comments, add a small description of it. Remember that you can add images, such as screenshots, dragging the image about the editor.


## Improving documentation
One of the objectives of this repository is that it is accessible and easy to edit for people without technical profile. This guide, for example, collects how to make modifications directly from the web. However, the content is not perfect and surely there are better ways to explain something or issues that could be clarified with an image.

Improving documents [README.md](README.md) and [CONTRIBUTING.md](CONTRIBUTING.md) is also a useful way to contribute.


## Contributing with code

Contributing to a MAPFRE project in GitHub is very simple. 
1. Create a feature branch from the branch `develop`. For example: `feature/title-of-your-feature`.
2. Make the changes in the corresponding files and commit/push them.
3. Create a pull request to the branch `develop`.

Then, wait for the project maintainers to review it. We will let you know if there is any problem or any change that should be considered.


## Branch strategy
This repository follows the MAPFRE guideline for simplified `GitHub Flow`, that is:
- There is a branch `main` with the production version of the code.
- There is a branch `develop` where new features, modifications or bug fixes are integrated. In this way, all branches for new functionalities or corrections must get out of this branch and when the changes are finished, they are integrated back here using a _pull request_.
- The `feature/*` branches are created always from `develop` to add new functionalities or modifying existing ones. It is also used for non-urgent bug fixes.
- The `hotfix/*` branches are used to work on urgent bug fixes and provide a fast path to production. When the work is finished, it must be merged into `main` with one _pull request_. Afterwards, you have to carry these changes back to `develop` with a _downstream pull request_.


## Review process

When you propose any changes, a _pull request_ must be created with the proposed changes, which will also open a conversation to discuss the same. The team will review the request and GitHub will notify you by email as new comments are added.

Pay attention to the changes that we may have asked to you and discuss them in a kind and civic manner, specially with those that you do not agree, adding your own comments. (You can use the [Markdown GitHub variant](https://guides.github.com/features/mastering-markdown/) for it).

Remember that you can edit the file directly in the browser, standing on the page of the _pull request_, go to "_Files changed_", and clicking on the button "_Change this file using the online editor_" that is represented with the pencil icon.

Solving the problems related with a _pull request_ may not be the final step. The review process can be iterated until we are comfortable with it. Pay attention to the successive reviews and please take into consideration that this may take some time, as we are also busy with our everyday tasks.

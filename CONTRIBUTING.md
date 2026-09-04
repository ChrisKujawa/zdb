# Contributing to ZDB

> Copied from https://github.com/camunda-cloud/zeebe/blob/main/CONTRIBUTING.md

* [Build from source](#build-from-source)
* [Report issues or contact developers](#report-issues-or-contact-developers)
* [GitHub Issue Guidelines](#github-issue-guidelines)
  * [Starting on an Issue](#starting-on-an-issue)
  * [Creating a Pull Request](#creating-a-pull-request)
  * [Reviewing a Pull Request](#reviewing-a-pull-request)
  * [Review Emoji Code](#review-emoji-code)
  * [Backporting changes](#backporting-changes)
  * [Commit Message Guidelines](#commit-message-guidelines)
* [Contributor License Agreement](#contributor-license-agreement)
* [Licenses](#licenses)
* [Code of Conduct](#code-of-conduct)

## Golden file tests

ZDB integration tests for each supported Zeebe version use a pre-committed snapshot and golden
files instead of a live Docker container. This makes the test suite fast (no Docker dependency)
and deterministic (keys, ordering, and timestamps are frozen in the snapshot).

**Running the tests:**

```bash
mvn test -pl backend -DskipTests=false
```

**Updating golden files after a ZDB output change:**

When a ZDB behavior change causes golden tests to fail and the new output is correct:

```bash
# Regenerate all golden files for v8.8
mvn test -pl backend -DskipTests=false -DupdateGoldens=true -Dtest=Version88GoldenTest

# To regenerate all versions at once:
mvn test -pl backend -DskipTests=false -DupdateGoldens=true \
  '-Dtest=Version81GoldenTest,Version82GoldenTest,Version83GoldenTest,Version84GoldenTest,Version85GoldenTest,Version86GoldenTest,Version87GoldenTest,Version88GoldenTest'

# Review what changed
git diff backend/src/test/resources/golden/

# Commit if the diff looks right
git add backend/src/test/resources/golden/ && git commit -m "test: update golden files"
```

**Regenerating a snapshot (rare - only needed when Zeebe changes its on-disk format):**

```bash
# Override the default surefire.excludedGroups to un-exclude snapshot-generator tests
mvn test -pl backend -DskipTests=false '-Dsurefire.excludedGroups=' -Dtest=SnapshotGeneratorV88Test
git add backend/src/test/resources/zeebe-states/v8.8.zip && git commit -m "test: regenerate v8.8 snapshot"
# Then regenerate golden files as above
```

**Adding a new Zeebe version:**

1. Copy `SnapshotGeneratorV88Test` to `SnapshotGeneratorV89Test`, update the Docker image tag
2. Run the generator: `mvn test -pl backend -DskipTests=false '-Dsurefire.excludedGroups=' -Dtest=SnapshotGeneratorV89Test`
3. Copy `Version88GoldenTest` to `Version89GoldenTest`, update paths to `v8.9`
4. Seed golden files: `mvn test -pl backend -DskipTests=false -DupdateGoldens=true -Dtest=Version89GoldenTest`
5. Commit the new snapshot and golden files

## Build from source

ZDB is a multi-module maven project. Partly written in Kotlin. To build all components,
run the command: `mvn clean install -DskipTests` in the root folder.

The resulting ZDB cli can be found in the folder `cli/target`, i.e.

```
cli/target/cli-X.Y.Z-SNAPSHOT-jar-with-dependencies.jar
```

## Releasing from GitHub

Use the **Release** workflow in the GitHub Actions UI to publish a new version.
Enter the release version without `-SNAPSHOT`, for example `2.7.0`. The workflow
expects `main` to currently be on that matching snapshot version, for example
`2.7.0-SNAPSHOT`.

The workflow builds the release artifacts, publishes the Docker image, commits the
release version, tags it, commits the next `-SNAPSHOT` version, pushes both commits
and the tag, and creates the GitHub release with the CLI and frontend jars attached.
If the next development version is left empty, the workflow increments the patch
version automatically.

This is a small overview of the contents of the different modules:
- `cli` contains the cli facade
- `backend` contains the backend and the general logic of zdb

## Report issues or contact developers

ZDB uses GitHub issues to organize the development process. If you want to
report a bug or request a new feature feel free to open a new issue on
[GitHub][issues].

If you are reporting a bug, please help to speed up problem diagnosis by
providing as much information as possible. Ideally, that would include a small
[sample project][sample] that reproduces the problem.

If you have a general usage question please ask on the [forum][] or [slack][] channel.

## GitHub Issue Guidelines

Every issue should have a meaningful name and a description which either
describes:
- a new feature with details about the use case the feature would solve or
improve
- a problem, how we can reproduce it and what would be the expected behavior
- a change and the intention how this would improve the system

## Starting on an issue

The `main` branch contains the current in-development state of the project. To work on an issue,
follow the following steps:

1. Check that a [GitHub issue][issues] exists for the task you want to work on.
   If one does not, create one. Refer to the [issue guidelines](#github-issue-guidelines).
2. Checkout the `main` branch and pull the latest changes.

   ```
   git checkout main
   git pull
   ```
3. Create a new branch with the naming scheme `issueId-description`.

   ```
   git checkout -b 123-adding-bpel-support`
   ```
4. Follow the [Google Java Format](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides)
   and [Zeebe Code Style](https://github.com/zeebe-io/zeebe/wiki/Code-Style) while coding.
5. Implement the required changes on your branch and regularly push your
   changes to the origin so that the CI can run. Code formatting, style and
   license header are fixed automatically by running maven. Checkstyle
   violations have to be fixed manually.

   ```
   git commit -am 'feat(broker): bpel support'
   git push -u origin 123-adding-bpel-support
   ```
6. If you think you finished the issue please prepare the branch for reviewing.
   Please consider our [pull requests and code
   reviews](https://github.com/camunda-cloud/zeebe/wiki/Pull-Requests-and-Code-Reviews)
   guide, before requesting a review. In general the commits should be squashed
   into meaningful commits with a helpful message. This means cleanup/fix etc
   commits should be squashed into the related commit. If you made refactorings
   it would be best if they are split up into another commit. Rule of thumb is
   that you should think about how a reviewer can best understand your changes.
   Please follow the [commit message guidelines](#commit-message-guidelines).
7. After finishing up the squashing force push your changes to your branch.

   ```
   git push --force-with-lease
   ```

## Creating a pull request

Before opening your first pull request, please have a look at this [guide](https://github.com/camunda-cloud/zeebe/wiki/Pull-Requests-and-Code-Reviews#pull-requests).

1. To start the review process create a new pull request on GitHub from your
   branch to the `main` branch. Give it a meaningful name and describe
   your changes in the body of the pull request. Lastly add a link to the issue
   this pull request closes, i.e. by writing in the description `closes #123`
2. Assign the pull request to one developer to review, if you are not sure who
   should review the issue skip this step. Someone will assign a reviewer for
   you.
3. The reviewer will look at the pull request in the following days and give
   you either feedback or accept the changes. Your reviewer might use
   [emoji code](#review-emoji-code) during the reviewing process.
   1. If there are changes requested address them in a new commit. Notify the
      reviewer in a comment if the pull request is ready for review again. If
      the changes are accepted squash them again in the related commit and force push.
   2. If no changes are requested the reviewer will initiate a merge

## Reviewing a pull request

Before doing your first review, please have a look at this [guide](https://github.com/camunda-cloud/zeebe/wiki/Pull-Requests-and-Code-Reviews#code-reviews).

As a reviewer, you are encouraged to use the following [emoji code](#review-emoji-code) in your comments.

The review should result in:
- approving the changes if there are only optional suggestions/minor issues 🔧, throughts 💭, or likes 👍
- requesting changes if there are major issues ❌
- commenting if there are open questions ❓

### Review emoji code

The following emojis can be used in a review to express the intention of a comment.
For example, to distinguish a required change from an optional suggestion.

- 👍 or `:+1:`: This is great! It always feels good when somebody likes your work. Show them!
- ❓ or `:question:`: I have a question. Please clarify.
- ❌ or `:x:`: This has to change. It’s possibly an error or strongly violates existing conventions.
- 🔧 or `:wrench:`: This is a well-meant suggestion or minor issue. Take it or leave it. Nothing major that blocks merging.
- 💭 or `:thought_balloon:`: I’m just thinking out loud here. Something doesn’t necessarily have to change, but I want to make sure to share my thoughts.

_Inspired by [Microsofts emoji code](https://devblogs.microsoft.com/appcenter/how-the-visual-studio-mobile-center-team-does-code-review/#introducing-the-emoji-code)._

## Commit Message Guidelines

Commit messages use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/#summary) format.

```
<header>
<BLANK LINE>
<body>
<BLANK LINE> (optional - mandatory with footer)
<footer> (optional)
```


### Commit message header

Examples:

* `docs: add start event to bpmn symbol support matrix`
* `perf: reduce latency in backpressure`
* `feat: allow more than 9000 jobs in a single call`

The commit header should match the following pattern:

```
%{type}(%{scope}): %{description}
```

The commit header should be kept short, preferably under 72 chars but we allow a max of 120 chars.

- `type` should be one of:
  - `build`: Changes that affect the build system (e.g. Maven, Docker, etc)
  - `ci`: Changes to our CI configuration files and scripts (e.g. Jenkins, Bors, etc)
  - `deps`: A change to the external dependencies (was already used by Dependabot)
  - `docs`:  A change to the documentation
  - `feat`: A new feature (both internal or user-facing)
  - `fix`: A bug fix (both internal or user-facing)
  - `perf`: A code change that improves performance
  - `refactor`: A code change that does not change the behavior
  - `style`: A change to align the code with our style guide
  - `test`: Adding missing tests or correcting existing tests
- `scope` (optional): name of the changed component (e.g. `engine`, `journal`, `README`)
- `description`: short description of the change in present tense

### Commit message body

Should describe the motivation for the change.

## Contributor License Agreement

You will be asked to sign our Contributor License Agreement when you open a Pull Request. We are not
asking you to assign copyright to us, but to give us the right to distribute
your code without restriction. We ask this of all contributors in order to
assure our users of the origin and continuing existence of the code. You only
need to sign the CLA once.

## Licenses

Zeebe source files are made available under the [Zeebe Community License
Version 1.1](/licenses/ZEEBE-COMMUNITY-LICENSE-1.1.txt) except for the parts listed
below, which are made available under the [Apache License, Version
2.0](/licenses/APACHE-2.0.txt).  See individual source files for details.

Available under the [Apache License, Version 2.0](/licenses/APACHE-2.0.txt):
- Cli

If you would like to contribute something, or simply want to hack on the code
this document should help you get started.

## Code of Conduct

This project adheres to the [Camunda Code of Conduct](https://camunda.com/events/code-conduct/).
By participating, you are expected to uphold this code. Please [report](https://camunda.com/events/code-conduct/reporting-violations/)
unacceptable behavior as soon as possible.

[issues]: https://github.com/zeebe-io/zeebe/issues
[forum]: https://forum.zeebe.io/
[slack]: https://zeebe-slack-invite.herokuapp.com/
[sample]: https://github.com/zeebe-io/zeebe-test-template-java
[status]: https://github.com/zeebe-io/zeebe/labels?q=Type
[planned]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Enhancement
[ready]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Maintenance
[in progress]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Bug
[needs review]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Docs
[type]: https://github.com/zeebe-io/zeebe/labels?q=Type
[enhancement]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Enhancement
[maintenance]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Maintenance
[bug]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Bug
[docs]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Docs
[question]: https://github.com/zeebe-io/zeebe/labels/Type%3A%20Question
[scope]: https://github.com/zeebe-io/zeebe/labels?q=Scope
[broker]: https://github.com/zeebe-io/zeebe/labels/Scope%3A%20broker
[clients/java]: https://github.com/zeebe-io/zeebe/labels/Scope%3A%20clients%2Fjava
[clients/go]: https://github.com/zeebe-io/zeebe/labels/Scope%3A%20clients%2Fgo

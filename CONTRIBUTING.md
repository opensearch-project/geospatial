- [Contributing to OpenSearch](#contributing-to-opensearch)
- [First Things First](#first-things-first)
- [Ways to Contribute](#ways-to-contribute)
  - [Bug Reports](#bug-reports)
  - [Feature Requests](#feature-requests)
  - [Documentation Changes](#documentation-changes)
  - [Contributing Code](#contributing-code)
- [Developer Certificate of Origin](#developer-certificate-of-origin)
- [Changelog](#changelog)
- [Review Process](#review-process)

## Contributing to OpenSearch

OpenSearch is a community project that is built and maintained by people just like **you**. We're glad you're interested in helping out. There are several different ways you can do it, but before we talk about that, let's talk about how to get started.

## First Things First

1. **When in doubt, open an issue** - For almost any type of contribution, the first step is opening an issue. Even if you think you already know what the solution is, writing down a description of the problem you're trying to solve will help everyone get context when they review your pull request. If it's truly a trivial change (e.g. spelling error), you can skip this step -- but as the subject says, when it doubt, [open an issue](https://github.com/opensearch-project/geospatial/issues).

2. **Only submit your own work**  (or work you have sufficient rights to submit) - Please make sure that any code or documentation you submit is your work or you have the rights to submit. We respect the intellectual property rights of others, and as part of contributing, we'll ask you to sign your contribution with a "Developer Certificate of Origin" (DCO) that states you have the rights to submit this work and you understand we'll use your contribution. There's more information about this topic in the [DCO section](#developer-certificate-of-origin).

## Ways to Contribute

### Bug Reports

Ugh! Bugs!

A bug is when software behaves in a way that you didn't expect and the developer didn't intend. To help us understand what's going on, we first want to make sure you're working from the latest version.

Once you've confirmed that the bug still exists in the latest version, you'll want to check to make sure it's not something we already know about on the [open issues GitHub page](https://github.com/opensearch-project/geospatial/issues).

If you've upgraded to the latest version and you can't find it in our open issues list, then you'll need to tell us how to reproduce it Provide as much information as you can. You may think that the problem lies with your query, when actually it depends on how your data is indexed. The easier it is for us to recreate your problem, the faster it is likely to be fixed.

### Feature Requests

If you've thought of a way that OpenSearch could be better, we want to hear about it. We track feature requests using GitHub, so please feel free to open an issue which describes the feature you would like to see, why you need it, and how it should work.

### Documentation Changes

TODO

### Contributing Code

As with other types of contributions, the first step is to [open an issue on GitHub](https://github.com/opensearch-project/geospatial/issues). Opening an issue before you make changes makes sure that someone else isn't already working on that particular problem. It also lets us all work together to find the right approach before you spend a bunch of time on a PR. So again, when in doubt, open an issue.

## Developer Certificate of Origin

OpenSearch is an open source product released under the Apache 2.0 license (see either [the Apache site](https://www.apache.org/licenses/LICENSE-2.0) or the [LICENSE file](LICENSE)). The Apache 2.0 license allows you to freely use, modify, distribute, and sell your own products that include Apache 2.0 licensed software.

We respect intellectual property rights of others and we want to make sure all incoming contributions are correctly attributed and licensed. A Developer Certificate of Origin (DCO) is a lightweight mechanism to do that.

The DCO is a declaration attached to every contribution made by every developer. In the commit message of the contribution, the developer simply adds a `Signed-off-by` statement and thereby agrees to the DCO, which you can find below or at [DeveloperCertificate.org](http://developercertificate.org/).

```
Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the
    best of my knowledge, is covered under an appropriate open
    source license and I have the right under that license to
    submit that work with modifications, whether created in whole
    or in part by me, under the same open source license (unless
    I am permitted to submit under a different license), as
    Indicated in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including
    all personal information I submit with it, including my
    sign-off) is maintained indefinitely and may be redistributed
    consistent with this project or the open source license(s)
    involved.
 ```

We require that every contribution to OpenSearch is signed with a Developer Certificate of Origin. Additionally, please use your real name. We do not accept anonymous contributors nor those utilizing pseudonyms.

Each commit must include a DCO which looks like this

```
Signed-off-by: Jane Smith <jane.smith@email.com>
```

You may type this line on your own when writing your commit messages. However, if your user.name and user.email are set in your git configs, you can use `-s` or `– – signoff` to add the `Signed-off-by` line to the end of the commit message.

## Changelog

OpenSearch maintains version specific changelog by enforcing a change to the ongoing [CHANGELOG](CHANGELOG.md) file adhering to the [Keep A Changelog](https://keepachangelog.com/en/1.0.0/) format. The purpose of the changelog is for the contributors and maintainers to incrementally build the release notes throughout the development process to avoid a painful and error-prone process of attempting to compile the release notes at release time. On each release the "unreleased" entries of the changelog are moved to the appropriate release notes document in the `./release-notes` folder. Also, incrementally building the changelog provides a concise, human-readable list of significant features that have been added to the unreleased version under development.

### Which changes require a CHANGELOG entry?
Changelogs are intended for operators/administrators, developers integrating with libraries and APIs, and end-users interacting with OpenSearch Dashboards and/or the REST API (collectively referred to as "user"). In short, any change that a user of OpenSearch might want to be aware of should be included in the changelog. The changelog is _not_ intended to replace the git commit log that developers of OpenSearch itself rely upon. The following are some examples of changes that should be in the changelog:

- A newly added feature
- A fix for a user-facing bug
- Dependency updates
- Fixes for security issues

The following are some examples where a changelog entry is not necessary:

- Adding, modifying, or fixing tests
- An incremental PR for a larger feature (such features should include _one_ changelog entry for the feature)
- Documentation changes or code refactoring
- Build-related changes

Any PR that does not include a changelog entry will result in a failure of the validation workflow in GitHub. If the contributor and maintainers agree that no changelog entry is required, then the `skip-changelog` label can be applied to the PR which will result in the workflow passing.

### How to add my changes to [CHANGELOG](CHANGELOG.md)?

Adding in the change is two step process:
1. Add your changes to the corresponding section within the CHANGELOG file with dummy pull request information, publish the PR
2. Update the entry for your change in [`CHANGELOG.md`](CHANGELOG.md) and make sure that you reference the pull request there.

### Where should I put my CHANGELOG entry?
Please review the [branching strategy](https://github.com/opensearch-project/.github/blob/main/RELEASING.md#opensearch-branching) document. The changelog on the `main` branch will contain sections for the _next major_ and _next minor_ releases. Your entry should go into the section it is intended to be released in. In practice, most changes to `main` will be backported to the next minor release so most entries will likely be in that section.

The following examples assume the _next major_ release on main is 3.0, then _next minor_ release is 2.5, and the _current_ release is 2.4.

- **Add a new feature to release in next minor:** Add a changelog entry to `[Unreleased 2.x]` on main, then backport to 2.x (including the changelog entry).
- **Introduce a breaking API change to release in next major:** Add a changelog entry to `[Unreleased 3.0]` on main, do not backport.
- **Upgrade a dependency to fix a CVE:** Add a changelog entry to `[Unreleased 2.x]` on main, then backport to 2.x (including the changelog entry), then backport to 2.4 and ensure the changelog entry is added to `[Unreleased 2.4.1]`.

## Review Process

We deeply appreciate everyone who takes the time to make a contribution. We will review all contributions as quickly as possible. As a reminder, [opening an issue](https://github.com/opensearch-project/geospatial/issues) discussing your change before you make it is the best way to smooth the PR process. This will prevent a rejection because someone else is already working on the problem, or because the solution is incompatible with the architectural direction.

During the PR process, expect that there will be some back-and-forth. Please try to respond to comments in a timely fashion, and if you don't wish to continue with the PR, let us know. If a PR takes too many iterations for its complexity or size, we may reject it. Additionally, if you stop responding we may close the PR as abandoned. In either case, if you feel this was done in error, please add a comment on the PR.

If we accept the PR, a [maintainer](MAINTAINERS.md) will merge your change and usually take care of backporting it to appropriate branches ourselves.

If we reject the PR, we will close the pull request with a comment explaining why. This decision isn't always final: if you feel we have misunderstood your intended change or otherwise think that we should reconsider then please continue the conversation with a comment on the PR and we'll do our best to address any further points you raise.

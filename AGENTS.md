# Project operating instructions

## Git, CI/CD, and source-of-truth workflow

- Direct pushes to `master` are prohibited.
- Each Linear ticket must be scoped to its own branch.
- Branch names, commit messages, and pull request titles must include the Linear ticket identifier, for example `HOR-123-short-description`, `HOR-123 describe change`, and `HOR-123 — Describe change`.
- Commit to the ticket branch as work progresses and as commits make sense.
- When work is ready for review, open a pull request; do not merge it yourself.
- Pull request descriptions must be valid Markdown with real line breaks, not escaped `\n` text; when using `gh`, write the body to a file and use `--body-file` for both create/edit operations.
- Pull request descriptions should use this structure: `## Summary`, `## Validation`, `## Production impact`, and `## Ticket state`; include concise bullets under each heading and mark non-applicable sections as `None` or `N/A`.
- Workflows that target production writes must only run from merges to `main`, and those merges require user approval.
- Only the user may approve and merge pull requests to `main`.
- A ticket is not considered complete/closable until its branch has been merged to `main` and required workflows have passed.
- The repository is the source of truth for infrastructure state, whether expressed as code or, only when strictly required, Markdown documentation.
- Linear is the source of truth for ticket state, ownership, sequencing, and completion status.


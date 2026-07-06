# Project operating instructions

## Git and ticket workflow

- Direct pushes to `main` are prohibited.
- Each Linear ticket must be scoped to its own branch.
- Branch names, commit messages, and pull request titles must include the Linear ticket identifier, for example `HOR-123-short-description`, `HOR-123 describe change`, and `HOR-123 — Describe change`.
- Commit to the ticket branch as work progresses and as commits make sense.
- When work is ready for review, open a pull request; do not merge it yourself.
- Pull request descriptions must be valid Markdown with real line breaks, not escaped `\n` text; when using `gh`, write the body to a file and use `--body-file` for both create/edit operations.
- Pull request descriptions should use this structure: `## Summary`, `## Validation`, `## Production impact`, and `## Ticket state`; include concise bullets under each heading and mark non-applicable sections as `None` or `N/A`.
- Only the user may approve and merge pull requests to `main`.
- A ticket is not complete until its branch has been merged to `main` and any required external checks have passed.
- The repository is the source of truth for non-secret infrastructure intent and architecture.
- Linear is the source of truth for ticket state, ownership, sequencing, and completion status.

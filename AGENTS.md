# Project operating instructions

## Git and ticket workflow

- Direct pushes to `master` are prohibited.
- Each Linear ticket must be scoped to its own branch.
- Branch names, commit messages, and pull request titles must include the Linear ticket identifier, for example `HOR-123-short-description`, `HOR-123 describe change`, and `HOR-123 — Describe change`.
- Commit to the ticket branch as work progresses and as commits make sense.
- When work is ready for review, open a pull request; do not merge it yourself.
- Pull request descriptions must be valid Markdown with real line breaks, not escaped `\n` text; when using `gh`, write the body to a file and use `--body-file` for both create/edit operations.
- Pull request descriptions should use this structure: `## Summary`, `## Validation`, `## Production impact`, and `## Ticket state`; include concise bullets under each heading and mark non-applicable sections as `None` or `N/A`.
- Only the user may approve and merge pull requests to `master`.
- A ticket is not complete until its branch has been merged to `master` and any required external checks have passed.
- A pull request is not considered addressable-complete (review addressed, ready to merge) until CI is green on the PR's branch. After pushing changes that should resolve review findings or CI failures, re-check that CI passes before declaring the work done or asking the user to merge; if CI is red, fix it before finishing. Never mark a review round or a ticket complete while CI is failing.
- The repository is the source of truth for non-secret infrastructure intent and architecture.
- Linear is the source of truth for ticket state, ownership, sequencing, and completion status.

## Architecture decisions

- Architectural decisions always require explicit user approval **before** implementation — regardless of whether they were covered in the agreed plan or appear to match existing guidelines/docs.
- If a decision is ambiguous or seems to call for a deviation (even a seemingly beneficial one), surface it and get approval first; do not make architectural choices unilaterally.
- "Architectural" includes, non-exhaustively: choosing or changing a datastore, cache, or transport mechanism; cross-service contracts; failure/isolation models; and anything that sets a pattern other tickets will follow. When unsure whether a change is architectural, treat it as architectural and ask.

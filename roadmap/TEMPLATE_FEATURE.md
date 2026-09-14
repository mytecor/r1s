# Feature template

Copy this file into `roadmap/<feature-id>-<feature-slug>/README.md` and fill it in.
`<feature-id>` is the feature number (like `f5`), `<feature-slug>` is a short slug; the directory is
named after the feature file. Example: `roadmap/f5-partition-recovery/README.md`.

Then add a milestone section for the feature in [ROADMAP.md](../ROADMAP.md) and file its tasks in
the same directory using the [task template](./TEMPLATE_TASK.md).

> The feature number is a stable identifier, and the directory is named after the feature file
> (`<feature-id>-<feature-slug>/`). Implementation order comes from explicit dependencies, not from
> the number or a calendar. Start a new feature only for a need the existing verticals do not cover.

---

# F7. Feature name

Corresponds to its milestone in [ROADMAP.md](../ROADMAP.md) — link the milestone anchor, for
example `[milestone N](../ROADMAP.md#f2-rns-transport)`.

**Status:** ⏳ Planned

## Outcome

Describe the observable capability this feature delivers.

## Dependencies

- List prerequisite features or `None`.

## Scope

- List included behavior.

## Completion criteria

- State verifiable acceptance conditions.

## Tasks

- Link one file per task stored in this directory (`<feature-id>-<task-id>-<task-slug>.md`).

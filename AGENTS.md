# AGENTS.md

## Cursor Cloud specific instructions

This repository is **Chargebee's GitHub organization profile repo**. Its only content is the
profile README that renders on the org's GitHub landing page.

- The entire tracked source is `profile/README.md`, plus a top-level `README.md` symlink → `profile/README.md`.
- There is **no application code, no package manager, no build system, no automated tests, and no services** to run. There are no dependencies to install, so the startup update script is a no-op.
- The "product" is the rendered Markdown page. To preview it locally the way GitHub would render it, convert the Markdown to HTML (e.g. `pip3 install markdown` then a short script using the `markdown` module) and open it in a browser. `markdown` is only a preview convenience and is intentionally not part of the repo.
- Editing work here is limited to Markdown content in `profile/README.md`. After edits, verify the Markdown renders correctly rather than running lint/build/test.

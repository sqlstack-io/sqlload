# Docs style guide

These rules apply to every page under `website/src/content/docs/` and to the homepage,
`public/index.html`. Pages that follow them well: `concepts/load-methods.mdx`, `cli/contract.mdx`
and `quickstart.mdx`.

## Write for users

- Document what a user does, sees and must decide: commands, flags, config keys, defaults, limits,
  messages, output, exit codes, and the fix for each problem.
- Leave out how SQLLoad works inside: crate layout, internal types and module names. Include a
  mechanism only when the user must act on it, and then say what to do.
- Document only what is released from `main`. Never describe planned features.
- Never document behaviour you haven't checked in the source or with the real `sqlload` command.
  Paste real output, trimmed where marked `…`.

## Structure

- Lead with what the feature does and a short example: a command, a config block or the output a
  user sees.
- Organise sections around the user's tasks, not around the implementation.
- Put flags, config keys and messages in reference tables. Keep cells short.
- Keep headings that other pages link to. Before renaming or removing one, run
  `grep -rn "<page>#" website/src` and update the links.

## Sentences

- Write short, plain sentences in the present tense. One idea per sentence.
- Use neutral orders-style examples (`shop`, `app`, orders, customers, `example-bucket`).

## No history

- Describe the product as it is. Don't write "now", "no longer", "used to" or "previously". History
  belongs in `CHANGELOG.md`.

## Changing a page

- When a feature changes a page, edit the page into shape: put the new fact in the section it
  belongs to, update tables and examples, and remove what is no longer true.
- Don't repeat a rule that another page owns. Link to it instead.

## Measured figures and comparisons

- State only measured figures, each with its basis: the date, commit, versions, hardware and method.
  Benchmark figures come from `benchmarks/README.md`.
- Show only scenarios where SQLLoad is fastest, as `benchmarks/README.md` publishes them.
- Every claim about another tool cites its own docs or source at a stated version.

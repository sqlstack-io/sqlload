# Website

The SQLLoad website: the homepage at `/` and the documentation at `/docs/`, built as one
[Astro Starlight](https://starlight.astro.build) site. It shares its structure, styling and
components with the SQLQuack, SQLClean and SQLBuild websites.

- `public/index.html` and `public/styles.css`: the homepage, plain static HTML and CSS. Its
  terminal examples are real `sqlload` output on a small orders example, trimmed where marked `…`.
- `src/content/docs/docs/`: the documentation pages (MDX). `src/sidebar.json` holds the sidebar and
  `src/navigation.mjs` the redirects. `STYLE.md` is the style guide for these pages.
- `src/components/`: the page layout, header and page title, replacing Starlight's defaults.
- `public/fonts/`: self-hosted fonts, each with its licence.

The logo uses the SQLLoad icon from `sqlstack-io/sqlstack` (`assets/icons/sqlload.png`); the
footer's SQLStack symbols are unchanged copies from the same repository.

## Domain

No domain is set. Every link is root-relative, so the site works wherever it is served. Set
`SITE_URL` (such as `https://example.com`) at build time for absolute URLs, the sitemap and the
`llms.txt` files; Cloudflare Pages builds use the deployment's own URL (`CF_PAGES_URL`) when
`SITE_URL` is unset. Without either, the build leaves out `llms.txt`.

## Hosting

Cloudflare Pages builds it from this folder: root directory `website`, build command
`npm ci && npm run build`, output directory `dist`, Node from `.nvmrc`.

The Website workflow builds the site and checks its links and content on pull requests that touch
it, and when run by hand. It deploys nothing.

## Commands

Requires Node 22.12 or newer.

```bash
npm install
npm run dev      # live preview
npm run build    # static site in dist/, including the search index
npm run preview  # serve dist/
python3 scripts/check_links.py dist
python3 scripts/check_content.py   # every page in the sidebar; no system SQLLoad does not ship
```

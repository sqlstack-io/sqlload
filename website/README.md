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

The header mark (`public/assets/mark.png`) is the SQLLoad icon cropped tight from
`.github/sqlload-logo-light.png`, so it draws as large as the other product marks; the footer's
SQLStack symbols are unchanged copies from `sqlstack-io/sqlstack`. The favicons (`public/favicon.png`,
`favicon-dark.png` and `favicon.ico`) use the cylinder alone, without the speed lines, so it fills
the square and stays legible at 16px; everywhere else uses the full mark.

## Domain

The site is https://sqlload.io. Cloudflare Pages builds of `main` use it for canonical links, the
sitemap and the `llms.txt` files; preview builds use their own deployment URL (`CF_PAGES_URL`).
`SITE_URL` overrides both. Every link is root-relative, so the site works wherever it is served;
a local build without `SITE_URL` leaves out the sitemap and `llms.txt`.

## Hosting

Cloudflare Pages builds it from this folder: root directory `website`, build command
`npm ci && npm run build`, output directory `dist`, Node from `.nvmrc`.

The Website workflow builds the site and checks its links, content and layout on pull requests
that touch it, and when run by hand. It deploys nothing.

## Commands

Requires Node 22.12 or newer.

```bash
npm install
npm run dev      # live preview
npm run build    # checks content (scripts/check-content.mjs), then builds dist/ with the search index
npm run preview  # serve dist/
python3 scripts/check_links.py dist
node scripts/check-layout.mjs dist   # no docs page wider than its content column; needs Chrome (CHROME_PATH)
```

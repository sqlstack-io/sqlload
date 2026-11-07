import { readFile, writeFile } from 'node:fs/promises';
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';
import { sqlloadDark, sqlloadLight } from './src/code-themes.mjs';
import { redirects, sidebar } from './src/navigation.mjs';

// The site lives at https://sqlload.io. Cloudflare Pages builds of main use it; preview builds use
// their own deployment URL, so their canonical links and llms.txt point at the preview. SITE_URL
// overrides both. Every link in the site is root-relative, so the pages work without a site URL;
// only the sitemap and llms.txt files, which need absolute URLs, are left out.
const site =
	process.env.SITE_URL ||
	(process.env.CF_PAGES_BRANCH === 'main' ? 'https://sqlload.io' : process.env.CF_PAGES_URL) ||
	undefined;

// The homepage is a static file in public/, so Starlight gives it no canonical link and the sitemap
// leaves it out. Add both once the build is done, from the same site URL as the docs pages.
const homepageSiteUrls = {
	name: 'sqlload-homepage-site-urls',
	hooks: {
		'astro:build:done': async ({ dir }) => {
			if (!site) return;
			const home = new URL('/', site).href;
			const index = new URL('index.html', dir);
			const html = await readFile(index, 'utf8');
			const tags = `<link rel="canonical" href="${home}" />\n\t\t<meta property="og:url" content="${home}" />\n\t</head>`;
			await writeFile(index, html.replace('</head>', tags));
			const sitemap = new URL('sitemap-0.xml', dir);
			const xml = await readFile(sitemap, 'utf8');
			await writeFile(sitemap, xml.replace('<url>', `<url><loc>${home}</loc></url><url>`));
		},
	},
};

export default defineConfig({
	site,
	redirects,
	integrations: [
		starlight({
			title: 'SQLLoad',
			description:
				'SQLLoad loads Postgres tables, files and Python sources into Snowflake, Databricks, DuckDB or Parquet files, and shows you how before it changes anything.',
			favicon: '/favicon.png',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/sqlstack-io/sqlload' }],
			// The social preview image. Absolute, since link previews don't resolve relative URLs.
			head: [
				['meta', { property: 'og:image', content: 'https://sqlload.io/assets/og-sqlload-v2.png' }],
				['meta', { property: 'og:image:width', content: '1200' }],
				['meta', { property: 'og:image:height', content: '630' }],
				['meta', { property: 'og:image:alt', content: 'SQLLoad. Load data into your warehouse. Fast. $ sqlload plan. See the plan before any data moves.' }],
				['meta', { name: 'twitter:image', content: 'https://sqlload.io/assets/og-sqlload-v2.png' }],
			].map(([tag, attrs]) => ({ tag, attrs })),
			components: {
				PageFrame: './src/components/PageFrame.astro',
				TwoColumnContent: './src/components/TwoColumnContent.astro',
				Header: './src/components/Header.astro',
				PageTitle: './src/components/PageTitle.astro',
				SiteTitle: './src/components/SiteTitle.astro',
				Pagination: './src/components/Pagination.astro',
				ThemeSelect: './src/components/ThemeSelect.astro',
				Footer: './src/components/Footer.astro',
			},
			customCss: ['./src/styles/fonts.css', './src/styles/sqlload.css'],
			expressiveCode: {
				themes: [sqlloadDark, sqlloadLight],
				defaultProps: {
					// Plain boxes for shell snippets, without the fake terminal title bar.
					overridesByLang: { 'bash,sh,shell,zsh,console,powershell': { frame: 'none' } },
				},
				styleOverrides: {
					borderRadius: '2px',
					borderWidth: '1px',
					codeFontFamily: 'var(--sl-font-mono)',
					codeFontSize: '13px',
					codeLineHeight: '1.65',
					uiFontFamily: 'var(--sl-font)',
					frames: {
						shadowColor: 'transparent',
						inlineButtonBorder: 'transparent',
						inlineButtonBackgroundIdleOpacity: '0',
						inlineButtonBackgroundHoverOrFocusOpacity: '0.08',
						inlineButtonForeground: 'var(--sld-muted)',
						frameBoxShadowCssValue: 'none',
						editorTabBarBackground: 'var(--sld-code-bg)',
						editorActiveTabBackground: 'var(--sld-code-bg)',
						editorActiveTabIndicatorTopColor: 'var(--sld-primary)',
						editorTabBarBorderBottomColor: 'var(--sld-line)',
						terminalTitlebarBackground: 'var(--sld-code-bg)',
						terminalTitlebarBorderBottomColor: 'var(--sld-line)',
						terminalTitlebarDotsOpacity: '0.35',
						terminalBackground: 'var(--sld-code-bg)',
					},
					borderColor: 'var(--sld-line)',
				},
			},
			sidebar,
			plugins: site
				? [
				starlightLlmsTxt({
					projectName: 'SQLLoad',
					description:
						'SQLLoad loads Postgres tables, files (Parquet, CSV, JSON, NDJSON) and Python sources into Snowflake, Databricks, DuckDB or Parquet files. `sqlload plan` shows every table, column, write strategy and route before `apply` and `load` change anything.',
				}),
				]
				: [],
		}),
		homepageSiteUrls,
	],
});

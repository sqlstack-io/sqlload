import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';
import { sqlloadDark, sqlloadLight } from './src/code-themes.mjs';
import { redirects, sidebar } from './src/navigation.mjs';

// The public domain is not chosen yet. SITE_URL (such as https://example.com) sets it; Cloudflare
// Pages builds fall back to the deployment's own URL. Every link in the site is root-relative, so
// the pages work without either; only the llms.txt files, which need absolute URLs, are left out.
const site = process.env.SITE_URL || process.env.CF_PAGES_URL || undefined;

export default defineConfig({
	site,
	redirects,
	integrations: [
		starlight({
			title: 'SQLLoad',
			description:
				'SQLLoad loads Postgres tables, files and Python sources into Snowflake, DuckDB or Parquet files, and shows you how before it changes anything.',
			logo: {
				light: './src/assets/logo-light.png',
				dark: './src/assets/logo-dark.png',
				replacesTitle: true,
			},
			favicon: '/favicon.png',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/sqlstack-io/sqlload' }],
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
						'SQLLoad loads Postgres tables, files (Parquet, CSV, JSON, NDJSON) and Python sources into Snowflake, DuckDB or Parquet files. `sqlload plan` shows every table, column, write strategy and route before `apply` and `load` change anything.',
				}),
				]
				: [],
		}),
	],
});

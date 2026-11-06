// Fail the build when a docs page is missing from the sidebar, or the site names a system SQLLoad
// does not ship. Only the roadmap page may name the planned systems. Runs before every build.
import { readFileSync, readdirSync } from 'node:fs';
import { join, relative } from 'node:path';

const website = new URL('..', import.meta.url).pathname;
const docs = join(website, 'src/content/docs');
const roadmap = join(docs, 'docs/roadmap.mdx');
const unshipped = /\b(BigQuery|Databricks|Redshift|ClickHouse|SQL Server|MySQL|CDC|change data capture)\b/gi;
const planned = new Set(['bigquery', 'databricks', 'cdc', 'change data capture']);

const slugs = (items) =>
	items.flatMap((item) =>
		typeof item === 'string' ? [item] : 'slug' in item ? [item.slug] : slugs(item.items ?? []),
	);
const listed = new Set(slugs(JSON.parse(readFileSync(join(website, 'src/sidebar.json'), 'utf8'))));
const pages = readdirSync(docs, { recursive: true })
	.filter((file) => file.endsWith('.mdx'))
	.sort()
	.map((file) => join(docs, file));

const problems = [];
for (const page of pages) {
	const slug = relative(docs, page).replace(/\.mdx$/, '').replace(/\/index$/, '');
	if (!listed.has(slug)) problems.push(`${relative(website, page)}: not in src/sidebar.json`);
}
for (const path of [...pages, join(website, 'public/index.html')]) {
	readFileSync(path, 'utf8')
		.split('\n')
		.forEach((line, index) => {
			for (const [name] of line.matchAll(unshipped)) {
				if (path === roadmap && planned.has(name.toLowerCase())) continue;
				problems.push(`${relative(website, path)}:${index + 1}: names ${name}, which SQLLoad does not ship`);
			}
		});
}
for (const problem of problems) console.error(problem);
console.log(`${listed.size} sidebar pages, ${problems.length} problems`);
process.exitCode = problems.length ? 1 : 0;

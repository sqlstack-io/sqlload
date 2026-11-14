// Fails when a built docs page is wider than its content column at desktop or phone width: a code
// line, table or signature running past the column, or a page that scrolls sideways. Content in a
// box that scrolls on its own, such as a code block or a wide table, is fine.
//
// Usage: node scripts/check-layout.mjs [dist]. Needs Chrome: CHROME_PATH, else `google-chrome`.
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { extname, join, resolve } from 'node:path';

const dist = resolve(process.argv[2] ?? 'dist');
const widths = [1280, 390];
const types = {
	'.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript', '.mjs': 'text/javascript',
	'.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp',
	'.woff2': 'font/woff2', '.json': 'application/json',
};

// Every docs page, leaving out the redirect pages.
async function docsPages(dir, route = '/docs/') {
	const pages = [];
	for (const entry of await readdir(dir, { withFileTypes: true })) {
		if (entry.isDirectory()) pages.push(...(await docsPages(join(dir, entry.name), `${route}${entry.name}/`)));
		else if (entry.name === 'index.html' && !(await readFile(join(dir, entry.name), 'utf8')).includes('http-equiv="refresh"')) pages.push(route);
	}
	return pages.sort();
}

function serve() {
	const server = createServer(async (req, res) => {
		let path = join(dist, decodeURIComponent(new URL(req.url, 'http://x').pathname));
		if (path.endsWith('/')) path += 'index.html';
		try {
			const body = await readFile(path);
			res.writeHead(200, { 'content-type': types[extname(path)] ?? 'application/octet-stream' }).end(body);
		} catch {
			res.writeHead(404).end();
		}
	});
	return new Promise((done) => server.listen(0, '127.0.0.1', () => done(server)));
}

async function launchChrome() {
	const profile = await mkdtemp(join(tmpdir(), 'check-layout-'));
	const chrome = spawn(process.env.CHROME_PATH || 'google-chrome', [
		'--headless=new', '--no-sandbox', '--disable-gpu', '--no-first-run', '--hide-scrollbars',
		'--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank',
	], { stdio: ['ignore', 'ignore', 'pipe'] });
	const url = await new Promise((done, fail) => {
		let log = '';
		chrome.stderr.on('data', (chunk) => {
			log += chunk;
			const match = log.match(/DevTools listening on (ws:\/\/\S+)/);
			if (match) done(match[1]);
		});
		chrome.on('exit', (code) => fail(new Error(`Chrome exited (${code}):\n${log}`)));
	});
	return { chrome, profile, url };
}

// A minimal Chrome DevTools Protocol client over Node's WebSocket.
async function connect(url) {
	const socket = new WebSocket(url);
	await new Promise((done, fail) => {
		socket.onopen = done;
		socket.onerror = fail;
	});
	let nextId = 0;
	const pending = new Map();
	const listeners = new Set();
	socket.onmessage = ({ data }) => {
		const message = JSON.parse(data);
		if (message.id === undefined) return listeners.forEach((listener) => listener(message));
		const { done, fail } = pending.get(message.id);
		pending.delete(message.id);
		message.error ? fail(new Error(message.error.message)) : done(message.result);
	};
	const send = (method, params = {}, sessionId) =>
		new Promise((done, fail) => {
			const id = ++nextId;
			pending.set(id, { done, fail });
			socket.send(JSON.stringify({ id, method, params, sessionId }));
		});
	const once = (method) =>
		new Promise((done) => {
			const listener = (message) => {
				if (message.method !== method) return;
				listeners.delete(listener);
				done(message.params);
			};
			listeners.add(listener);
		});
	return { send, once, close: () => socket.close() };
}

// Runs in the page. Lists what sticks out of the content column, outermost element only.
async function findOverflow() {
	await document.fonts.ready;
	const problems = [];
	const page = document.documentElement;
	if (page.scrollWidth > page.clientWidth + 1) problems.push(`the page is ${page.scrollWidth}px wide`);
	const column = document.querySelector('.sl-markdown-content');
	if (!column) return problems;
	const { left, right } = column.getBoundingClientRect();
	const reported = [];
	for (const element of column.querySelectorAll('*')) {
		const box = element.getBoundingClientRect();
		if (!box.width || (box.left >= left - 1 && box.right <= right + 1)) continue;
		if (reported.some((outer) => outer.contains(element))) continue;
		let parent = element.parentElement;
		while (parent !== column && getComputedStyle(parent).overflowX === 'visible') parent = parent.parentElement;
		if (parent !== column) continue;
		reported.push(element);
		const text = element.textContent.trim().replace(/\s+/g, ' ').slice(0, 60);
		const past = Math.round(Math.max(box.right - right, left - box.left));
		problems.push(`<${element.tagName.toLowerCase()}> "${text}" runs ${past}px past the content column`);
	}
	return problems;
}

const pages = await docsPages(join(dist, 'docs'));
const server = await serve();
const origin = `http://127.0.0.1:${server.address().port}`;
const { chrome, profile, url } = await launchChrome();
const cdp = await connect(url);
const { targetId } = await cdp.send('Target.createTarget', { url: 'about:blank' });
const { sessionId } = await cdp.send('Target.attachToTarget', { targetId, flatten: true });
const send = (method, params) => cdp.send(method, params, sessionId);
await send('Page.enable');

let failures = 0;
try {
	for (const width of widths) {
		await send('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: false });
		for (const page of pages) {
			const loaded = cdp.once('Page.loadEventFired');
			await send('Page.navigate', { url: origin + page });
			await loaded;
			const { result, exceptionDetails } = await send('Runtime.evaluate', {
				expression: `(${findOverflow})()`,
				awaitPromise: true,
				returnByValue: true,
			});
			const problems = exceptionDetails ? [exceptionDetails.exception?.description ?? exceptionDetails.text] : result.value;
			for (const problem of problems) console.log(`${page} at ${width}px: ${problem}`);
			failures += problems.length;
		}
	}
} finally {
	cdp.close();
	chrome.kill();
	server.close();
	await new Promise((done) => chrome.once('exit', done));
	await rm(profile, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}

console.log(`${pages.length} pages at ${widths.join(' and ')}px, ${failures} layout problems`);
process.exitCode = failures ? 1 : 0;

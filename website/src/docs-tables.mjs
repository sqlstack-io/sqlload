// Keeps docs tables inside the content column, as a Sätteri hast plugin.
// - Each table sits in a box (`.sld-table`) that scrolls sideways when the table can't fit.
// - Code in a table's first column doesn't wrap, so a key or flag never breaks. Code there with
//   spaces, such as a function signature or an error message, may still wrap between its words:
//   each word becomes a span that doesn't wrap.
// - A paragraph starting with a superscript number, the note a table's ¹ points to, is small print
//   (`.sld-note`).
const isElement = (node, tagName) => node?.type === 'element' && node.tagName === tagName;

function firstColumnCell(node, ctx) {
	for (let parent = ctx.parent(node); parent; parent = ctx.parent(parent)) {
		if (isElement(parent, 'pre')) return false;
		if (isElement(parent, 'td') || isElement(parent, 'th')) {
			const row = ctx.parent(parent);
			const first = row.children.findIndex((cell) => cell.type === 'element');
			return ctx.indexOf(parent) === first;
		}
	}
	return false;
}

export const docsTables = {
	name: 'sqlload-docs-tables',
	element: [
		{
			filter: ['table'],
			visit(node, ctx) {
				ctx.wrapNode(node, { type: 'element', tagName: 'div', properties: { className: ['sld-table'] }, children: [] });
			},
		},
		{
			filter: ['p'],
			visit(node, ctx) {
				if (!/^[¹²³⁴⁵⁶⁷⁸⁹]/.test(ctx.textContent(node))) return;
				return { ...node, properties: { ...node.properties, className: [...(node.properties?.className ?? []), 'sld-note'] } };
			},
		},
		{
			filter: ['code'],
			visit(node, ctx) {
				const text = ctx.textContent(node);
				if (!text.includes(' ') || !firstColumnCell(node, ctx)) return;
				return {
					type: 'element',
					tagName: 'code',
					properties: { ...node.properties, className: [...(node.properties?.className ?? []), 'sld-words'] },
					children: text
						.split(/( +)/)
						.filter(Boolean)
						.map((part) =>
							part.trim()
								? { type: 'element', tagName: 'span', properties: {}, children: [{ type: 'text', value: part }] }
								: { type: 'text', value: part },
						),
				};
			},
		},
	],
};

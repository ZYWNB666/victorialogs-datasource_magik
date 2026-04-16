import { SyntaxNode } from '@lezer/common';
import { escapeRegExp } from 'lodash';

import { Filter, FilterOp, LineFilter, OrFilter, parser, PipeExact, PipeMatch, String } from '@grafana/lezer-logql';

export function getNodesFromQuery(query: string, nodeTypes?: number[]): SyntaxNode[] {
  const nodes: SyntaxNode[] = [];
  const tree = parser.parse(query);
  tree.iterate({
    enter: (node): false | void => {
      if (nodeTypes === undefined || nodeTypes.includes(node.type.id)) {
        nodes.push(node.node);
      }
    },
  });
  return nodes;
}

export function getStringsFromLineFilter(filter: SyntaxNode): SyntaxNode[] {
  const nodes: SyntaxNode[] = [];
  let node: SyntaxNode | null = filter;
  do {
    const string = node.getChild(String);
    if (string && !node.getChild(FilterOp)) {
      nodes.push(string);
    }
    node = node.getChild(OrFilter);
  } while (node != null);

  return nodes;
}

const unescapeQueryValue = (value: string): string => {
  return value
    .replace(/\\(["'])/g, '$1')
    .replace(/\\\\/g, '\\');
};

export function getHighlighterExpressionsFromQuery(input = ''): string[] {
  const results = [];

  // Extract _msg filter values for highlighting.
  // Supports escaped quotes inside quoted strings, e.g. _msg:~"\\"start_time".
  const msgFilterRegex = /_msg\s*:\s*(!~|!=|!|~|=)?\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\S+)/gi;
  let match;
  while ((match = msgFilterRegex.exec(input)) !== null) {
    let value = match[2];
    // Remove quotes if present
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (value && value !== '*') {
      // For highlighting, we only highlight "contains" matches, not "not contains"
      // But we still add them for highlighting purposes
      // Escape special regex characters for highlighting (unless it's a regex match)
      const operator = match[1] || '';
      if (operator === '~' || operator === '!~') {
        // Regex pattern - use as-is but unescape
        results.push(unescapeQueryValue(value));
      } else {
        // Exact or word match - unescape and escape regex special chars
        results.push(escapeRegExp(unescapeQueryValue(value)));
      }
    }
  }

  const filters = getNodesFromQuery(input, [LineFilter]);

  for (const filter of filters) {
    const pipeExact = filter.getChild(Filter)?.getChild(PipeExact);
    const pipeMatch = filter.getChild(Filter)?.getChild(PipeMatch);
    const strings = getStringsFromLineFilter(filter);

    if ((!pipeExact && !pipeMatch) || !strings.length) {
      continue;
    }

    for (const string of strings) {
      const filterTerm = input.substring(string.from, string.to).trim();
      const backtickedTerm = filterTerm[0] === '`';
      const unwrappedFilterTerm = filterTerm.substring(1, filterTerm.length - 1);

      if (!unwrappedFilterTerm) {
        continue;
      }

      let resultTerm;

      // Only filter expressions with |~ operator are treated as regular expressions
      if (pipeMatch) {
        resultTerm = backtickedTerm ? unwrappedFilterTerm : unwrappedFilterTerm.replace(/\\\\/g, '\\');
      } else {
        // We need to escape this string so it is not matched as regular expression
        resultTerm = escapeRegExp(unwrappedFilterTerm);
      }

      if (resultTerm) {
        results.push(resultTerm);
      }
    }
  }
  return results;
}

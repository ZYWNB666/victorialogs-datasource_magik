import { FilterVisualQuery, LineFilterType, MsgFilterCondition, VisualQuery } from '../../../../types';

import { BUILDER_OPERATORS, isEmptyQuery } from './parsing';

interface Context {
  query: VisualQuery;
  errors: string[];
}

type ParsedExpression = string | ParsedExpression[];

export const buildVisualQueryFromString = (expr: string): Context => {
  const visQuery: VisualQuery = {
    filters: { operators: [], values: [] },
    pipes: [],
    msgFilters: []
  };

  const context: Context = {
    query: visQuery,
    errors: [],
  };

  try {
    const { filters, pipes, msgFilters } = handleExpression(expr);
    visQuery.filters = filters;
    visQuery.pipes = pipes;
    visQuery.msgFilters = msgFilters;
  } catch (err) {
    console.error(err);
    if (err instanceof Error) {
      context.errors.push(err.message);
    }
  }

  // If we have empty query, we want to reset errors
  if (isEmptyQuery(context.query)) {
    context.errors = [];
  }

  return context;
};

const handleExpression = (expr: string) => {
  const [filterStrPart, ...pipeParts] = expr.split('|').map(part => part.trim());
  const { filters, msgFilters } = parseStringToFilterVisualQuery(filterStrPart);
  return { filters, pipes: pipeParts, msgFilters };
};

export const splitExpression = (expr: string): string[] => {
  return expr.split('|').map(part => part.trim());
};

const parseStringToFilterVisualQuery = (expression: string): { filters: FilterVisualQuery; msgFilters: MsgFilterCondition[] } => {
  const parsedExpressions = parseExpression(expression);
  const msgFilters: MsgFilterCondition[] = [];

  const groupFilterQuery = (parts: ParsedExpression[]): FilterVisualQuery => {
    const filter: FilterVisualQuery = {
      values: [],
      operators: [],
    };

    const parsePart = (part: ParsedExpression, _index: number) => {
      if (!part) {
        return;
      }
      if (typeof part === 'string') {
        if (BUILDER_OPERATORS.includes(part.toUpperCase())) {
          filter.operators.push(part);
        } else {
          const parsedParts = parseStringPart(part);
          // Extract _msg filters and treat them as msgFilters
          for (const p of parsedParts) {
            // Check if this is a _msg filter: _msg:value or _msg:~value or _msg:=value etc.
            // Also support negation: _msg:!value, _msg:!~value (not contains)
            // Operators: ! (not contains), !~ (not regex), ~ (regex), = or empty (contains)
            const msgMatch = p.match(/^_msg\s*:\s*(!~|!|~|=)?\s*("[^"]*"|'[^']*'|\S+)?/i);
            if (msgMatch) {
              const operator = msgMatch[1] || '';
              let value = msgMatch[2] || '';
              // Remove quotes if present
              if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
                value = value.slice(1, -1);
              }
              
              // Determine the filter type based on operator and value
              let type: LineFilterType;
              const contains = operator !== '!~' && operator !== '!';
              
              // Check for case insensitive pattern: (?i) prefix
              const isCaseInsensitive = value.startsWith('(?i)');
              const cleanValue = isCaseInsensitive ? value.slice(4) : value;
              
              if (operator === '!~') {
                // Not regex match
                if (isCaseInsensitive) {
                  type = LineFilterType.NotContainsCaseInsensitive;
                } else {
                  type = LineFilterType.RegexNotMatch;
                }
              } else if (operator === '!') {
                // Not contains (exact)
                type = LineFilterType.NotContains;
              } else if (operator === '~') {
                // Regex match
                if (isCaseInsensitive) {
                  type = LineFilterType.ContainsCaseInsensitive;
                } else {
                  type = LineFilterType.RegexMatch;
                }
              } else {
                // Default contains (operator is '' or '=')
                type = LineFilterType.Contains;
              }
              
              if (cleanValue && cleanValue !== '*') {
                msgFilters.push({ text: cleanValue, type, contains });
              }
            } else {
              filter.values.push(p);
            }
          }
        }
      } else {
        filter.values.push(groupFilterQuery(part));
      }
    };
    parts.forEach(parsePart);

    return filter;
  };

  return { filters: groupFilterQuery(parsedExpressions), msgFilters };
};

const splitByTopLevelParentheses = (input: string) => {
  const result = [];
  let level = 0;
  let current = '';
  let inDoubleQuote = false;
  let inSingleQuote = false;

  for (let i = 0; i < input.length; i++) {
    const char = input[i];
    const prevChar = i > 0 ? input[i - 1] : '';

    // Track quote state (ignore escaped quotes)
    if (char === '"' && prevChar !== '\\' && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
      current += char;
    } else if (char === "'" && prevChar !== '\\' && !inDoubleQuote) {
      inSingleQuote = !inSingleQuote;
      current += char;
    } else if (char === '(' && !inDoubleQuote && !inSingleQuote) {
      // Check if it's :in(
      const isListItem = current.trim().endsWith(':in');
      if (level === 0 && current.trim() !== '' && !isListItem) {
        result.push(current.trim());
        current = '';
      }
      level++;
      current += char;
    } else if (char === ')' && !inDoubleQuote && !inSingleQuote) {
      level--;
      current += char;
      if (level === 0) {
        result.push(current.trim());
        current = '';
      }
    } else {
      current += char;
    }
  }

  if (current.trim() !== '') {
    result.push(current.trim());
  }
  const operatorsPattern = BUILDER_OPERATORS.join('|');
  const operatorRegex = new RegExp(`(?:^|\\s)(${operatorsPattern})\\s*(?:$|\\s+)`, 'i');

  const splitPartByOperators = (part: string) => {
    const isTopLevelGroup = part.startsWith('(') && !/\w+:in\(/.test(part);
    return isTopLevelGroup ? part : part.split(operatorRegex);
  };

  return result.flatMap(splitPartByOperators).filter(Boolean);

};

const parseExpression = (input: string): ParsedExpression[] => {
  const parts = splitByTopLevelParentheses(input);

  return parts.map(part => {
    if (part.startsWith('(') && part.endsWith(')') && !/\w+:in\(/.test(part)) {
      // Recursively parse the inner expression, but not for :in()
      return parseExpression(part.slice(1, -1));
    } else {
      return part.trim();
    }
  });
};

const parseStringPart = (expression: string) => {
  // Regex updated to handle :in(...) or simple key:value
  const regex = /("[^"]*"|'[^']*'|\S+)\s*:\s*(in\s*\([^)]*\)|"[^"]*"|'[^']*'|\S+)?|\S+/g;
  const matches = expression.match(regex) || [];
  return matches.map(match => match.trim());
};

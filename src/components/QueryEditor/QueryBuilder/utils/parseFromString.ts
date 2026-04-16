import { splitByPipes } from '../../../../LogsQL/splitByPipes';
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
    msgFilters: [],
    msgFilterOperators: [],
  };

  const context: Context = {
    query: visQuery,
    errors: [],
  };

  try {
    const { filters, pipes, msgFilters, msgFilterOperators } = handleExpression(expr);
    visQuery.filters = filters;
    visQuery.pipes = pipes;
    visQuery.msgFilters = msgFilters;
    visQuery.msgFilterOperators = msgFilterOperators;
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
  const [filterStrPart, ...pipeParts] = splitByPipes(expr).map(part => part.trim());
  const { filters, msgFilters, msgFilterOperators } = parseStringToFilterVisualQuery(filterStrPart);
  return { filters, pipes: pipeParts, msgFilters, msgFilterOperators };
};

export const splitExpression = (expr: string): string[] => {
  return splitByPipes(expr).map(part => part.trim());
};

const normalizeFilterOperator = (operator: string) => {
  const upperOperator = operator.toUpperCase();
  return BUILDER_OPERATORS.includes(upperOperator) ? operator : 'AND';
};

const parseStringToFilterVisualQuery = (expression: string): { filters: FilterVisualQuery; msgFilters: MsgFilterCondition[]; msgFilterOperators: string[] } => {
  const parsedExpressions = parseExpression(expression);
  const msgFilters: MsgFilterCondition[] = [];
  const msgFilterOperators: string[] = [];

  const normalizeMsgOperator = (operator: string) => {
    const upperOperator = operator.toUpperCase();
    return BUILDER_OPERATORS.includes(upperOperator) ? upperOperator : 'AND';
  };

  const groupFilterQuery = (parts: ParsedExpression[]): FilterVisualQuery => {
    const filter: FilterVisualQuery = {
      values: [],
      operators: [],
    };

    let pendingOperator: string | null = null;
    let previousTokenType: 'msg' | 'field' | null = null;

    const parsePart = (part: ParsedExpression) => {
      if (!part) {
        return;
      }

      if (typeof part === 'string') {
        if (BUILDER_OPERATORS.includes(part.toUpperCase())) {
          pendingOperator = part;
          return;
        }

        const parsedParts = parseStringPart(part);

        for (const p of parsedParts) {
          if (BUILDER_OPERATORS.includes(p.toUpperCase())) {
            pendingOperator = p;
            continue;
          }

          // Check if this is a _msg filter: _msg:value or _msg:~value or _msg:=value etc.
          // Also support negation: _msg:!value, _msg:!~value (not contains)
          // Operators: ! (not contains), !~ (not regex), ~ (regex), = or empty (contains)
          const msgMatch = p.match(/^_msg\s*:\s*(!~|!|~|=)?\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\S+)?/i);
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
              if (previousTokenType === 'msg' && pendingOperator) {
                msgFilterOperators.push(normalizeMsgOperator(pendingOperator));
              }
              msgFilters.push({ text: cleanValue, type, contains });
              previousTokenType = 'msg';
              pendingOperator = null;
            }
          } else {
            if (previousTokenType === 'field' && pendingOperator) {
              filter.operators.push(normalizeFilterOperator(pendingOperator));
            }
            filter.values.push(p);
            previousTokenType = 'field';
            pendingOperator = null;
          }
        }
      } else {
        if (previousTokenType === 'field' && pendingOperator) {
          filter.operators.push(normalizeFilterOperator(pendingOperator));
        }
        filter.values.push(groupFilterQuery(part));
        previousTokenType = 'field';
        pendingOperator = null;
      }
    };

    parts.forEach(parsePart);

    return filter;
  };

  return { filters: groupFilterQuery(parsedExpressions), msgFilters, msgFilterOperators };
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
  // Handles key:value pairs where value can be prefixed with !~|!|~|= and optionally quoted with spaces.
  // Supports escaped quotes inside quoted values.
  // Examples: _msg:~"\"start_time|Use Proxy:", field:"value with spaces", key:in("a","b")
  const regex = /("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\S+)\s*:\s*((?:!~|!|~|=)?\s*(?:in\s*\([^)]*\)|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|\S+))?|\S+/g;
  const matches = expression.match(regex) || [];
  return matches.map(match => match.trim());
};

import { FilterVisualQuery, LineFilterType, MsgFilterCondition, VisualQuery } from '../../../../types';

export const DEFAULT_FILTER_OPERATOR = 'AND';
export const DEFAULT_FIELD = 'namespace';

export const filterVisualQueryToString = (
  query: FilterVisualQuery,
  finishedOnly = false
): string => {
  // Convert every value (recursively for nested queries)
  const valueStrings = query.values.map(v =>
    typeof v === 'string' ? v.trim() : `(${filterVisualQueryToString(v, finishedOnly)})`
  );

  const operatorStrings = query.operators.map(op => op.trim());

  let output = '';
  for (let i = 0; i < valueStrings.length; i++) {
    const val = valueStrings[i];
    const isValidValue = /^.+:[~=]?.+$/.test(val); // field:value | field:~value | field:=value

    if (finishedOnly && !isValidValue) {break;}
    if (!val) {continue;} // ignore empty strings from nested calls

    if (i > 0) {
      const op = operatorStrings[i - 1] || DEFAULT_FILTER_OPERATOR;
      output += ` ${op} `;
    }
    output += val;
  }

  return output;
};

const normalizeOperator = (operator?: string): string => {
  if (!operator) {
    return DEFAULT_FILTER_OPERATOR;
  }

  const upperOperator = operator.toUpperCase();
  return upperOperator === 'OR' ? 'OR' : DEFAULT_FILTER_OPERATOR;
};

const escapeMsgFilterText = (text: string): string => {
  let escaped = '';

  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char !== '"') {
      escaped += char;
      continue;
    }

    let precedingBackslashes = 0;
    for (let j = i - 1; j >= 0 && text[j] === '\\'; j--) {
      precedingBackslashes++;
    }

    if (precedingBackslashes % 2 === 0) {
      escaped += '\\';
    }

    escaped += char;
  }

  return escaped;
};

const msgFilterConditionToString = (condition: MsgFilterCondition): string | null => {
  if (!condition.text || condition.text.trim() === '' || condition.text.trim() === '*') {
    return null;
  }

  const text = escapeMsgFilterText(condition.text);

  // Support backward compatibility with old boolean 'contains' field
  if (condition.contains !== undefined && !condition.type) {
    if (condition.contains) {
      return `_msg:"${text}"`;
    }
    return `_msg!~"${text}"`;
  }

  // Handle different filter types
  switch (condition.type) {
    case LineFilterType.Contains:
      // Line contains (case sensitive): _msg:"text"
      return `_msg:"${text}"`;

    case LineFilterType.NotContains:
      // Line does not contain (case sensitive): _msg:!"text"
      return `_msg:!"${text}"`;

    case LineFilterType.ContainsCaseInsensitive:
      // Line contains case insensitive: _msg:~"(?i)text"
      return `_msg:~"(?i)${text}"`;

    case LineFilterType.NotContainsCaseInsensitive:
      // Line does not contain case insensitive: _msg:!~"(?i)text"
      return `_msg:!~"(?i)${text}"`;

    case LineFilterType.RegexMatch:
      // Line contains regex match: _msg:~"regex"
      return `_msg:~"${text}"`;

    case LineFilterType.RegexNotMatch:
      // Line does not match regex: _msg:!~"regex"
      return `_msg:!~"${text}"`;

    case LineFilterType.IpFilter:
      // IP line filter expression: _msg:ip("ip_range")
      return `_msg:ip("${text}")`;

    default:
      // Default to contains for unknown types
      return `_msg:"${text}"`;
  }
};

/** Build _msg filter string from conditions */
const buildMsgFilterString = (msgFilters: MsgFilterCondition[], msgFilterOperators: string[] = []): string => {
  const parts = msgFilters
    .map(condition => msgFilterConditionToString(condition))
    .filter((part): part is string => Boolean(part));

  if (parts.length === 0) {
    return '';
  }

  let result = parts[0];
  for (let i = 1; i < parts.length; i++) {
    const operator = normalizeOperator(msgFilterOperators[i - 1]);
    result += ` ${operator} ${parts[i]}`;
  }

  return result;
};

export const parseVisualQueryToString = (query: VisualQuery): string => {
  const pipesPart = query.pipes?.length ? ` | ${query.pipes.join(' | ')}` : '';

  // Build _msg filter part from conditions
  const msgFilterPart = buildMsgFilterString(query.msgFilters || [], query.msgFilterOperators || []);

  const filtersPart = filterVisualQueryToString(query.filters);

  // Combine msgFilter with other filters
  let result = '';
  if (msgFilterPart && filtersPart.trim()) {
    result = `${msgFilterPart} AND ${filtersPart}`;
  } else if (msgFilterPart) {
    result = msgFilterPart;
  } else {
    result = filtersPart;
  }

  return result + pipesPart;
};

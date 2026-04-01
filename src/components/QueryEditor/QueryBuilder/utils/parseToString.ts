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

/** Build _msg filter string from conditions */
const buildMsgFilterString = (msgFilters: MsgFilterCondition[]): string => {
  const parts: string[] = [];
  
  for (const condition of msgFilters) {
    if (!condition.text || condition.text.trim() === '' || condition.text.trim() === '*') {
      continue;
    }
    
    const text = condition.text;
    
    // Support backward compatibility with old boolean 'contains' field
    if (condition.contains !== undefined && !condition.type) {
      if (condition.contains) {
        parts.push(`_msg:"${text}"`);
      } else {
        parts.push(`_msg!~"${text}"`);
      }
      continue;
    }
    
    // Handle different filter types
    switch (condition.type) {
      case LineFilterType.Contains:
        // Line contains (case sensitive): _msg:"text"
        parts.push(`_msg:"${text}"`);
        break;
      
      case LineFilterType.NotContains:
        // Line does not contain (case sensitive): _msg:!"text"
        parts.push(`_msg:!"${text}"`);
        break;
      
      case LineFilterType.ContainsCaseInsensitive:
        // Line contains case insensitive: _msg:~"(?i)text"
        parts.push(`_msg:~"(?i)${text}"`);
        break;
      
      case LineFilterType.NotContainsCaseInsensitive:
        // Line does not contain case insensitive: _msg:!~"(?i)text"
        parts.push(`_msg:!~"(?i)${text}"`);
        break;
      
      case LineFilterType.RegexMatch:
        // Line contains regex match: _msg:~"regex"
        parts.push(`_msg:~"${text}"`);
        break;
      
      case LineFilterType.RegexNotMatch:
        // Line does not match regex: _msg:!~"regex"
        parts.push(`_msg:!~"${text}"`);
        break;
      
      case LineFilterType.IpFilter:
        // IP line filter expression: _msg:ip("ip_range")
        parts.push(`_msg:ip("${text}")`);
        break;
      
      default:
        // Default to contains for unknown types
        parts.push(`_msg:"${text}"`);
    }
  }
  
  return parts.join(' AND ');
};

export const parseVisualQueryToString = (query: VisualQuery): string => {
  const pipesPart = query.pipes?.length ? ` | ${query.pipes.join(' | ')}` : '';
  
  // Build _msg filter part from conditions
  const msgFilterPart = buildMsgFilterString(query.msgFilters || []);
  
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

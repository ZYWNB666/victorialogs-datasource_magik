import { LineFilterType, VisualQuery } from '../../../../types';

import { parseVisualQueryToString } from './parseToString';

describe('parseVisualQueryToString', () => {
  const baseQuery = (): VisualQuery => ({
    filters: { values: [], operators: [] },
    pipes: [],
    msgFilters: [],
    msgFilterOperators: [],
  });

  it('should join message filters with AND by default', () => {
    const query = baseQuery();
    query.msgFilters = [
      { text: 'error', type: LineFilterType.Contains },
      { text: 'warn', type: LineFilterType.Contains },
    ];

    expect(parseVisualQueryToString(query)).toBe('_msg:"error" AND _msg:"warn"');
  });

  it('should join message filters with explicit OR operator', () => {
    const query = baseQuery();
    query.msgFilters = [
      { text: 'error', type: LineFilterType.Contains },
      { text: 'warn', type: LineFilterType.Contains },
    ];
    query.msgFilterOperators = ['OR'];

    expect(parseVisualQueryToString(query)).toBe('_msg:"error" OR _msg:"warn"');
  });

  it('should keep operators aligned for three message filters', () => {
    const query = baseQuery();
    query.msgFilters = [
      { text: 'error', type: LineFilterType.Contains },
      { text: 'warn', type: LineFilterType.RegexMatch },
      { text: 'debug', type: LineFilterType.NotContains },
    ];
    query.msgFilterOperators = ['OR', 'AND'];

    expect(parseVisualQueryToString(query)).toBe('_msg:"error" OR _msg:~"warn" AND _msg:!"debug"');
  });

  it('should combine message filters and normal filters', () => {
    const query = baseQuery();
    query.msgFilters = [{ text: 'error', type: LineFilterType.Contains }];
    query.filters = { values: ['service:"api"'], operators: [] };

    expect(parseVisualQueryToString(query)).toBe('_msg:"error" AND service:"api"');
  });

  it('should escape unescaped double quotes in msg filter text', () => {
    const query = baseQuery();
    query.msgFilters = [{ text: '"POST /tokenize HTTP/1.1" 200', type: LineFilterType.Contains }];

    expect(parseVisualQueryToString(query)).toBe('_msg:"\\"POST /tokenize HTTP/1.1\\" 200"');
  });

  it('should not double-escape already escaped double quotes in msg filter text', () => {
    const query = baseQuery();
    query.msgFilters = [{ text: '\\"start_time|Use Proxy:', type: LineFilterType.RegexMatch }];

    expect(parseVisualQueryToString(query)).toBe('_msg:~"\\"start_time|Use Proxy:"');
  });
});

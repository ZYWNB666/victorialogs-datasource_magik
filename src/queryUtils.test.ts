import { getHighlighterExpressionsFromQuery } from './queryUtils';

describe('getHighlighterExpressionsFromQuery', () => {
  it('extracts regex msg filters containing escaped quotes', () => {
    const expr = '_msg:~"\\"start_time" AND _msg:"915f2c6d-a05d-4132-9670-c769adb24d6c" AND pod:~"magik-ai-gateway.*|envoy.*"';
    const result = getHighlighterExpressionsFromQuery(expr);

    expect(result).toContain('"start_time');
    expect(result).toContain('915f2c6d-a05d-4132-9670-c769adb24d6c');
  });

  it('extracts regex msg filters with alternation and colon', () => {
    const expr = '_msg:~"\\"start_time|Use Proxy:" AND pod:~"magik-ai-gateway.*|envoy.*"';
    const result = getHighlighterExpressionsFromQuery(expr);

    expect(result).toContain('"start_time|Use Proxy:');
  });
});

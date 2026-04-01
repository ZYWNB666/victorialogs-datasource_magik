import { buildVisualQueryFromString } from './parseFromString';
import { LineFilterType } from '../../../../types';

describe('buildVisualQueryFromString', () => {
  it('should parse a simple expression correctly', () => {
    const expr = 'field:value';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters.values).toEqual(['field:value']);
    expect(result.query.pipes).toEqual([]);
  });

  it('should parse simple field expression', () => {
    const expr = 'field';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters.values).toEqual(['field']);
    expect(result.query.pipes).toEqual([]);
  });

  it('should parse field.subfield with value expression', () => {
    const expr = 'field.subfield:"value"';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters.values).toEqual(['field.subfield:"value"']);
    expect(result.query.pipes).toEqual([]);
  });

  it('should parse expression with quoted field and value', () => {
    const expr = '"field:subfield": "value"';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters.values).toEqual(['"field:subfield": "value"']);
    expect(result.query.pipes).toEqual([]);
  });

  it('should handle complex expressions with nested parentheses', () => {
    const expr = '(field1:value1 and field2:value2) or field3:value3';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters).toEqual({
      operators: ['or'],
      values: [
        { operators: ['and'], values: ['field1:value1', 'field2:value2'] },
        'field3:value3'
      ],
    });
    expect(result.query.pipes).toEqual([]);
  });

  it('should handle expressions with quotes correctly', () => {
    const expr = 'field: "value with spaces"';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters.values).toEqual(['field: "value with spaces"']);
    expect(result.query.pipes).toEqual([]);
  });

  it('should reset errors for empty queries', () => {
    const expr = '';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
  });

  it('should reset errors for non empty queries', () => {
    const expr = '*';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
  });

  it('should handle quoted values with parentheses and spaces', () => {
    const expr = '_msg: "(3/9) Installing libunistring (1.3-r0)"';
    const result = buildVisualQueryFromString(expr);
    expect(result.errors).toHaveLength(0);
    expect(result.query.filters.values).toEqual(['_msg: "(3/9) Installing libunistring (1.3-r0)"']);
    expect(result.query.pipes).toEqual([]);
  });

  // Tests for new line filter types
  describe('line filter types', () => {
    it('should parse _msg:"text" as Contains type', () => {
      const expr = '_msg:"error"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(1);
      expect(result.query.msgFilters[0]).toEqual({
        text: 'error',
        type: LineFilterType.Contains,
        contains: true
      });
    });

    it('should parse _msg:!"text" as NotContains type', () => {
      const expr = '_msg:!"error"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(1);
      expect(result.query.msgFilters[0]).toEqual({
        text: 'error',
        type: LineFilterType.NotContains,
        contains: false
      });
    });

    it('should parse _msg:~"(?i)text" as ContainsCaseInsensitive type', () => {
      const expr = '_msg:~"(?i)error"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(1);
      expect(result.query.msgFilters[0]).toEqual({
        text: 'error',
        type: LineFilterType.ContainsCaseInsensitive,
        contains: true
      });
    });

    it('should parse _msg:!~"(?i)text" as NotContainsCaseInsensitive type', () => {
      const expr = '_msg:!~"(?i)error"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(1);
      expect(result.query.msgFilters[0]).toEqual({
        text: 'error',
        type: LineFilterType.NotContainsCaseInsensitive,
        contains: false
      });
    });

    it('should parse _msg:~"regex" as RegexMatch type', () => {
      const expr = '_msg:~"err.*"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(1);
      expect(result.query.msgFilters[0]).toEqual({
        text: 'err.*',
        type: LineFilterType.RegexMatch,
        contains: true
      });
    });

    it('should parse _msg:!~"regex" as RegexNotMatch type', () => {
      const expr = '_msg:!~"err.*"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(1);
      expect(result.query.msgFilters[0]).toEqual({
        text: 'err.*',
        type: LineFilterType.RegexNotMatch,
        contains: false
      });
    });

    it('should parse multiple _msg filters', () => {
      const expr = '_msg:"error" AND _msg:!"debug"';
      const result = buildVisualQueryFromString(expr);
      expect(result.errors).toHaveLength(0);
      expect(result.query.msgFilters).toHaveLength(2);
      expect(result.query.msgFilters[0].type).toBe(LineFilterType.Contains);
      expect(result.query.msgFilters[1].type).toBe(LineFilterType.NotContains);
    });
  });
});

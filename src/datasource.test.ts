import { of } from 'rxjs';

import { AdHocVariableFilter, LogLevel, LogRowContextQueryDirection } from '@grafana/data';
import { TemplateSrv } from '@grafana/runtime';

// eslint-disable-next-line jest/no-mocks-import
import { createDatasource } from './__mocks__/datasource';
import { VARIABLE_ALL_VALUE } from './constants';
import { LogLevelRuleType } from './configuration/LogLevelRules/types';
import {
  DEFAULT_LOG_CONTEXT_MAX_LINES,
  DEFAULT_LOG_CONTEXT_WINDOW_MS,
  DEFAULT_MAX_LINES,
  MAX_LOG_CONTEXT_MAX_LINES,
  MAX_LOG_CONTEXT_WINDOW_MS,
  MAX_QUERY_MAX_LINES,
  REF_ID_STARTER_LOG_CONTEXT_QUERY,
  REF_ID_STARTER_LOG_CONTEXT_REQUEST,
  VictoriaLogsDatasource,
} from './datasource';

const replaceMock = jest.fn().mockImplementation((a: string) => a);

const templateSrvStub = {
  replace: replaceMock,
  getVariables: jest.fn().mockReturnValue([]),
} as unknown as TemplateSrv;

beforeEach(() => {
  jest.clearAllMocks();
});

describe('VictoriaLogsDatasource', () => {
  let ds: VictoriaLogsDatasource;

  beforeEach(() => {
    ds = createDatasource(templateSrvStub);
  });

  describe('limits', () => {
    it('should clamp datasource maxLines to default when settings value is non-positive', () => {
      const maxLinesDs = createDatasource(templateSrvStub, {
        jsonData: {
          maxLines: '0',
        },
      });

      expect(maxLinesDs.maxLines).toBe(DEFAULT_MAX_LINES);
    });

    it('should clamp datasource maxLines to max cap when settings value is too large', () => {
      const maxLinesDs = createDatasource(templateSrvStub, {
        jsonData: {
          maxLines: '50000',
        },
      });

      expect(maxLinesDs.maxLines).toBe(MAX_QUERY_MAX_LINES);
    });
  });

  describe('log context request bounds', () => {
    const makeRow = (timeEpochMs = 1_700_000_000_000) => ({
      timeEpochMs,
      rowIndex: 0,
      labels: {
        _stream_id: 'stream-id-1',
      },
      dataFrame: {
        refId: 'A',
      },
    }) as any;

    it('should use bounded defaults for context request when options are empty', () => {
      const row = makeRow();
      const request = (ds as any).makeLogContextDataRequest(row);

      expect(request.targets[0].maxLines).toBe(DEFAULT_LOG_CONTEXT_MAX_LINES);
      expect(request.range.from.valueOf()).toBe(row.timeEpochMs - DEFAULT_LOG_CONTEXT_WINDOW_MS);
      expect(request.range.to.valueOf()).toBe(row.timeEpochMs - 1);
    });

    it('should clamp context request limit and time window from options', () => {
      const row = makeRow();
      const request = (ds as any).makeLogContextDataRequest(row, {
        direction: LogRowContextQueryDirection.Forward,
        limit: 100_000,
        timeWindowMs: MAX_LOG_CONTEXT_WINDOW_MS * 2,
      });

      expect(request.targets[0].maxLines).toBe(MAX_LOG_CONTEXT_MAX_LINES);
      expect(request.range.from.valueOf()).toBe(row.timeEpochMs + 1);
      expect(request.range.to.valueOf()).toBe(row.timeEpochMs + MAX_LOG_CONTEXT_WINDOW_MS);
    });

    it('should generate unique context request and query ids for different cursors', () => {
      const rowA = makeRow(1_700_000_000_000);
      const rowB = {
        ...makeRow(1_700_000_001_000),
        rowIndex: 5,
      } as any;

      const requestA = (ds as any).makeLogContextDataRequest(rowA, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });
      const requestB = (ds as any).makeLogContextDataRequest(rowB, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 120_000,
      });

      expect(requestA.requestId).toContain(REF_ID_STARTER_LOG_CONTEXT_REQUEST);
      expect(requestA.targets[0].refId).toContain(REF_ID_STARTER_LOG_CONTEXT_QUERY);
      expect(requestA.requestId).not.toBe(requestB.requestId);
      expect(requestA.targets[0].refId).not.toBe(requestB.targets[0].refId);
      expect(requestA.requestId).not.toContain('undefined');
      expect(requestA.targets[0].refId).not.toContain('undefined');
    });
  });

  describe('When interpolating variables', () => {
    let customVariable: any;
    beforeEach(() => {
      customVariable = {
        id: '',
        global: false,
        multi: false,
        includeAll: false,
        allValue: null,
        query: '',
        options: [],
        current: {},
        name: '',
        type: 'custom',
        label: null,
        skipUrlSync: false,
        index: -1,
        initLock: null,
      };
    });

    it('should return a number for numeric value', () => {
      expect(ds.interpolateQueryExpr(1000 as any, customVariable)).toEqual(1000);
    });

    it('should return a value escaped by stringify for one array element', () => {
      expect(ds.interpolateQueryExpr(['arg // for &  test " this string ` end test'] as any, customVariable)).toEqual('$_StartMultiVariable_arg // for &  test " this string ` end test_EndMultiVariable');
    });
  });

  describe('applyTemplateVariables', () => {
    it('should correctly substitute variable in expression using replace function', () => {
      const expr = '_stream:{app!~"$name"}';
      const variables = { name: 'bar' };
      replaceMock.mockImplementation(() => `_stream:{app!~"${variables.name}"}`);
      const interpolatedQuery = ds.applyTemplateVariables({ expr, refId: 'A' }, {});
      expect(interpolatedQuery.expr).toBe('_stream:{app!~"bar"}');
    });

    it('should retain the original query when no variables are present', () => {
      const expr = 'error';
      replaceMock.mockImplementation((input) => input);
      const interpolatedQuery = ds.applyTemplateVariables({ expr, refId: 'A' }, {});
      expect(interpolatedQuery.expr).toBe('error');
    });

    it('should substitute logical operators within the query', () => {
      const expr = '$severity AND _time:$time';
      const variables = { severity: 'error', time: '5m' };
      replaceMock.mockImplementation(() => `${variables.severity} AND _time:${variables.time}`);
      const interpolatedQuery = ds.applyTemplateVariables({ expr, refId: 'A' }, {});
      expect(interpolatedQuery.expr).toBe('error AND _time:5m');
    });

    it('should correctly replace variables within exact match functions', () => {
      const expr = 'log.level:exact("$level")';
      const variables = { level: 'error' };
      replaceMock.mockImplementation(() => `log.level:exact("${variables.level}")`);
      const interpolatedQuery = ds.applyTemplateVariables({ expr, refId: 'A' }, {});
      expect(interpolatedQuery.expr).toBe('log.level:exact("error")');
    });

    it('should not substitute undeclared variables', () => {
      const expr = '_stream:{app!~"$undeclaredVariable"}';
      replaceMock.mockImplementation((input) => input);
      const interpolatedQuery = ds.applyTemplateVariables({ expr, refId: 'A' }, {});
      expect(interpolatedQuery.expr).toBe(expr);
    });

    it('should leave the expression unchanged if the variable is not provided', () => {
      const scopedVars = {};
      const templateSrvMock = {
        replace: jest.fn((a: string) => a),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'foo: $var', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('foo: $var');
    });

    it('should replace $var with the string "bar" in the query', () => {
      const scopedVars = {
        var: { text: 'bar', value: 'bar' },
      };
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var', '"bar"')),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'foo: $var', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('foo: "bar"');
    });

    it('should replace $var with an | expression for stream field when given an array of values', () => {
      const scopedVars = {
        var: { text: 'foo,bar', value: ['foo', 'bar'] },
      };
      const replaceValue = `$_StartMultiVariable_${scopedVars.var.value.join('_separator_')}_EndMultiVariable`;
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var', replaceValue)),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: '_stream{val=~"$var"}', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('_stream{val=~"(foo|bar)"}');
    });

    it('should replace $var with an OR expression when given an array of values', () => {
      const scopedVars = {
        var: { text: 'foo,bar', value: ['foo', 'bar'] },
      };
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var', '("foo" OR "bar")')),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'foo: $var', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('foo: ("foo" OR "bar")');
    });

    it('should correctly substitute an IP address and port variable', () => {
      const scopedVars = {
        var: { text: '0.0.0.0:3000', value: '0.0.0.0:3000' },
      };
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var', '"0.0.0.0:3000"')),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'foo: $var', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('foo: "0.0.0.0:3000"');
    });

    it('should correctly substitute an array of URLs into an OR expression', () => {
      const scopedVars = {
        var: {
          text: 'http://localhost:3001/,http://192.168.50.60:3000/foo',
          value: ['http://localhost:3001/', 'http://192.168.50.60:3000/foo']
        },
      };
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var', '("http://localhost:3001/" OR "http://192.168.50.60:3000/foo")')),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'foo: $var', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('foo: ("http://localhost:3001/" OR "http://192.168.50.60:3000/foo")');
    });

    it('should replace $var with an empty string if the variable is empty', () => {
      const scopedVars = {
        var: { text: '', value: '' },
      };
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var', '')),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'foo: $var', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('foo: ');
    });

    it('should correctly substitute multiple variables within a single expression', () => {
      const scopedVars = {
        var1: { text: 'foo', value: 'foo' },
        var2: { text: 'bar', value: 'bar' },
      };
      const templateSrvMock = {
        replace: jest.fn((a: string) => a?.replace('$var1', '"foo"').replace('$var2', '"bar"')),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables({ expr: 'baz: $var1 AND qux: $var2', refId: 'A' }, scopedVars);
      expect(replacedQuery.expr).toBe('baz: "foo" AND qux: "bar"');
    });

    it('should apply ad-hoc filters to root query when isApplyExtraFiltersToRootQuery is true', () => {
      const adhocFilters: AdHocVariableFilter[] = [
        { key: 'level', operator: '=', value: 'error' },
      ];
      const templateSrvMock = {
        replace: jest.fn((a: string) => a),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables(
        { expr: '_time:5m', refId: 'A', isApplyExtraFiltersToRootQuery: true },
        {},
        adhocFilters
      );
      expect(replacedQuery.expr).toBe('level:="error" | _time:5m');
      expect(replacedQuery.extraFilters).toBeUndefined();
    });

    it('should not apply ad-hoc filters to root query when isApplyExtraFiltersToRootQuery is false', () => {
      const adhocFilters: AdHocVariableFilter[] = [
        { key: 'level', operator: '=', value: 'error' },
      ];
      const templateSrvMock = {
        replace: jest.fn((a: string) => a),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables(
        { expr: '_time:5m', refId: 'A', isApplyExtraFiltersToRootQuery: false },
        {},
        adhocFilters
      );
      expect(replacedQuery.expr).toBe('_time:5m');
      expect(replacedQuery.extraFilters).toBe('level:="error"');
    });

    it('should apply multiple ad-hoc filters to root query when isApplyExtraFiltersToRootQuery is true', () => {
      const adhocFilters: AdHocVariableFilter[] = [
        { key: 'level', operator: '=', value: 'error' },
        { key: 'app', operator: '!=', value: 'test' },
      ];
      const templateSrvMock = {
        replace: jest.fn((a: string) => a),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables(
        { expr: '_time:5m', refId: 'A', isApplyExtraFiltersToRootQuery: true },
        {},
        adhocFilters
      );
      expect(replacedQuery.expr).toBe('level:="error" AND app:!="test" | _time:5m');
      expect(replacedQuery.extraFilters).toBeUndefined();
    });

    it('should handle isApplyExtraFiltersToRootQuery when no ad-hoc filters are present', () => {
      const templateSrvMock = {
        replace: jest.fn((a: string) => a),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables(
        { expr: '_time:5m', refId: 'A', isApplyExtraFiltersToRootQuery: true },
        {}
      );
      expect(replacedQuery.expr).toBe('_time:5m');
      expect(replacedQuery.extraFilters).toBeUndefined();
    });

    it('should preserve existing extraFilters and apply them to root query when isApplyExtraFiltersToRootQuery is true', () => {
      const adhocFilters: AdHocVariableFilter[] = [
        { key: 'level', operator: '=', value: 'error' },
      ];
      const templateSrvMock = {
        replace: jest.fn((a: string) => a),
        getVariables: jest.fn().mockReturnValue([]),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const replacedQuery = ds.applyTemplateVariables(
        { expr: '_time:5m', refId: 'A', extraFilters: 'app:="frontend"', isApplyExtraFiltersToRootQuery: true },
        {},
        adhocFilters
      );
      expect(replacedQuery.expr).toBe('app:="frontend" AND level:="error" | _time:5m');
      expect(replacedQuery.extraFilters).toBeUndefined();
    });



  });


  describe('getLogRowContext fallback behavior', () => {
    const makeRow = (timeEpochMs = 1_700_000_000_000, sourceExpr?: string, searchWords?: string[]) => ({
      timeEpochMs,
      rowIndex: 0,
      labels: {
        _stream_id: 'stream-id-1',
      },
      dataFrame: {
        refId: 'A',
        meta: sourceExpr || searchWords
          ? {
            ...(sourceExpr
              ? {
                custom: {
                  sourceQuery: {
                    expr: sourceExpr,
                  },
                },
              }
              : {}),
            ...(searchWords ? { searchWords } : {}),
          }
          : undefined,
      },
    }) as any;

    const makeFrameWithTimes = (times: number[]) => ({
      fields: [
        {
          name: 'Time',
          type: 'time',
          values: {
            length: times.length,
            get: (index: number) => times[index],
          },
        },
      ],
      refId: 'A',
    }) as any;

    const makeResponseObservable = (times: number[]) => of({ data: [makeFrameWithTimes(times)] }) as any;

    it('should retry with max context window when no directional rows are returned', async () => {
      const row = makeRow();
      const runQuerySpy = jest
        .spyOn(ds, 'runQuery')
        .mockReturnValueOnce(makeResponseObservable([row.timeEpochMs]))
        .mockReturnValueOnce(makeResponseObservable([row.timeEpochMs - 1000]));

      await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(2);
      expect(runQuerySpy.mock.calls[0][0].range.from.valueOf()).toBe(row.timeEpochMs - 60_000);
      expect(runQuerySpy.mock.calls[0][0].range.to.valueOf()).toBe(row.timeEpochMs - 1);
      expect(runQuerySpy.mock.calls[1][0].range.from.valueOf()).toBe(row.timeEpochMs - MAX_LOG_CONTEXT_WINDOW_MS);
      expect(runQuerySpy.mock.calls[1][0].range.to.valueOf()).toBe(row.timeEpochMs - 1);
      expect(runQuerySpy.mock.calls[0][0].requestId).not.toBe(runQuerySpy.mock.calls[1][0].requestId);
      expect(runQuerySpy.mock.calls[0][0].targets[0].refId).not.toBe(runQuerySpy.mock.calls[1][0].targets[0].refId);
    });

    it('should not retry when directional rows are already present', async () => {
      const row = makeRow();
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(1);
    });

    it('should not retry when request already uses max context window', async () => {
      const row = makeRow();
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs]));

      await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: MAX_LOG_CONTEXT_WINDOW_MS,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(1);
    });

    it('should treat same-second second-precision values as non-directional for forward requests', async () => {
      const row = makeRow(1_700_000_000_500);
      const sameSecond = Math.trunc(row.timeEpochMs / 1000);
      const runQuerySpy = jest
        .spyOn(ds, 'runQuery')
        .mockReturnValueOnce(makeResponseObservable([sameSecond]))
        .mockReturnValueOnce(makeResponseObservable([sameSecond + 1]));

      await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Forward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(2);
    });

    it('should treat same-second second-precision values as non-directional for backward requests', async () => {
      const row = makeRow(1_700_000_000_500);
      const sameSecond = Math.trunc(row.timeEpochMs / 1000);
      const runQuerySpy = jest
        .spyOn(ds, 'runQuery')
        .mockReturnValueOnce(makeResponseObservable([sameSecond]))
        .mockReturnValueOnce(makeResponseObservable([sameSecond - 1]));

      await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(2);
    });


    it('should preserve non-level filters while removing _stream_id and level filters from context query', async () => {
      const row = makeRow();
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await ds.getLogRowContext(
        row,
        {
          direction: LogRowContextQueryDirection.Backward,
          timeWindowMs: 60_000,
        },
        {
          refId: 'A',
          expr: '_stream_id:="stream-id-1" AND level:contains_common_case("warn","warning") AND pod:~"api.*" | stats count()',
        } as any
      );

      expect(runQuerySpy.mock.calls[0][0].targets[0].expr).toBe('pod:~"api.*" | stats count()');
    });

    it('should remove msg and level filters from context query', async () => {
      const row = makeRow();
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await ds.getLogRowContext(
        row,
        {
          direction: LogRowContextQueryDirection.Backward,
          timeWindowMs: 60_000,
        },
        {
          refId: 'A',
          expr: '_msg:~"error|warn" AND level:contains_common_case("error") AND app:="frontend" | stats count()',
        } as any
      );

      expect(runQuerySpy.mock.calls[0][0].targets[0].expr).toBe('app:="frontend" | stats count()');
    });
    it('should remove detected_level filter from context query', async () => {
      const row = makeRow();
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await ds.getLogRowContext(
        row,
        {
          direction: LogRowContextQueryDirection.Backward,
          timeWindowMs: 60_000,
        },
        {
          refId: 'A',
          expr: 'detected_level:="error" AND app:="frontend"',
        } as any
      );

      expect(runQuerySpy.mock.calls[0][0].targets[0].expr).toBe('app:="frontend"');
    });

    it('should remove filters based on active custom level rule fields', async () => {
      const dsWithRule = createDatasource(templateSrvStub, {
        jsonData: {
          maxLines: '20',
          logLevelRules: [
            {
              field: 'severity_text',
              operator: LogLevelRuleType.Equals,
              value: 'ERROR',
              level: LogLevel.error,
              enabled: true,
            },
          ],
        },
      });
      const row = makeRow();
      const runQuerySpy = jest.spyOn(dsWithRule, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await dsWithRule.getLogRowContext(
        row,
        {
          direction: LogRowContextQueryDirection.Backward,
          timeWindowMs: 60_000,
        },
        {
          refId: 'A',
          expr: 'severity_text:="ERROR" AND app:="frontend"',
        } as any
      );

      expect(runQuerySpy.mock.calls[0][0].targets[0].expr).toBe('app:="frontend"');
    });

    it('should preserve source searchWords on context response frames', async () => {
      const row = makeRow(1_700_000_000_000, undefined, ['error', 'warn']);
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      const response = await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(1);
      expect(response.data[0].meta?.searchWords).toEqual(['error', 'warn']);
    });

    it('should merge source searchWords with existing context response frame searchWords', async () => {
      const row = makeRow(1_700_000_000_000, undefined, ['warn', 'error']);
      const contextFrame = {
        ...makeFrameWithTimes([row.timeEpochMs - 1]),
        meta: {
          searchWords: ['existing', 'warn'],
        },
      };
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(of({ data: [contextFrame] }) as any);

      const response = await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(1);
      expect(response.data[0].meta?.searchWords).toEqual(['existing', 'warn', 'error']);
    });

    it('should preserve source searchWords after fallback context request', async () => {
      const row = makeRow(1_700_000_000_000, undefined, ['fallback']);
      const runQuerySpy = jest
        .spyOn(ds, 'runQuery')
        .mockReturnValueOnce(makeResponseObservable([row.timeEpochMs]))
        .mockReturnValueOnce(makeResponseObservable([row.timeEpochMs - 1000]));

      const response = await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy).toHaveBeenCalledTimes(2);
      expect(response.data[0].meta?.searchWords).toEqual(['fallback']);
    });

    it('should not enforce _stream_id filter when source query comes from frame metadata', async () => {
      const row = makeRow(1_700_000_000_000, 'level:contains_common_case("info") AND env:="prod"');
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await ds.getLogRowContext(row, {
        direction: LogRowContextQueryDirection.Backward,
        timeWindowMs: 60_000,
      });

      expect(runQuerySpy.mock.calls[0][0].targets[0].expr).toBe('env:="prod"');
    });

    it('should fallback to wildcard context query when only excluded filters remain', async () => {
      const row = makeRow();
      const runQuerySpy = jest.spyOn(ds, 'runQuery').mockReturnValue(makeResponseObservable([row.timeEpochMs - 1]));

      await ds.getLogRowContext(
        row,
        {
          direction: LogRowContextQueryDirection.Backward,
          timeWindowMs: 60_000,
        },
        {
          refId: 'A',
          expr: '_stream_id:="stream-id-1" AND level:contains_common_case("info")',
        } as any
      );

      expect(runQuerySpy.mock.calls[0][0].targets[0].expr).toBe('*');
    });
  });

  describe('getExtraFilters', () => {
    it('should return undefined when no adhoc filters are provided', () => {
      const result = ds.getExtraFilters();
      expect(result).toBeUndefined();
    });

    it('should return a valid query string when adhoc filters are present', () => {
      const filters: AdHocVariableFilter[] = [
        { key: 'key1', operator: '=', value: 'value1' },
        { key: 'key2', operator: '!=', value: 'value2' },
      ];
      const result = ds.getExtraFilters(filters);
      expect(result).toBe('key1:="value1" AND key2:!="value2"');
    });
  });

  describe('interpolateString', () => {
    it('should interpolate string with all and multi values', () => {
      const scopedVars = {};
      const variables = [
        {
          name: 'var1',
          current: [{ value: 'foo' }, { value: 'bar' }],
          multi: true,
          type: 'query',
          query: {
            type: 'fieldValue'
          }
        }, {
          name: 'var2',
          current: { value: VARIABLE_ALL_VALUE },
          multi: false,
        }
      ];
      const templateSrvMock = {
        replace: jest.fn(() => 'foo: in($_StartMultiVariable_foo_separator_bar_EndMultiVariable) bar: in(*)'),
        getVariables: jest.fn().mockReturnValue(variables),
      } as unknown as TemplateSrv;
      const ds = createDatasource(templateSrvMock);
      const result = ds.interpolateString('foo: $var1 bar: $var2', scopedVars);
      expect(result).toStrictEqual('foo: in(\"foo\",\"bar\") bar:in(*)');
    });
  });
});

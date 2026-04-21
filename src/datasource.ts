import { cloneDeep } from 'lodash';
import { lastValueFrom, map, merge, Observable } from 'rxjs';

import {
  AdHocVariableFilter,
  CoreApp,
  DataFrame,
  DataQuery,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceGetTagKeysOptions,
  DataSourceGetTagValuesOptions,
  DataSourceInstanceSettings,
  DataSourceWithLogsContextSupport,
  DEFAULT_FIELD_DISPLAY_VALUES_LIMIT,
  FieldType,
  LegacyMetricFindQueryOptions,
  LiveChannelScope,
  LoadingState,
  LogRowContextOptions,
  LogRowContextQueryDirection,
  LogRowModel,
  MetricFindValue,
  QueryVariableModel,
  rangeUtil,
  ScopedVars,
  SupplementaryQueryOptions,
  SupplementaryQueryType,
  TimeRange,
  toUtc,
  TypedVariableModel,
} from '@grafana/data';
import { config, DataSourceWithBackend, getGrafanaLiveSrv, getTemplateSrv, TemplateSrv } from '@grafana/runtime';

import { correctMultiExactOperatorValueAll } from './LogsQL/multiExactOperator';
import { correctRegExpValueAll, doubleQuoteRegExp, isRegExpOperatorInLastFilter } from './LogsQL/regExpOperator';
import {
  buildStreamExtraFilters
} from './components/QueryEditor/QueryBuilder/components/StreamFilters/streamFilterUtils';
import QueryEditor from './components/QueryEditor/QueryEditor';
import { LogLevelRule } from './configuration/LogLevelRules/types';
import { TEXT_FILTER_ALL_VALUE, VARIABLE_ALL_VALUE } from './constants';
import { escapeLabelValueInSelector } from './languageUtils';
import LogsQlLanguageProvider from './language_provider';
import { LOGS_VOLUME_BARS, queryLogsVolume } from './logsVolumeLegacy';
import {
  addLabelToQuery,
  addSortPipeToQuery,
  getQueryFormat,
  queryHasFilter,
  removeLabelFromQuery,
} from './modifyQuery';
import { buildVisualQueryFromString, splitExpression } from './components/QueryEditor/QueryBuilder/utils/parseFromString';
import { parseVisualQueryToString } from './components/QueryEditor/QueryBuilder/utils/parseToString';
import { removeDoubleQuotesAroundVar } from './parsing';
import { replaceOperatorWithIn, returnVariables } from './parsingUtils';
import { transformBackendResult } from './transformers';
import {
  DerivedFieldConfig,
  FilterActionType,
  FilterFieldType,
  FilterVisualQuery,
  MultitenancyHeaders,
  Options,
  Query,
  QueryBuilderLimits,
  QueryFilterOptions,
  QueryType,
  StreamFilterState,
  SupportingQueryType,
  Tenant,
  TenantHeaderNames,
  ToggleFilterAction,
  VariableQuery,
} from './types';
import { formatOffsetDuration, getMillisecondsFromDuration } from './utils/timeUtils';
import { VariableSupport } from './variableSupport/VariableSupport';

export const REF_ID_STARTER_LOG_VOLUME = 'log-volume-';
export const REF_ID_STARTER_LOG_SAMPLE = 'log-sample-';
export const REF_ID_STARTER_LOG_CONTEXT_REQUEST = 'log-context-request-';
export const REF_ID_STARTER_LOG_CONTEXT_QUERY = 'log-context-query-';
export const DEFAULT_MAX_LINES = 500;
export const MAX_QUERY_MAX_LINES = 10000;
export const DEFAULT_LOG_CONTEXT_MAX_LINES = 100;
export const MAX_LOG_CONTEXT_MAX_LINES = 1000;
export const DEFAULT_LOG_CONTEXT_WINDOW_MS = 5 * 60 * 1000;
export const MAX_LOG_CONTEXT_WINDOW_MS = 2 * 60 * 60 * 1000;

const clampPositiveInt = (value: number | undefined, fallback: number, max: number): number => {
  const normalized = Number.isFinite(value) ? Math.trunc(value as number) : NaN;
  const candidate = normalized > 0 ? normalized : fallback;
  return Math.min(candidate, max);
};

export class VictoriaLogsDatasource
  extends DataSourceWithBackend<Query, Options>
  implements DataSourceWithLogsContextSupport<Query> {
  id: number | undefined;
  uid: string;
  url: string;
  maxLines: number;
  derivedFields: DerivedFieldConfig[];
  basicAuth?: string;
  withCredentials?: boolean;
  httpMethod: string;
  customQueryParameters: URLSearchParams;
  languageProvider?: LogsQlLanguageProvider;
  queryBuilderLimits?: QueryBuilderLimits;
  logLevelRules: LogLevelRule[];
  multitenancyHeaders?: MultitenancyHeaders;

  constructor(
    instanceSettings: DataSourceInstanceSettings<Options>,
    private readonly templateSrv: TemplateSrv = getTemplateSrv(),
    languageProvider?: LogsQlLanguageProvider
  ) {
    super(instanceSettings);

    const settingsData = instanceSettings.jsonData || {};
    this.id = instanceSettings.id;
    this.uid = instanceSettings.uid;
    this.url = instanceSettings.url!;
    this.basicAuth = instanceSettings.basicAuth;
    this.withCredentials = instanceSettings.withCredentials;
    this.httpMethod = settingsData.httpMethod || 'POST';
    this.maxLines = clampPositiveInt(parseInt(settingsData.maxLines ?? '0', 10), DEFAULT_MAX_LINES, MAX_QUERY_MAX_LINES);
    this.derivedFields = settingsData.derivedFields || [];
    this.customQueryParameters = new URLSearchParams(settingsData.customQueryParameters);
    this.languageProvider = languageProvider ?? new LogsQlLanguageProvider(this);
    this.annotations = {
      QueryEditor: QueryEditor,
    };
    this.variables = new VariableSupport(this);
    this.queryBuilderLimits = settingsData.queryBuilderLimits;
    this.logLevelRules = settingsData.logLevelRules || [];
    this.multitenancyHeaders = this.parseMultitenancyHeaders(settingsData.multitenancyHeaders);
  }

  query(request: DataQueryRequest<Query>): Observable<DataQueryResponse> {
    const timezoneOffset = formatOffsetDuration(request.timezone, request.range.from.utcOffset());
    const queries = request.targets
      .filter((q) => q.expr || config.publicDashboardAccessToken !== '')
      .map((q) => {
        return {
          ...q,
          // to backend sort for limited data to show first logs in the selected time range if the user clicks on the sort button
          expr: addSortPipeToQuery(q, request.app, request.liveStreaming),
          maxLines: clampPositiveInt(q.maxLines, this.maxLines, MAX_QUERY_MAX_LINES),
          timezoneOffset,
          format: getQueryFormat(q.expr),
          step: this.templateSrv.replace(q.step, request.scopedVars),
        };
      });

    // if step is defined, use it as the request interval to set the width of bars correctly
    request.intervalMs = queries[0]?.step ? getMillisecondsFromDuration(queries[0]?.step) : request.intervalMs;
    request.targets = queries;

    if (request.liveStreaming) {
      return this.runLiveQueryThroughBackend(request);
    }

    return this.runQuery(request);
  }

  runQuery(fixedRequest: DataQueryRequest<Query>) {
    return super
      .query(fixedRequest)
      .pipe(
        map((response) =>
          transformBackendResult(
            response,
            fixedRequest,
            this.derivedFields ?? [],
            this.getActiveLevelRules()
          )
        )
      );
  }

  toggleQueryFilter(query: Query, filter: ToggleFilterAction): Query {
    let expression = query.expr ?? '';

    if (!filter.options?.key || !filter.options?.value) {
      return { ...query, expr: expression };
    }

    const value = escapeLabelValueInSelector(filter.options.value);
    const hasFilter = queryHasFilter(expression, filter.options.key, value);

    if (hasFilter) {
      expression = removeLabelFromQuery(expression, filter.options.key, value);
    }

    const isFilterFor = filter.type === FilterActionType.FILTER_FOR;
    const isFilterOut = filter.type === FilterActionType.FILTER_OUT;

    if ((isFilterFor && !hasFilter) || isFilterOut) {
      const operator = isFilterFor ? '=' : '!=';
      expression = addLabelToQuery(expression, { key: filter.options.key, value, operator });
    }

    return { ...query, expr: expression };
  }

  queryHasFilter(query: Query, filter: QueryFilterOptions): boolean {
    const expression = query.expr ?? '';
    return queryHasFilter(expression, filter.key, filter.value, '=');
  }

  filterQuery(query: Query): boolean {
    if (query.hide || query.expr === '') {
      return false;
    }
    return true;
  }

  applyTemplateVariables(target: Query, scopedVars: ScopedVars, adhocFilters?: AdHocVariableFilter[]): Query {
    const { __auto, __interval, __interval_ms, __range, __range_s, __range_ms, ...rest } = scopedVars || {};

    const variables = {
      ...rest,
      __interval: {
        value: '$__interval',
      },
      __interval_ms: {
        value: '$__interval_ms',
      },
    };

    let extraFilters = this.getExtraFilters(adhocFilters, target.extraFilters);
    let expr = this.interpolateString(target.expr, variables);
    if (target.isApplyExtraFiltersToRootQuery && extraFilters) {
      expr = `${extraFilters} | ${expr}`;
      extraFilters = undefined;
    }

    const extraStreamFilters = this.getExtraStreamFilters(target.streamFilters, scopedVars);
    return {
      ...target,
      legendFormat: this.templateSrv.replace(target.legendFormat, rest),
      expr,
      extraFilters,
      extraStreamFilters,
    };
  }

  getExtraFilters(adhocFilters?: AdHocVariableFilter[], initialExpr = ''): string | undefined {
    if (!adhocFilters) {
      return initialExpr || undefined;
    }

    const expr = adhocFilters.reduce((acc: string, filter: AdHocVariableFilter) => {
      return addLabelToQuery(acc, filter);
    }, initialExpr);

    return returnVariables(expr);
  }

  getExtraStreamFilters(streamFilters: StreamFilterState[] | undefined, scopedVars: ScopedVars): string | undefined {
    if (!streamFilters) {
      return undefined;
    }

    return this.interpolateString(buildStreamExtraFilters(streamFilters), scopedVars) || undefined;
  }

  interpolateQueryExpr(value: any, _variable: any) {
    if (typeof value === 'string' && value) {
      value = [value];
    }

    if (Array.isArray(value)) {
      return value.length > 0 ? `$_StartMultiVariable_${value.join('_separator_')}_EndMultiVariable` : '';
    }

    return value;
  }

  interpolateVariablesInQueries(queries: Query[], scopedVars: ScopedVars, filters?: AdHocVariableFilter[]): Query[] {
    let expandedQueries = queries;
    if (queries && queries.length) {
      expandedQueries = queries.map((query) => ({
        ...query,
        datasource: this.getRef(),
        expr: this.interpolateString(query.expr, scopedVars),
        interval: this.templateSrv.replace(query.interval, scopedVars),
        extraFilters: this.getExtraFilters(filters, query.extraFilters),
        extraStreamFilters: this.getExtraStreamFilters(query.streamFilters, scopedVars)
      }));
    }
    return expandedQueries;
  }

  async metricFindQuery(
    query: VariableQuery,
    options?: LegacyMetricFindQueryOptions
  ): Promise<MetricFindValue[]> {
    if (!query) {
      return Promise.resolve([]);
    }

    const interpolatedVariableQuery: VariableQuery = {
      ...query,
      field: this.interpolateString(query.field || '', options?.scopedVars),
      query: this.interpolateString(query.query || '', options?.scopedVars),
    };

    return await this.processMetricFindQuery(interpolatedVariableQuery, options?.range);
  }

  async getTagKeys(options?: DataSourceGetTagKeysOptions<Query>): Promise<MetricFindValue[]> {
    const list = await this.languageProvider?.getFieldList({
      type: FilterFieldType.FieldName,
      timeRange: options?.timeRange,
      limit: DEFAULT_FIELD_DISPLAY_VALUES_LIMIT,
    }, this.customQueryParameters);
    return list
      ? list.map(({ value }) => ({ text: value || ' ' }))
      : [];
  }

  async getTagValues(options: DataSourceGetTagValuesOptions<Query>): Promise<MetricFindValue[]> {
    const list = await this.languageProvider?.getFieldList({
      type: FilterFieldType.FieldValue,
      timeRange: options.timeRange,
      limit: DEFAULT_FIELD_DISPLAY_VALUES_LIMIT,
      field: options.key,
    }, this.customQueryParameters);
    return list
      ? list.map(({ value }) => ({ text: value || ' ' }))
      : [];
  }

  isAllOption(variable: TypedVariableModel): boolean {
    const value = 'current' in variable && variable?.current?.value;
    if (!value) {
      return false;
    }

    if (typeof value === 'string') {
      return value === VARIABLE_ALL_VALUE || value === TEXT_FILTER_ALL_VALUE;
    }

    return Array.isArray(value) ? value.includes(VARIABLE_ALL_VALUE) : false;
  }

  replaceOperatorsToInForMultiQueryVariables(expr: string) {
    const variables = this.templateSrv.getVariables();
    const fieldValuesVariables = variables.filter(v => v.type === 'query' && v.query.type === 'fieldValue' && v.multi || this.isAllOption(v)) as QueryVariableModel[];
    let result = expr;
    for (const variable of fieldValuesVariables) {
      result = removeDoubleQuotesAroundVar(result, variable.name);
      result = replaceOperatorWithIn(result, variable.name);
    }
    return result;
  }

  interpolateString(string: string, scopedVars?: ScopedVars) {
    let expr = this.replaceOperatorsToInForMultiQueryVariables(string);
    const variableNamesList = this.templateSrv.getVariables().map(v => v.name);
    expr = doubleQuoteRegExp(expr, variableNamesList);
    expr = this.templateSrv.replace(expr, scopedVars, this.interpolateQueryExpr);
    expr = correctRegExpValueAll(expr);
    expr = correctMultiExactOperatorValueAll(expr);
    return this.replaceMultiVariables(expr);
  }

  private replaceMultiVariables(input: string): string {
    const multiVariablePattern = /\$_StartMultiVariable_(.+?)_EndMultiVariable?/g;

    return input.replace(multiVariablePattern, (match, valueList: string, offset) => {
      const values = valueList.split('_separator_');

      const queryBeforeOffset = input.slice(0, offset);
      const precedingChars = queryBeforeOffset.replace(/\s+/g, '').slice(-3);

      if (isRegExpOperatorInLastFilter(queryBeforeOffset)) {
        return `(${values.join('|')})`;
      } else if (precedingChars.includes('in(')) {
        return values.map(value => JSON.stringify(value)).join(',');
      }
      return values.join(' OR ');
    });
  }

  private async processMetricFindQuery(query: VariableQuery, timeRange?: TimeRange): Promise<MetricFindValue[]> {
    const list = await this.languageProvider?.getFieldList({
      type: query.type,
      timeRange,
      field: query.field,
      query: query.query,
      limit: query.limit,
    }, this.customQueryParameters);
    return (list ? list.map(({ value }) => ({ text: value })) : []);
  }

  getQueryBuilderLimits(key: FilterFieldType): number {
    return this.queryBuilderLimits?.[key] || 0;
  }

  private runLiveQueryThroughBackend(request: DataQueryRequest<Query>): Observable<DataQueryResponse> {
    const observables = request.targets.map((query) => {
      return getGrafanaLiveSrv()
        .getDataStream({
          addr: {
            scope: LiveChannelScope.DataSource,
            // @ts-expect-error - for the Grafana with React version < 19,
            // the interface of the Live feature expects the `stream` field instead of the `namespace`,
            // so we need to send both for compatibility with older versions
            namespace: this.uid,
            stream: this.uid,
            path: `${request.requestId}/${query.refId}`,
            data: {
              ...query,
            },
          },
        })
        .pipe(
          map((response) => {
            return {
              data: response.data || [],
              key: `victoriametrics-logs-datasource-${request.requestId}-${query.refId}`,
              state: LoadingState.Streaming,
            };
          })
        );
    });

    return merge(...observables);
  }

  getSupplementaryRequest(
    type: SupplementaryQueryType,
    request: DataQueryRequest<Query>,
    options?: SupplementaryQueryOptions
  ): DataQueryRequest<Query> | undefined {
    const logsVolumeOption = { ...options, type };
    const logsVolumeRequest = cloneDeep(request);
    const targets = logsVolumeRequest.targets
      .map((query) => this.getSupplementaryQuery(logsVolumeOption, query, logsVolumeRequest))
      .filter((query): query is Query => !!query);

    if (!targets.length) {
      return undefined;
    }

    return { ...logsVolumeRequest, targets };
  }

  getSupportedSupplementaryQueryTypes(): SupplementaryQueryType[] {
    return [SupplementaryQueryType.LogsVolume, SupplementaryQueryType.LogsSample];
  }

  getSupplementaryQuery(options: SupplementaryQueryOptions, query: Query, request: DataQueryRequest<Query>): Query | undefined {
    if (query.hide) {
      return undefined;
    }

    switch (options.type) {
      case SupplementaryQueryType.LogsVolume: {
        const totalSeconds = request.range.to.diff(request.range.from, 'second');
        const step = Math.ceil(totalSeconds / LOGS_VOLUME_BARS) || '';

        const fields = this.getActiveLevelRules().map(r => r.field);
        const uniqFields = Array.from(new Set([...fields, 'level']));

        return {
          ...query,
          step: `${step}s`,
          fields: uniqFields,
          queryType: QueryType.Hits,
          refId: `${REF_ID_STARTER_LOG_VOLUME}${query.refId}`,
          supportingQueryType: SupportingQueryType.LogsVolume,
          timezoneOffset: formatOffsetDuration(request.timezone, request.range.from.utcOffset()),
        };
      }
      case SupplementaryQueryType.LogsSample:
        return {
          ...query,
          queryType: QueryType.Instant,
          refId: `${REF_ID_STARTER_LOG_SAMPLE}${query.refId}`,
          supportingQueryType: SupportingQueryType.LogsSample,
          maxLines: this.maxLines
        };

      default:
        return undefined;
    }
  }

  getDataProvider(
    type: SupplementaryQueryType,
    request: DataQueryRequest<Query>
  ): Observable<DataQueryResponse> | undefined {
    if (!this.getSupportedSupplementaryQueryTypes().includes(type)) {
      return undefined;
    }

    const newRequest = this.getSupplementaryRequest(type, request);
    if (!newRequest) {
      return;
    }

    switch (type) {
      case SupplementaryQueryType.LogsVolume:
        return queryLogsVolume(this, newRequest);
      default:
        return undefined;
    }
  }

  getQueryDisplayText(query: Query): string {
    return query.expr || '';
  }

  getActiveLevelRules(): LogLevelRule[] {
    return (this.logLevelRules || []).filter(r => r.enabled !== false);
  }

  getLogRowContext = async (
    row: LogRowModel,
    options?: LogRowContextOptions,
    query?: DataQuery
  ): Promise<{ data: DataFrame[] }> => {
    const direction = options?.direction || LogRowContextQueryDirection.Backward;
    const requestedWindowMs = clampPositiveInt(options?.timeWindowMs, DEFAULT_LOG_CONTEXT_WINDOW_MS, MAX_LOG_CONTEXT_WINDOW_MS);
    const sourceSearchWords = this.getLogContextSearchWords(row);

    const contextRequest = this.makeLogContextDataRequest(row, {
      ...options,
      direction,
      timeWindowMs: requestedWindowMs,
    }, query);
    const response = this.applyContextSearchWords(await lastValueFrom(this.runQuery(contextRequest)), sourceSearchWords);

    const hasDirectionalRows = this.hasDirectionalContextRows(response.data, row.timeEpochMs, direction);
    if (hasDirectionalRows || requestedWindowMs >= MAX_LOG_CONTEXT_WINDOW_MS) {
      return response;
    }

    const expandedRequest = this.makeLogContextDataRequest(row, {
      ...options,
      direction,
      timeWindowMs: MAX_LOG_CONTEXT_WINDOW_MS,
    }, query);

    return this.applyContextSearchWords(await lastValueFrom(this.runQuery(expandedRequest)), sourceSearchWords);
  };


  private hasDirectionalContextRows = (
    frames: DataFrame[],
    rowTimeEpochMs: number,
    direction: LogRowContextQueryDirection
  ): boolean => {
    const isForwardDirection = direction === LogRowContextQueryDirection.Forward;
    const rowTimestampSec = Math.trunc(rowTimeEpochMs / 1000);

    for (const frame of frames) {
      const timeField = frame.fields.find((field) => field.type === FieldType.time);
      if (!timeField) {
        continue;
      }

      const values = timeField.values as { length?: number; get?: (index: number) => unknown; [index: number]: unknown };
      const valuesLength = typeof values.length === 'number' ? values.length : 0;

      for (let index = 0; index < valuesLength; index++) {
        const rawValue = typeof values.get === 'function' ? values.get(index) : values[index];
        if (rawValue == null) {
          continue;
        }

        let timestampMs: number;
        let isSecondPrecisionTimestamp = false;
        if (rawValue instanceof Date) {
          timestampMs = rawValue.valueOf();
        } else {
          const numericValue = Number(rawValue);
          if (!Number.isFinite(numericValue)) {
            continue;
          }

          if (numericValue > 1e14) {
            timestampMs = Math.trunc(numericValue / 1e6);
          } else if (numericValue > 1e11) {
            timestampMs = Math.trunc(numericValue);
          } else {
            timestampMs = Math.trunc(numericValue * 1000);
            isSecondPrecisionTimestamp = true;
          }
        }

        if (isSecondPrecisionTimestamp) {
          const timestampSec = Math.trunc(timestampMs / 1000);
          if (isForwardDirection ? timestampSec > rowTimestampSec : timestampSec < rowTimestampSec) {
            return true;
          }
          continue;
        }

        if (isForwardDirection ? timestampMs > rowTimeEpochMs : timestampMs < rowTimeEpochMs) {
          return true;
        }
      }
    }

    return false;
  };

  private getLogContextSourceExpr = (row: LogRowModel, query?: DataQuery): string | undefined => {
    if (typeof (query as Query | undefined)?.expr === 'string' && (query as Query).expr.trim()) {
      return (query as Query).expr;
    }

    const sourceQuery = (row.dataFrame.meta?.custom as { sourceQuery?: Partial<Query> } | undefined)?.sourceQuery;
    if (typeof sourceQuery?.expr === 'string' && sourceQuery.expr.trim()) {
      return sourceQuery.expr;
    }

    return undefined;
  };

  private getLogContextSearchWords = (row: LogRowModel): string[] | undefined => {
    const searchWords = row.dataFrame.meta?.searchWords;
    if (!Array.isArray(searchWords)) {
      return undefined;
    }

    const normalizedSearchWords = searchWords.filter((word): word is string => typeof word === 'string' && word.length > 0);
    return normalizedSearchWords.length ? normalizedSearchWords : undefined;
  };

  private applyContextSearchWords = (
    response: { data: DataFrame[] },
    sourceSearchWords?: string[]
  ): { data: DataFrame[] } => {
    if (!sourceSearchWords?.length) {
      return response;
    }

    return {
      ...response,
      data: response.data.map((frame) => {
        const frameSearchWords = Array.isArray(frame.meta?.searchWords)
          ? frame.meta.searchWords.filter((word): word is string => typeof word === 'string' && word.length > 0)
          : [];

        const mergedSearchWords = Array.from(new Set([...frameSearchWords, ...sourceSearchWords]));

        return {
          ...frame,
          meta: {
            ...frame.meta,
            searchWords: mergedSearchWords,
          },
        };
      }),
    };
  };

  private getContextFilterFieldsToRemove = (): Set<string> => {
    const fields = new Set<string>(['_stream_id', 'level', 'detected_level']);

    for (const rule of this.getActiveLevelRules()) {
      const normalizedField = rule.field?.trim().replace(/^"|"$/g, '').toLowerCase();
      if (normalizedField) {
        fields.add(normalizedField);
      }
    }

    return fields;
  };

  private extractFilterFieldName = (filterPart: string): string | undefined => {
    const match = filterPart
      .trim()
      .match(/^(?:!\s*)?(?:\(\s*)?(?:"((?:[^"\\]|\\.)+)"|([^\s:()]+))\s*:/);

    if (!match) {
      return undefined;
    }

    return (match[1] || match[2] || '').replace(/\\"/g, '"').trim();
  };

  private shouldExcludeContextFilter = (filterPart: string, excludedFields: Set<string>): boolean => {
    const fieldName = this.extractFilterFieldName(filterPart);
    if (!fieldName) {
      return false;
    }

    return excludedFields.has(fieldName.toLowerCase());
  };

  private isDetachedFunctionArgsGroup = (filter: FilterVisualQuery): boolean => {
    if (filter.values.length === 0) {
      return false;
    }

    return filter.values.every((value) => {
      if (typeof value === 'string') {
        return !value.includes(':');
      }
      return this.isDetachedFunctionArgsGroup(value);
    });
  };

  private pruneContextFilters = (
    filters: FilterVisualQuery,
    shouldRemove: (filterPart: string) => boolean
  ): { filter?: FilterVisualQuery; removed: boolean } => {
    const prune = (node: FilterVisualQuery): { filter?: FilterVisualQuery; removed: boolean } => {
      const values: Array<string | FilterVisualQuery> = [];
      const operators: string[] = [];
      let removed = false;
      let removeDetachedFunctionArgs = false;

      node.values.forEach((value, index) => {
        if (typeof value === 'string') {
          const trimmedValue = value.trim();
          const isDetachedFunctionArgs =
            removeDetachedFunctionArgs &&
            trimmedValue.startsWith('(') &&
            trimmedValue.endsWith(')') &&
            !trimmedValue.includes(':');
          const isRemoved = shouldRemove(value) || isDetachedFunctionArgs;

          if (isRemoved) {
            removed = true;
            removeDetachedFunctionArgs = /:[a-zA-Z_][\w.]*$/.test(trimmedValue);
            return;
          }

          removeDetachedFunctionArgs = false;
          if (values.length > 0) {
            operators.push(node.operators[index - 1] || 'AND');
          }
          values.push(value);
          return;
        }

        const nested = prune(value);
        removed = removed || nested.removed;

        if (removeDetachedFunctionArgs && nested.filter && this.isDetachedFunctionArgsGroup(nested.filter)) {
          removed = true;
          removeDetachedFunctionArgs = false;
          return;
        }

        removeDetachedFunctionArgs = false;
        if (!nested.filter || nested.filter.values.length === 0) {
          return;
        }

        if (values.length > 0) {
          operators.push(node.operators[index - 1] || 'AND');
        }
        values.push(nested.filter);
      });

      if (values.length === 0) {
        return { removed };
      }

      return {
        filter: {
          values,
          operators,
        },
        removed,
      };
    };

    return prune(filters);
  };

  private prepareLogContextQueryExpr = (sourceExpr?: string): string => {
    const baseExpr = (sourceExpr || '').trim();
    if (!baseExpr) {
      return '*';
    }

    const [filterPart = '', ...pipeParts] = splitExpression(baseExpr);
    if (!filterPart) {
      return baseExpr;
    }

    const parsed = buildVisualQueryFromString(filterPart);
    if (parsed.errors.length) {
      return baseExpr;
    }

    const excludedFields = this.getContextFilterFieldsToRemove();
    const { filter, removed } = this.pruneContextFilters(parsed.query.filters, (part) =>
      this.shouldExcludeContextFilter(part, excludedFields)
    );
    const hasMsgFilters = (parsed.query.msgFilters || []).length > 0;

    if (!removed && !hasMsgFilters) {
      return baseExpr;
    }

    const filterExpr = parseVisualQueryToString({
      filters: removed ? filter || { values: [], operators: [] } : filter || parsed.query.filters,
      pipes: [],
      msgFilters: [],
      msgFilterOperators: [],
    }).trim();

    const queryParts = [
      filterExpr,
      ...pipeParts.map((part) => part.trim()).filter(Boolean),
    ].filter(Boolean);

    return queryParts.length > 0 ? queryParts.join(' | ') : '*';
  };

  private makeLogContextDataRequest = (row: LogRowModel, options?: LogRowContextOptions, sourceQuery?: DataQuery): DataQueryRequest<Query> => {
    const direction = options?.direction || LogRowContextQueryDirection.Backward;

    const contextMaxLines = clampPositiveInt(options?.limit, DEFAULT_LOG_CONTEXT_MAX_LINES, MAX_LOG_CONTEXT_MAX_LINES);
    const contextWindowMs = clampPositiveInt(options?.timeWindowMs, DEFAULT_LOG_CONTEXT_WINDOW_MS, MAX_LOG_CONTEXT_WINDOW_MS);
    const contextSourceExpr = this.getLogContextSourceExpr(row, sourceQuery);
    const interpolatedExpr = contextSourceExpr
      ? this.interpolateString(contextSourceExpr, options?.scopedVars)
      : undefined;

    const contextExpr = this.prepareLogContextQueryExpr(interpolatedExpr);
    const contextCursorKey = `${row.dataFrame.refId}-${row.rowIndex}-${row.timeEpochMs}-${direction}-${contextWindowMs}`;

    const query: Query = {
      expr: contextExpr,
      refId: `${REF_ID_STARTER_LOG_CONTEXT_QUERY}${contextCursorKey}`,
      maxLines: contextMaxLines,
    };

    const range = this.createContextTimeRange(row.timeEpochMs, direction, contextWindowMs);

    const interval = rangeUtil.calculateInterval(range, 1);

    return {
      app: CoreApp.Explore,
      interval: interval.interval,
      intervalMs: interval.intervalMs,
      range: range,
      requestId: `${REF_ID_STARTER_LOG_CONTEXT_REQUEST}${contextCursorKey}`,
      scopedVars: options?.scopedVars || {},
      startTime: Date.now(),
      targets: [query],
      timezone: 'UTC'
    };
  };

  private createContextTimeRange = (rowTimeEpochMs: number, direction?: LogRowContextQueryDirection, windowMs = DEFAULT_LOG_CONTEXT_WINDOW_MS): TimeRange => {
    const offset = windowMs;

    const timeRange =
      direction === LogRowContextQueryDirection.Backward
        ? {
          from: toUtc(rowTimeEpochMs - offset),
          to: toUtc(rowTimeEpochMs - 1)
        }
        : {
          from: toUtc(rowTimeEpochMs + 1),
          to: toUtc(rowTimeEpochMs + offset)
        };

    return { ...timeRange, raw: timeRange };
  };

  async fetchTenantIds(): Promise<{ hint: string } | string[]> {
    try {
      const res = await this.postResource<{ hint: string } | Tenant[]>('select/tenant_ids', {});

      if (!Array.isArray(res)) {
        if (res.hint) {
          return res;
        }
        return [];
      }

      const tenantSet = new Set<string>();
      res.forEach((item: Tenant) => {
        tenantSet.add(`${item.account_id}:${item.project_id}`);
      });

      return Array.from(tenantSet);
    } catch (error) {
      console.error('Failed to fetch tenants:', error);
      return [];
    }
  }

  parseMultitenancyHeaders(multitenancyHeaders?: Partial<Record<TenantHeaderNames, string>>): MultitenancyHeaders {
    const formatTenantId = (value: string | number | undefined): string => {
      if (value === undefined || value === '') {
        return '0';
      }
      const num = Number(value);
      return Number.isInteger(num) ? String(num) : '0';
    };

    return {
      [TenantHeaderNames.AccountID]: formatTenantId(multitenancyHeaders?.AccountID),
      [TenantHeaderNames.ProjectID]: formatTenantId(multitenancyHeaders?.ProjectID),
    };
  }
}

import { css } from '@emotion/css';
import React, { Fragment, memo, useCallback } from 'react';

import { GrafanaTheme2, TimeRange } from '@grafana/data';
import { IconButton, Input, Label, Select, useStyles2 } from '@grafana/ui';

import { VictoriaLogsDatasource } from '../../../datasource';
import { FilterVisualQuery, LineFilterType, VisualQuery } from '../../../types';

import QueryBuilderAddFilter from './components/QueryBuilderAddFilter';
import QueryBuilderFieldFilter from './components/QueryBuilderFilters/QueryBuilderFieldFilter';
import QueryBuilderSelectOperator from './components/QueryBuilderOperators/QueryBuilderSelectOperator';
import { DEFAULT_FILTER_OPERATOR } from './utils/parseToString';

interface Props {
  query: VisualQuery;
  datasource: VictoriaLogsDatasource;
  timeRange?: TimeRange;
  onChange: (update: VisualQuery) => void;
  onRunQuery: () => void;
}

const MsgFilterConditionTypeOptions = [
  {
    label: 'Line contains',
    value: LineFilterType.Contains,
    description: '精确包含（区分大小写）→ _msg:"text"',
  },
  {
    label: 'Line does not contain',
    value: LineFilterType.NotContains,
    description: '精确不包含（区分大小写）→ _msg:!"text"',
  },
  {
    label: 'Line contains case insensitive',
    value: LineFilterType.ContainsCaseInsensitive,
    description: '包含（不区分大小写）→ _msg:~"(?i)text"',
  },
  {
    label: 'Line does not contain case insensitive',
    value: LineFilterType.NotContainsCaseInsensitive,
    description: '不包含（不区分大小写）→ _msg:!~"(?i)text"',
  },
  {
    label: 'Line contains regex match',
    value: LineFilterType.RegexMatch,
    description: '正则匹配 → _msg:~"regex"',
  },
  {
    label: 'Line does not match regex',
    value: LineFilterType.RegexNotMatch,
    description: '不匹配正则 → _msg:!~"regex"',
  },
  {
    label: 'IP line filter expression',
    value: LineFilterType.IpFilter,
    description: 'IP 地址过滤 → _msg:ip("ip_range")',
  },
];

const QueryBuilder = memo<Props>(({ datasource, query, onChange, timeRange }) => {
  const styles = useStyles2(getStyles);
  const { filters, msgFilters } = query;

  const handleAddMsgFilter = useCallback(() => {
    const newMsgFilters = [...(msgFilters || []), { text: '', type: LineFilterType.Contains }];
    onChange({ ...query, msgFilters: newMsgFilters });
  }, [onChange, query, msgFilters]);

  const handleRemoveMsgFilter = useCallback((index: number) => {
    const newMsgFilters = (msgFilters || []).filter((_, i) => i !== index);
    onChange({ ...query, msgFilters: newMsgFilters });
  }, [onChange, query, msgFilters]);

  const handleMsgFilterTextChange = useCallback((index: number, text: string) => {
    const newMsgFilters = [...(msgFilters || [])];
    newMsgFilters[index] = { ...newMsgFilters[index], text };
    onChange({ ...query, msgFilters: newMsgFilters });
  }, [onChange, query, msgFilters]);

  const handleMsgFilterTypeChange = useCallback((index: number, type: LineFilterType) => {
    const newMsgFilters = [...(msgFilters || [])];
    newMsgFilters[index] = { ...newMsgFilters[index], type };
    onChange({ ...query, msgFilters: newMsgFilters });
  }, [onChange, query, msgFilters]);

  // Ensure at least one msgFilter is displayed (like Loki does by default)
  const displayFilters = (msgFilters && msgFilters.length > 0) ? msgFilters : [{ text: '', type: LineFilterType.Contains }];
  const hasActualFilters = msgFilters && msgFilters.length > 0;

  return (
    <div className={styles.container}>
      {/* Label filters section - similar to Loki's visual-query-builder-dimensions-filter-item */}
      <div className={styles.section}>
        <Label className={styles.sectionLabel}>Label filters</Label>
        <div className={styles.filtersContainer}>
          <QueryBuilderFilter
            datasource={datasource}
            filters={filters}
            onChange={onChange}
            query={query}
            timeRange={timeRange}
            indexPath={[]}
          />
        </div>
      </div>

      {/* Line contains section - similar to Loki's operations.0.wrapper */}
      <div className={styles.section}>
        <Label className={styles.sectionLabel}>Line contains</Label>
        <div className={styles.lineContainsWrapper}>
          {displayFilters.map((filter, index) => (
            <div key={index} className={styles.msgFilterRow}>
              <Select
                options={MsgFilterConditionTypeOptions}
                value={filter.type ?? (filter.contains ? LineFilterType.Contains : LineFilterType.NotContains)}
                onChange={(e) => {
                  if (!hasActualFilters && index === 0) {
                    // First interaction - create the actual filter
                    onChange({ ...query, msgFilters: [{ text: filter.text, type: e.value as LineFilterType }] });
                  } else {
                    handleMsgFilterTypeChange(index, e.value as LineFilterType);
                  }
                }}
                width={30}
                menuShouldPortal
              />
              <Input
                placeholder="Text to find"
                value={filter.text}
                onChange={(e) => {
                  if (!hasActualFilters && index === 0) {
                    // First interaction - create the actual filter
                    onChange({ ...query, msgFilters: [{ text: e.currentTarget.value, type: filter.type || LineFilterType.Contains }] });
                  } else {
                    handleMsgFilterTextChange(index, e.currentTarget.value);
                  }
                }}
                className={styles.msgInput}
              />
              {hasActualFilters && (
                <IconButton
                  name="times"
                  tooltip="Remove"
                  size="sm"
                  onClick={() => handleRemoveMsgFilter(index)}
                />
              )}
              {index === displayFilters.length - 1 && (
                <IconButton
                  name="plus"
                  tooltip="Add line filter"
                  size="md"
                  onClick={handleAddMsgFilter}
                  variant="primary"
                />
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
});

interface QueryBuilderFilterProps {
  datasource: VictoriaLogsDatasource;
  query: VisualQuery;
  filters: FilterVisualQuery;
  indexPath: number[];
  timeRange?: TimeRange;
  onChange: (query: VisualQuery) => void;
}

const QueryBuilderFilter = (props: QueryBuilderFilterProps) => {
  const styles = useStyles2(getStyles);
  const { datasource, filters, query, indexPath, timeRange, onChange } = props;
  const isRoot = !indexPath.length;
  return (
    <div className={isRoot ? styles.filterGroup : styles.nestedFilterGroup}>
      {filters.values.map((filter, index) => (
        <Fragment key={index}>
          <div className={styles.filterItem}>
            {typeof filter === 'string'
              ?
              <QueryBuilderFieldFilter
                datasource={datasource}
                indexPath={[...indexPath, index]}
                filter={filter}
                query={query}
                timeRange={timeRange}
                onChange={onChange}
              />
              :
              <QueryBuilderFilter
                datasource={datasource}
                indexPath={[...indexPath, index]}
                filters={filter}
                query={query}
                timeRange={timeRange}
                onChange={onChange}
              />
            }
          </div>
          {index !== filters.values.length - 1 && (
            <QueryBuilderSelectOperator
              query={query}
              operator={filters.operators[index] || DEFAULT_FILTER_OPERATOR}
              indexPath={[...indexPath, index]}
              onChange={onChange}
            />
          )}
        </Fragment>
      )
      )}
      {/* for new filters*/}
      {!filters.values.length && (
        <QueryBuilderFieldFilter
          datasource={datasource}
          indexPath={[...indexPath, filters.values.length]}
          filter={''}
          query={query}
          timeRange={timeRange}
          onChange={onChange}
        />
      )}
      <QueryBuilderAddFilter query={query} onAddFilter={onChange} />
    </div>
  );
};

const getStyles = (theme: GrafanaTheme2) => {
  return {
    container: css`
      display: flex;
      flex-direction: column;
      gap: ${theme.spacing(2)};
    `,
    section: css`
      display: flex;
      flex-direction: column;
      gap: ${theme.spacing(0.5)};
    `,
    sectionLabel: css`
      font-weight: ${theme.typography.fontWeightBold};
      color: ${theme.colors.text.primary};
    `,
    filtersContainer: css`
      display: flex;
      flex-wrap: wrap;
      align-items: flex-start;
      gap: ${theme.spacing(1)};
    `,
    filterGroup: css`
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: ${theme.spacing(1)};
    `,
    nestedFilterGroup: css`
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: ${theme.spacing(1)};
      border: 1px solid ${theme.colors.border.strong};
      background-color: ${theme.colors.border.weak};
      padding: ${theme.spacing(1)};
    `,
    filterItem: css`
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: ${theme.spacing(1)};
    `,
    msgFiltersContainer: css`
      display: flex;
      flex-direction: column;
      gap: ${theme.spacing(0.5)};
    `,
    lineContainsWrapper: css`
      display: flex;
      flex-direction: column;
      gap: ${theme.spacing(0.5)};
    `,
    msgFilterRow: css`
      display: flex;
      align-items: center;
      gap: ${theme.spacing(0.5)};
    `,
    lineContainsButtons: css`
      display: flex;
      align-items: center;
      gap: ${theme.spacing(0.5)};
      margin-left: ${theme.spacing(0.5)};
    `,
    msgInput: css`
      flex: 1;
      min-width: 200px;
      max-width: 400px;
    `,
  };
};

QueryBuilder.displayName = 'QueryBuilder';

export default QueryBuilder;








import { css } from '@emotion/css';
import React, { useCallback, useMemo } from 'react';

import { GrafanaTheme2, SelectableValue, TimeRange } from '@grafana/data';
import { IconButton, Label, Select, useStyles2 } from '@grafana/ui';

import { VictoriaLogsDatasource } from '../../../../../datasource';
import { escapeLabelValueInExactSelector } from '../../../../../languageUtils';
import { normalizeKey } from '../../../../../modifyQuery';
import { VisualQuery } from '../../../../../types';
import { CompatibleCombobox } from '../../../../CompatibleCombobox';
import { deleteByIndexPath } from '../../utils/modifyFilterVisualQuery/deleteByIndexPath';
import { updateValueByIndexPath } from '../../utils/modifyFilterVisualQuery/updateByIndexPath';
import { DEFAULT_FIELD } from '../../utils/parseToString';

import { useFetchFilters } from './useFetchFilters';

type MatchOperator = '=' | '~' | '!~' | '!=';

const DEFAULT_MATCH_OPERATOR: MatchOperator = '=';

const MATCH_OPERATOR_OPTIONS: Array<SelectableValue<MatchOperator>> = [
  { label: '=', value: '=', description: 'Exact match' },
  { label: '!=', value: '!=', description: 'Not exact match' },
  { label: '=~', value: '~', description: 'Regexp match' },
  { label: '!~', value: '!~', description: 'Not regexp match' },
];

interface Props {
  datasource: VictoriaLogsDatasource;
  filter: string;
  query: VisualQuery;
  indexPath: number[];
  timeRange?: TimeRange;
  onChange: (query: VisualQuery) => void;
}

type SelectedOption = string | { value?: string; label?: string } | null;

const getSelectedOptionValue = (option: SelectedOption): string | undefined => {
  if (!option) {
    return undefined;
  }

  if (typeof option === 'string') {
    return option;
  }

  return option.value ?? option.label;
};

const QueryBuilderFieldFilter = ({ datasource, filter, query, indexPath, timeRange, onChange }: Props) => {
  const styles = useStyles2(getStyles);

  const { field, matchOp, fieldValue } = useMemo(() => {
    // Matches: field:value  |  field:~value  |  field:=value  |  field:!~value  |  field:!=value  |  field:"quoted"
    const regex = /("[^"]*"|'[^']*'|\S+)\s*:\s*(!~|!=|~|=)?\s*("[^"]*"|'[^']*'|\S+)?|\S+/i;
    const matches = filter.match(regex);
    if (!matches || matches.length < 1) {
      return { matchOp: DEFAULT_MATCH_OPERATOR };
    }
    const field = matches[1] || DEFAULT_FIELD;
    const matchOp = (matches[2] || DEFAULT_MATCH_OPERATOR) as MatchOperator;
    let fieldValue = matches[3] ?? (matches[1] ? '' : matches[0]);

    // Remove surrounding quotes from fieldValue
    if (
      fieldValue &&
      ((fieldValue.startsWith('"') && fieldValue.endsWith('"')) ||
        (fieldValue.startsWith("'") && fieldValue.endsWith("'")))
    ) {
      fieldValue = fieldValue.slice(1, -1);
    }

    return { field, matchOp, fieldValue };
  }, [filter]);

  const { loadFieldNames, loadFieldValues } = useFetchFilters({
    datasource,
    query,
    field,
    indexPath,
    timeRange,
  });

  const handleRemoveFilter = useCallback(() => {
    onChange({
      ...query,
      filters: deleteByIndexPath(query.filters, indexPath),
    });
  }, [onChange, query, indexPath]);

  const handleSelectFieldName = useCallback(
    (option: SelectedOption) => {
      const selectedField = getSelectedOptionValue(option);
      if (!selectedField) {
        return;
      }
      // Preserve current operator (or use default), clear field value when field name changes
      const op = matchOp || DEFAULT_MATCH_OPERATOR;
      const fullFilter = `${selectedField}:${op} `;

      onChange({
        ...query,
        filters: updateValueByIndexPath(query.filters, indexPath, fullFilter),
      });
    },
    [onChange, query, indexPath, matchOp]
  );

  const handleSelectMatchOperator = useCallback(
    ({ value }: SelectableValue<MatchOperator>) => {
      const op = value ?? DEFAULT_MATCH_OPERATOR;
      // Preserve field name and field value when operator changes
      let valueStr = '';
      if (fieldValue) {
        if (field === '_stream') {
          // Stream filters: use value as-is (no quotes)
          valueStr = fieldValue;
        } else if (op === '~' || op === '!~') {
          // Regexp: wrap value in double quotes
          valueStr = `"${fieldValue}"`;
        } else {
          valueStr = `"${escapeLabelValueInExactSelector(fieldValue)}"`;
        }
      }
      const fullFilter = `${normalizeKey(field || '')}:${op}${valueStr} `;
      onChange({
        ...query,
        filters: updateValueByIndexPath(query.filters, indexPath, fullFilter),
      });
    },
    [onChange, query, indexPath, field, fieldValue]
  );

  const handleSelectFieldValue = useCallback(
    (option: SelectedOption) => {
      const selectedFieldValue = getSelectedOptionValue(option);
      if (selectedFieldValue === undefined) {
        return;
      }
      const op = matchOp || DEFAULT_MATCH_OPERATOR;
      let valueStr: string;
      if (field === '_stream') {
        // Stream filters: use value as-is (no quotes)
        valueStr = selectedFieldValue;
      } else if (op === '~' || op === '!~') {
        // Regexp: wrap value in double quotes
        valueStr = `"${selectedFieldValue}"`;
      } else {
        valueStr = `"${escapeLabelValueInExactSelector(selectedFieldValue)}"`;
      }
      const fullFilter = `${normalizeKey(field || '')}:${op}${valueStr} `;

      onChange({
        ...query,
        filters: updateValueByIndexPath(query.filters, indexPath, fullFilter),
      });
    },
    [onChange, query, indexPath, field, matchOp]
  );

  return (
    <div className={styles.wrapper}>
      <div className={styles.header}>
        <Label>Filter</Label>
        <IconButton name={'times'} tooltip={'Remove filter'} size='sm' onClick={handleRemoveFilter} />
      </div>
      <div className={styles.content}>
        <CompatibleCombobox
          placeholder='Select field name'
          value={field ? { label: field, value: field } : null}
          options={loadFieldNames}
          onChange={handleSelectFieldName}
          width={'auto'}
          minWidth={10}
          createCustomValue
        />
        <Select
          width='auto'
          options={MATCH_OPERATOR_OPTIONS}
          value={matchOp}
          onChange={handleSelectMatchOperator}
          menuShouldPortal
        />
        <CompatibleCombobox
          key={`${field}-${matchOp}`}
          placeholder='Select field value'
          value={fieldValue ? { label: fieldValue, value: fieldValue } : null}
          options={loadFieldValues}
          onChange={handleSelectFieldValue}
          width={'auto'}
          minWidth={10}
          createCustomValue
        />
      </div>
    </div>
  );
};

const getStyles = (theme: GrafanaTheme2) => {
  return {
    wrapper: css`
      display: grid;
      gap: ${theme.spacing(0.5)};
      width: max-content;
      border: 1px solid ${theme.colors.border.strong};
      background-color: ${theme.colors.background.secondary};
      padding: ${theme.spacing(1)};
    `,
    header: css`
      display: flex;
      align-items: center;
      justify-content: space-between;
    `,
    content: css`
      display: flex;
      align-items: center;
      justify-content: center;
      gap: ${theme.spacing(0.5)};
    `,
  };
};

export default QueryBuilderFieldFilter;

import { CoreApp } from '@grafana/data';

import { PLUGIN_ID } from '../../constants';
import store from '../../store/store';
import { Query, QueryEditorMode, QueryType } from '../../types';

const queryEditorModeDefaultLocalStorageKey = `${PLUGIN_ID}:QueryEditorMode`;

export function getQueryWithDefaults(query: Query, app?: CoreApp, panelPluginId?: string): Query {
  let result = query;

  if (!query.editorMode) {
    result = { ...query, editorMode: getDefaultEditorMode(query.expr) };
  }

  if (!query.expr) {
    result = { ...result, expr: '' };
  }

  if (!query.queryType) {
    result = {
      ...result,
      queryType: getDefaultQueryTypeByPanel(panelPluginId) ?? getDefaultQueryTypeByApp(app),
    };
  }

  return result;
}

export function changeEditorMode(query: Query, editorMode: QueryEditorMode, onChange: (query: Query) => void) {
  if (query.expr === '') {
    store.set(queryEditorModeDefaultLocalStorageKey, editorMode);
  }

  onChange({ ...query, editorMode });
}

export function getDefaultEditorMode(expr: string) {
  if (expr != null && expr !== '') {
    return QueryEditorMode.Code;
  }

  const value = store.get(queryEditorModeDefaultLocalStorageKey);
  // Default to Builder mode instead of Code
  return value === QueryEditorMode.Code ? QueryEditorMode.Code : QueryEditorMode.Builder;
}

function getDefaultQueryTypeByPanel(panelPluginId?: string) {
  switch (panelPluginId) {
    case 'logs':
    case 'table':
      return QueryType.Instant;
    case 'timeseries':
      return QueryType.StatsRange;
    default:
      return null;
  }
}

function getDefaultQueryTypeByApp(app?: CoreApp) {
  switch (app) {
    case CoreApp.Explore:
      return QueryType.Instant;
    default:
      return QueryType.StatsRange;
  }
}

import React, { useCallback, useMemo } from 'react';

import { AsyncSelect, Combobox, Select, SelectValue } from '@grafana/ui';

/**
 * A compatibility wrapper for static select that uses Combobox in Grafana 11+
 * and Select in older versions.
 */
export const CompatibleCombobox: typeof Combobox = (props) => {
  // Normalize value to Select format
  const normalizedValue = useMemo<SelectValue<any>>(() => {
    if (!props.value) {
      return null;
    }
    if (typeof props.value === 'string') {
      return { value: props.value, label: props.value };
    }
    return props.value;
  }, [props.value]);


  const normalizeSelected = useCallback((selected: SelectValue<any> | string | null): SelectValue<any> | null => {
    if (!selected) {
      return null;
    }

    if (typeof selected === 'string') {
      return { value: selected, label: selected };
    }

    const normalizedSelectedValue = selected.value ?? selected.label;
    if (normalizedSelectedValue === undefined) {
      return null;
    }

    return {
      value: normalizedSelectedValue,
      label: selected.label ?? String(normalizedSelectedValue),
      description: selected.description,
    };
  }, []);

  const handleSelectChange = useCallback((selected: SelectValue<any> | string | null) => {
    const normalizedSelected = normalizeSelected(selected);
    if (!normalizedSelected) {
      props.onChange(null as any);
      return;
    }

    props.onChange({
      value: normalizedSelected.value,
      label: normalizedSelected.label ?? normalizedSelected.value,
      description: normalizedSelected.description,
    } as any);
  }, [normalizeSelected, props]);

  const asyncOption = useCallback((value: SelectValue<any>) => {
    if (typeof props.options === 'function') {
      return props.options(value);
    }
    return Promise.resolve([]);
  }, [props]);

  const selectOptions = useMemo(() => {
    if (Array.isArray(props.options)) {
      return props.options.map<SelectValue<any>>((opt) => ({
        value: opt.value,
        label: opt.label,
        description: opt.description,
      }));
    }

    return asyncOption;
  }, [asyncOption, props.options]);

  if (Combobox) {
    return (
      <Combobox {...props} value={normalizedValue} onChange={handleSelectChange as any} />
    );
  }

  if (typeof selectOptions === 'function') {
    return (
      <AsyncSelect
        placeholder={props.placeholder}
        width={props.width}
        value={normalizedValue}
        loadOptions={selectOptions}
        defaultOptions
        allowCustomValue={props.createCustomValue}
        onChange={handleSelectChange}
        isClearable={props.isClearable}
        isLoading={props.loading}
        disabled={props.disabled}
      />
    );
  }

  return (
    <Select
      placeholder={props.placeholder}
      width={props.width}
      value={normalizedValue}
      options={selectOptions as any}
      allowCustomValue={props.createCustomValue}
      onChange={handleSelectChange}
      isClearable={props.isClearable}
      isLoading={props.loading}
      disabled={props.disabled}
    />
  );
};

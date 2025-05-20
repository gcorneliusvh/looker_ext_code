// src/FieldDisplayConfigurator.js
import React, { useState, useEffect } from 'react';
import {
    Box,
    Button,
    Checkbox,
    Dialog,
    DialogLayout,
    Heading,
    IconButton,
    Select,
    Space,
} from '@looker/components';
import { Close } from '@styled-icons/material/Close';

const NUMERIC_TYPES = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"];

const ALIGNMENT_OPTIONS = [
    { value: '', label: 'Default' }, { value: 'left', label: 'Left' },
    { value: 'center', label: 'Center' }, { value: 'right', label: 'Right' },
];

const NUMBER_FORMAT_OPTIONS = [
    { value: '', label: 'Default' }, { value: 'INTEGER', label: 'Integer (0 dec)' },
    { value: 'DECIMAL_2', label: 'Number (2 dec)' }, { value: 'USD', label: 'Currency (USD)' },
    { value: 'EUR', label: 'Currency (EUR)' }, { value: 'PERCENT_2', label: 'Percentage (x.xx%)' },
];

// For String fields: determines if they trigger subtotals/totals
const GROUP_SUMMARY_ACTION_OPTIONS = [
    { value: '', label: 'None' },
    { value: 'GRAND_TOTAL_ONLY', label: 'Trigger Grand Totals Only' }, // e.g., show grand totals at the end if this field is present
    { value: 'SUBTOTAL_ONLY', label: 'Trigger Subtotals on Change' },
    { value: 'SUBTOTAL_AND_GRAND_TOTAL', label: 'Trigger Subtotals & Grand Totals' },
];

// For Numeric fields: determines how they aggregate in summaries
const NUMERIC_AGGREGATION_OPTIONS = [
    { value: '', label: 'None' }, // Default, no specific aggregation for summaries
    { value: 'SUM', label: 'Sum' },
    { value: 'AVERAGE', label: 'Average' },
    { value: 'MIN', label: 'Minimum' },
    { value: 'MAX', label: 'Maximum' },
];

const REPEAT_GROUP_VALUE_OPTIONS = [
    { value: 'REPEAT', label: 'Repeat Group Value on Each Row' },
    { value: 'SHOW_ON_CHANGE', label: 'Show Group Value Only on Change' },
];


function FieldDisplayConfigurator({ schema, isOpen, onClose, onApply, reportName, initialConfigs = [] }) {
    const [fieldConfigs, setFieldConfigs] = useState({});

    useEffect(() => {
        const newConfigs = {};
        if (schema && isOpen && schema.length > 0) {
            schema.forEach(field => {
                const existingConfig = initialConfigs.find(c => c.field_name === field.name);
                const defaultConfig = {
                    includeInBody: true, includeAtTop: false, includeInHeader: false,
                    contextNote: '', alignment: '', numberFormat: '',
                    groupSummaryAction: '', // For string fields (renamed from subtotalAction)
                    repeatGroupValue: 'REPEAT',
                    numericAggregation: '', // For numeric fields
                };
                if (existingConfig) {
                    newConfigs[field.name] = {
                        ...defaultConfig,
                        ...existingConfig,
                        includeInBody: existingConfig.include_in_body !== undefined ? existingConfig.include_in_body : defaultConfig.includeInBody,
                        includeAtTop: existingConfig.include_at_top !== undefined ? existingConfig.include_at_top : defaultConfig.includeAtTop,
                        includeInHeader: existingConfig.include_in_header !== undefined ? existingConfig.include_in_header : defaultConfig.includeInHeader,
                        groupSummaryAction: existingConfig.group_summary_action || existingConfig.subtotal_action || defaultConfig.groupSummaryAction, // Handle old subtotal_action name
                        repeatGroupValue: existingConfig.repeat_group_value || defaultConfig.repeatGroupValue,
                        numericAggregation: existingConfig.numeric_aggregation || defaultConfig.numericAggregation,
                    };
                } else {
                    newConfigs[field.name] = defaultConfig;
                }
            });
        }
        setFieldConfigs(newConfigs);
    }, [schema, isOpen, initialConfigs]);

    const handleFieldConfigChange = (fieldName, configKey, value) => {
        setFieldConfigs(prev => {
            const updatedField = {
                ...prev[fieldName],
                [configKey]: value,
            };
            // If groupSummaryAction is set to None, reset repeatGroupValue
            if (configKey === 'groupSummaryAction' && !value) {
                updatedField.repeatGroupValue = 'REPEAT';
            }
            return {
                ...prev,
                [fieldName]: updatedField,
            };
        });
    };

    const handleApplyConfigs = () => {
        const configsToApply = Object.keys(fieldConfigs).map(fieldName => ({
            field_name: fieldName,
            include_in_body: fieldConfigs[fieldName].includeInBody,
            include_at_top: fieldConfigs[fieldName].includeAtTop,
            include_in_header: fieldConfigs[fieldName].includeInHeader,
            context_note: fieldConfigs[fieldName].contextNote || null,
            alignment: fieldConfigs[fieldName].alignment || null,
            number_format: fieldConfigs[fieldName].numberFormat || null,
            group_summary_action: fieldConfigs[fieldName].groupSummaryAction || null, // Renamed
            repeat_group_value: fieldConfigs[fieldName].groupSummaryAction ? fieldConfigs[fieldName].repeatGroupValue : null,
            numeric_aggregation: fieldConfigs[fieldName].numericAggregation || null, // New field
        }));
        onApply(configsToApply);
    };

    if (!isOpen) return null;

    const thStyle = { fontWeight: 'bold', padding: '8px 4px', textAlign: 'left', borderBottom: '1px solid #ccc', fontSize: '0.85em', verticalAlign: 'bottom' };
    const tdStyle = { padding: '6px 4px', borderBottom: '1px solid #eee', verticalAlign: 'top', fontSize: '0.9em' };
    const smallTextStyle = { fontSize: '0.8em', color: '#666' };
    const selectStyle = { width: '100%', height: '36px', fontSize: '0.9em', marginTop: '2px', boxSizing: 'border-box'};
    const textAreaStyle = { ...selectStyle, minHeight: '40px', padding: '5px', border: '1px solid #ccc', borderRadius: '3px' };

    const dialogHeader = (
        <Space between alignItems="center" p="medium" style={{ borderBottom: '1px solid #ccc' }}>
            <Heading as="h3" fontWeight="semiBold" style={{ margin: 0 }}>
                Configure Column Display for: {reportName || "Report"}
            </Heading>
            <IconButton icon={<Close />} label="Close Dialog" onClick={onClose} size="small" />
        </Space>
    );

    const dialogFooter = (
        <Space between p="medium" style={{ borderTop: '1px solid #ccc' }}>
            <Button onClick={onClose} id="fdc-cancel-button">Cancel</Button>
            <Button color="key" onClick={handleApplyConfigs} id="fdc-apply-button">Apply Configurations</Button>
        </Space>
    );

    return (
        <Dialog isOpen={isOpen} onClose={onClose} maxWidth="98vw" width="1800px"> {/* Wider dialog for new column */}
            <DialogLayout
                header={dialogHeader}
                footer={dialogFooter}
            >
                <Box p="medium" style={{ maxHeight: '80vh', overflowY: 'auto' }}>
                    <p style={{ marginBottom: '16px' }}>
                        Configure display, styling, and summary actions for each field.
                    </p>
                    {schema && schema.length > 0 ? (
                        <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'fixed' }}>
                            <thead>
                                <tr>
                                    <th style={{...thStyle, width: "15%"}}>Field (Type)</th>
                                    <th style={{...thStyle, textAlign: 'center', width: "5%"}}>In Body</th>
                                    <th style={{...thStyle, textAlign: 'center', width: "5%"}}>At Top</th>
                                    <th style={{...thStyle, textAlign: 'center', width: "5%"}}>In Header</th>
                                    <th style={{...thStyle, width: "10%"}}>Alignment</th>
                                    <th style={{...thStyle, width: "13%"}}>Number Format</th>
                                    <th style={{...thStyle, width: "17%"}}>Subtotal/Total Trigger (String Fields)</th>
                                    <th style={{...thStyle, width: "15%"}}>Numeric Aggregation (Numeric Fields)</th>
                                    <th style={{...thStyle, width: "15%"}}>Context Note</th>
                                </tr>
                            </thead>
                            <tbody>
                                {schema.map((field) => {
                                    const currentConfig = fieldConfigs[field.name] || {};
                                    const isNumeric = NUMERIC_TYPES.includes(field.type.toUpperCase());
                                    const isString = field.type.toUpperCase() === 'STRING';

                                    return (
                                        <tr key={field.name}>
                                            <td style={tdStyle}>
                                                <span style={{ fontWeight: '500', display: 'block', wordBreak: 'break-word' }}>{field.name}</span>
                                                <span style={smallTextStyle}>({field.type}{field.mode === 'REPEATED' ? ' ARRAY' : ''})</span>
                                            </td>
                                            <td style={{...tdStyle, textAlign: 'center'}}>
                                                <input type="checkbox" checked={!!currentConfig.includeInBody}
                                                    onChange={(e) => handleFieldConfigChange(field.name, 'includeInBody', e.target.checked)} />
                                            </td>
                                            <td style={{...tdStyle, textAlign: 'center'}}>
                                                <input type="checkbox" checked={!!currentConfig.includeAtTop}
                                                    onChange={(e) => handleFieldConfigChange(field.name, 'includeAtTop', e.target.checked)} />
                                            </td>
                                            <td style={{...tdStyle, textAlign: 'center'}}>
                                                <input type="checkbox" checked={!!currentConfig.includeInHeader}
                                                    onChange={(e) => handleFieldConfigChange(field.name, 'includeInHeader', e.target.checked)} />
                                            </td>
                                            <td style={tdStyle}>
                                                <Select options={ALIGNMENT_OPTIONS} value={currentConfig.alignment || ''}
                                                    onChange={(value) => handleFieldConfigChange(field.name, 'alignment', value)}
                                                    placeholder="Default" style={selectStyle} />
                                            </td>
                                            <td style={tdStyle}>
                                                {isNumeric ? (
                                                    <Select options={NUMBER_FORMAT_OPTIONS} value={currentConfig.numberFormat || ''}
                                                        onChange={(value) => handleFieldConfigChange(field.name, 'numberFormat', value)}
                                                        placeholder="Default" disabled={!isNumeric} style={selectStyle} />
                                                ) : (<span style={smallTextStyle}>N/A</span>)}
                                            </td>
                                            <td style={tdStyle}> {/* Column for String field summary actions */}
                                                {isString ? (
                                                    <>
                                                        <Select options={GROUP_SUMMARY_ACTION_OPTIONS} value={currentConfig.groupSummaryAction || ''}
                                                            onChange={(value) => handleFieldConfigChange(field.name, 'groupSummaryAction', value)}
                                                            placeholder="None" style={selectStyle} />
                                                        {currentConfig.groupSummaryAction && (
                                                            <Select options={REPEAT_GROUP_VALUE_OPTIONS}
                                                                value={currentConfig.repeatGroupValue || 'REPEAT'}
                                                                onChange={(value) => handleFieldConfigChange(field.name, 'repeatGroupValue', value)}
                                                                style={{...selectStyle, marginTop: '5px'}} />
                                                        )}
                                                    </>
                                                ) : (<span style={smallTextStyle}>N/A for non-string</span>)}
                                            </td>
                                            <td style={tdStyle}> {/* New Column for Numeric field aggregation actions */}
                                                {isNumeric ? (
                                                    <Select options={NUMERIC_AGGREGATION_OPTIONS} value={currentConfig.numericAggregation || ''}
                                                        onChange={(value) => handleFieldConfigChange(field.name, 'numericAggregation', value)}
                                                        placeholder="None" style={selectStyle} />
                                                ) : (<span style={smallTextStyle}>N/A for non-numeric</span>)}
                                            </td>
                                            <td style={tdStyle}>
                                                <textarea value={currentConfig.contextNote || ''}
                                                    onChange={(e) => handleFieldConfigChange(field.name, 'contextNote', e.target.value)}
                                                    placeholder="e.g., 'Use as report title'" rows={2} style={textAreaStyle} />
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    ) : ( <p>No schema fields found...</p> )}
                </Box>
            </DialogLayout>
        </Dialog>
    );
}
export default FieldDisplayConfigurator;
// src/PlaceholderMappingDialog.js
import React, { useState, useEffect, useMemo } from 'react';
import {
    Dialog,
    DialogLayout,
    Heading,
    IconButton,
    Button,
    Select,
    FieldText,
    Box,
    Paragraph,
    Space,
} from '@looker/components';
import { Close } from '@styled-icons/material';

// --- Re-usable styles for the table ---
const tableStyle = { width: '100%', borderCollapse: 'collapse', marginBottom: '24px', fontSize: '0.875rem' };
const thStyle = { borderBottom: '1px solid #dee2e6', padding: '12px 16px', textAlign: 'left', fontWeight: '600', backgroundColor: '#f7f8fa', lineHeight: '1.5' };
const tdStyle = { borderBottom: '1px solid #dee2e6', padding: '12px 16px', verticalAlign: 'top', lineHeight: '1.5' };

function PlaceholderMappingDialog({
    isOpen,
    onClose,
    reportName,
    discoveredPlaceholders = [],
    schema = [],
    lookConfigs = [],
    filterConfigs = [],
    onApplyMappings
}) {
    const [mappings, setMappings] = useState({});

    useEffect(() => {
        if (isOpen) {
            const initialMappings = {};
            discoveredPlaceholders.forEach(p => {
                let defaultMapType = 'ignore';
                let defaultSchemaField = '';
                let defaultLookPlaceholder = '';
                let defaultFilterKey = '';

                // Use the new, smarter suggestions from the backend to set defaults
                if (p.suggestion) {
                    switch (p.suggestion.map_to_type) {
                        case 'standardize_top':
                            defaultMapType = 'standardize_top';
                            defaultSchemaField = p.suggestion.map_to_value || '';
                            break;
                        case 'standardize_header':
                            defaultMapType = 'standardize_header';
                            defaultSchemaField = p.suggestion.map_to_value || '';
                            break;
                        case 'map_to_look':
                            defaultMapType = 'map_to_look';
                            defaultLookPlaceholder = p.suggestion.map_to_value || '';
                            break;
                        case 'map_to_filter':
                             defaultMapType = 'map_to_filter';
                             defaultFilterKey = p.suggestion.map_to_value || '';
                             break;
                        default:
                            defaultMapType = 'ignore';
                    }
                }

                initialMappings[p.original_tag] = {
                    original_tag: p.original_tag,
                    key_in_tag: p.key_in_tag,
                    map_type: defaultMapType,
                    map_to_schema_field: defaultSchemaField,
                    map_to_look_placeholder: defaultLookPlaceholder,
                    map_to_filter_key: defaultFilterKey,
                    static_text_value: '',
                    fallback_value: '',
                };
            });
            setMappings(initialMappings);
        }
    }, [isOpen, discoveredPlaceholders]);

    const handleMappingChange = (placeholderTag, field, value) => {
        setMappings(prev => ({
            ...prev,
            [placeholderTag]: { ...prev[placeholderTag], [field]: value },
        }));
    };

    const handleApply = () => {
        const mappingsToApply = Object.values(mappings).map(m => ({
            original_tag: m.original_tag,
            map_type: m.map_type,
            map_to_schema_field: (m.map_type === 'standardize_top' || m.map_type === 'standardize_header') ? m.map_to_schema_field : null,
            map_to_look_placeholder: m.map_type === 'map_to_look' ? m.map_to_look_placeholder : null,
            map_to_filter_key: m.map_type === 'map_to_filter' ? m.map_to_filter_key : null,
            static_text_value: m.map_type === 'static_text' ? m.static_text_value : null,
            fallback_value: (m.map_type === 'standardize_top' || m.map_type === 'standardize_header') ? (m.fallback_value || null) : null,
        }));
        onApplyMappings(reportName, mappingsToApply);
    };

    const schemaFieldOptions = useMemo(() => schema.map(f => ({ value: f.name, label: f.name })), [schema]);
    const lookOptions = useMemo(() => lookConfigs.map(lc => ({ value: lc.placeholder_name, label: `Look: ${lc.placeholder_name}` })), [lookConfigs]);
    const filterOptions = useMemo(() => filterConfigs.map(fc => ({ value: fc.ui_filter_key, label: `Filter: ${fc.ui_label}` })), [filterConfigs]);

    const mappingTypeOptions = [
        { value: 'ignore', label: 'Ignore (Remove Placeholder)' },
        { value: 'static_text', label: 'Set Static Text' },
        { value: 'standardize_top', label: 'Map to TOP Field ({{TOP_FieldName}})' },
        { value: 'standardize_header', label: 'Map to HEADER Field ({{HEADER_FieldName}})' },
        { value: 'map_to_look', label: 'Map to Look Chart' },
        { value: 'map_to_filter', label: 'Map to Filter Value' },
    ];

    if (!isOpen) return null;

    const editablePlaceholders = discoveredPlaceholders.filter(p => !p.key_in_tag.startsWith('TABLE_ROWS_'));

    return (
        <Dialog isOpen={isOpen} onClose={onClose} maxWidth="90vw" width="1200px">
            <DialogLayout
                header={
                    <Box display="flex" justifyContent="space-between" alignItems="center" p="medium" borderBottom="1px solid default">
                        <Heading as="h3" fontWeight="semiBold">Configure Placeholders for: {reportName}</Heading>
                        <IconButton icon={<Close />} label="Close" onClick={onClose} size="small" />
                    </Box>
                }
                footer={
                    <Space between p="medium" borderTop="1px solid default">
                        <Button onClick={onClose}>Cancel</Button>
                        <Button color="key" onClick={handleApply} disabled={Object.keys(mappings).length === 0}>
                            Save Mappings & Finalize Template
                        </Button>
                    </Space>
                }
            >
                <Box p="large" style={{ maxHeight: '70vh', overflowY: 'auto' }}>
                    <Paragraph fontSize="small" color="text2" mb="large">
                        Review placeholders found in the generated HTML template. Map them to data sources, provide static content, or ignore them.
                        Table row placeholders are handled automatically.
                    </Paragraph>
                    {editablePlaceholders.length === 0 ? (
                        <Paragraph>No editable placeholders were discovered in this template.</Paragraph>
                    ) : (
                        <table style={tableStyle}>
                            <thead>
                                <tr>
                                    <th style={{...thStyle, width: '25%'}}>Placeholder</th>
                                    <th style={{...thStyle, width: '25%'}}>Detected Status / Suggestion</th>
                                    <th style={{...thStyle, width: '50%'}}>Action / Configuration</th>
                                </tr>
                            </thead>
                            <tbody>
                                {editablePlaceholders.map((p) => {
                                    const currentMapping = mappings[p.original_tag] || {};
                                    return (
                                        <tr key={p.original_tag}>
                                            <td style={tdStyle}>
                                                <Box style={{ wordBreak: 'break-all' }}>
                                                    <code style={{fontSize: '0.9em', backgroundColor: '#f0f0f0', padding: '2px 4px', borderRadius: '3px'}}>{p.original_tag}</code>
                                                </Box>
                                            </td>
                                            <td style={tdStyle}>
                                                <Box fontSize="small" color={p.status.startsWith('auto_matched') ? 'positive' : (p.status === 'unrecognized' ? 'warn' : 'text2')}>
                                                    {p.status.replace(/_/g, ' ')}
                                                </Box>
                                                {p.suggestion && (
                                                    <Box mt="xxsmall" fontSize="xsmall" color="text2">
                                                        Suggests: {p.suggestion.map_to_value}
                                                    </Box>
                                                )}
                                            </td>
                                            <td style={tdStyle}>
                                                <Select
                                                    options={mappingTypeOptions}
                                                    value={currentMapping.map_type}
                                                    onChange={(value) => handleMappingChange(p.original_tag, 'map_type', value)}
                                                    mb="small" width="100%"
                                                />
                                                {(currentMapping.map_type === 'standardize_top' || currentMapping.map_type === 'standardize_header') && (
                                                    <Select
                                                        options={[{label: '--- Select Schema Field ---', value: ''}, ...schemaFieldOptions]}
                                                        value={currentMapping.map_to_schema_field}
                                                        onChange={(value) => handleMappingChange(p.original_tag, 'map_to_schema_field', value)}
                                                        placeholder="Select Schema Field"
                                                        mb="small" width="100%"
                                                    />
                                                )}
                                                {currentMapping.map_type === 'map_to_look' && (
                                                    <Select
                                                        options={[{label: '--- Select Look Placeholder ---', value: ''}, ...lookOptions]}
                                                        value={currentMapping.map_to_look_placeholder}
                                                        onChange={(value) => handleMappingChange(p.original_tag, 'map_to_look_placeholder', value)}
                                                        width="100%"
                                                    />
                                                )}
                                                 {currentMapping.map_type === 'map_to_filter' && (
                                                    <Select
                                                        options={[{label: '--- Select Filter ---', value: ''}, ...filterOptions]}
                                                        value={currentMapping.map_to_filter_key}
                                                        onChange={(value) => handleMappingChange(p.original_tag, 'map_to_filter_key', value)}
                                                        width="100%"
                                                    />
                                                )}
                                                {currentMapping.map_type === 'static_text' && (
                                                    <FieldText
                                                        label="Static Text Value"
                                                        value={currentMapping.static_text_value}
                                                        onChange={(e) => handleMappingChange(p.original_tag, 'static_text_value', e.target.value)}
                                                        width="100%"
                                                    />
                                                )}
                                            </td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    )}
                </Box>
            </DialogLayout>
        </Dialog>
    );
}

export default PlaceholderMappingDialog;
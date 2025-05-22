// src/PlaceholderMappingDialog.js
import React, { useState, useEffect } from 'react';
import {
    Dialog,
    DialogLayout,
    Heading,
    IconButton,
    Button,
    Select,         // Looker Component for dropdowns
    FieldText,      // Looker Component for text inputs
    Box,            // Looker Component for layout
    Paragraph,
    Space,
    // Table, TableHead, TableBody, TableRow, TableHeaderCell, TableDataCell are REMOVED
} from '@looker/components';
import { Close } from '@styled-icons/material';

// Define some basic styles for the HTML table elements to mimic Looker's general feel
const tableStyle = {
    width: '100%',
    borderCollapse: 'collapse',
    marginBottom: '24px', // Equivalent to mb="xlarge"
    fontSize: '0.875rem', // Common base font size in Looker UIs
};

const thStyle = {
    borderBottom: '1px solid #dee2e6', // A common border color
    padding: '12px 16px',             // Spacing similar to Looker's TableHeaderCell
    textAlign: 'left',
    fontWeight: '600', // semiBold
    backgroundColor: '#f7f8fa', // Light background for header
    lineHeight: '1.5',
};

const tdStyle = {
    borderBottom: '1px solid #dee2e6',
    padding: '12px 16px',              // Spacing similar to Looker's TableDataCell
    verticalAlign: 'top',              // Important for multi-line content in cells
    lineHeight: '1.5',
};

function PlaceholderMappingDialog({
    isOpen,
    onClose,
    reportName,
    discoveredPlaceholders = [],
    schema = [],
    onApplyMappings // Expects (reportName, mappingsToApply)
}) {
    const [mappings, setMappings] = useState({});

    useEffect(() => {
        if (isOpen) {
            const initialMappings = {};
            discoveredPlaceholders.forEach(p => {
                let defaultMapType = 'ignore';
                let defaultSchemaField = '';

                if (p.suggestion) {
                    if (p.status === 'auto_matched_top') {
                        defaultMapType = 'standardize_top';
                        defaultSchemaField = p.suggestion.map_to_value || '';
                    } else if (p.status === 'auto_matched_header') {
                        defaultMapType = 'standardize_header';
                        defaultSchemaField = p.suggestion.map_to_value || '';
                    }
                }

                initialMappings[p.original_tag] = {
                    original_tag: p.original_tag,
                    key_in_tag: p.key_in_tag,
                    map_type: defaultMapType,
                    map_to_schema_field: defaultSchemaField,
                    static_text_value: '',
                    fallback_value: '',
                };
            });
            setMappings(initialMappings);
        }
    }, [isOpen, discoveredPlaceholders]);

    const handleMappingChange = (placeholderTag, field, value) => {
        setMappings(prev => {
            const updatedMapping = { ...prev[placeholderTag], [field]: value };
            if (field === 'map_type') {
                if (value !== 'schema_field' && value !== 'standardize_top' && value !== 'standardize_header') {
                    updatedMapping.map_to_schema_field = '';
                    updatedMapping.fallback_value = '';
                }
                if (value !== 'static_text') {
                    updatedMapping.static_text_value = '';
                }
            }
            return { ...prev, [placeholderTag]: updatedMapping };
        });
    };

    const handleApply = () => {
        const mappingsToApply = Object.values(mappings)
            .filter(m => m.map_type !== 'ignore' || (m.map_type === 'ignore' && m.original_tag))
            .map(m => ({
                original_tag: m.original_tag,
                map_type: m.map_type,
                map_to_schema_field: (m.map_type === 'schema_field' || m.map_type === 'standardize_top' || m.map_type === 'standardize_header') ? m.map_to_schema_field : null,
                static_text_value: m.map_type === 'static_text' ? m.static_text_value : null,
                fallback_value: (m.map_type === 'schema_field' || m.map_type === 'standardize_top' || m.map_type === 'standardize_header') ? (m.fallback_value || null) : null,
            }));
        
        onApplyMappings(reportName, mappingsToApply);
    };

    const schemaFieldOptions = schema.map(f => ({ value: f.name, label: f.name }));

    const mappingTypeOptions = [
        { value: 'ignore', label: 'Ignore (Remove Placeholder)' },
        { value: 'static_text', label: 'Set Static Text' },
        { value: 'standardize_top', label: 'Use as TOP Field ({{TOP_FieldName}})' },
        { value: 'standardize_header', label: 'Use as HEADER Field ({{HEADER_FieldName}})' },
    ];

    if (!isOpen) return null;

    const editablePlaceholders = discoveredPlaceholders.filter(p => p.key_in_tag !== 'TABLE_ROWS_HTML_PLACEHOLDER');

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
                        The placeholder `{'{{TABLE_ROWS_HTML_PLACEHOLDER}}'}` is handled automatically.
                    </Paragraph>
                    {editablePlaceholders.length === 0 ? (
                        <Paragraph>No editable placeholders were discovered in this template, or the discovery process failed.</Paragraph>
                    ) : (
                        <table style={tableStyle}>
                            <thead>
                                <tr>
                                    <th style={{...thStyle, width: '30%'}}>Placeholder in Template</th>
                                    <th style={{...thStyle, width: '20%'}}>Detected Status / Suggestion</th>
                                    <th style={{...thStyle, width: '50%'}}>Action / Configuration</th>
                                </tr>
                            </thead>
                            <tbody>
                                {editablePlaceholders.map((p) => {
                                    const currentMapping = mappings[p.original_tag] || { map_type: 'ignore', map_to_schema_field: '', static_text_value: '', fallback_value: '' };
                                    
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
                                                        Suggests: {p.suggestion.map_to_type?.replace(/_/g, ' ')}
                                                        {p.suggestion.map_to_value && ` -> '${p.suggestion.map_to_value}'`}
                                                        {p.suggestion.usage_as && ` (as ${p.suggestion.usage_as})`}
                                                    </Box>
                                                )}
                                            </td>
                                            <td style={tdStyle}>
                                                <Select
                                                    options={mappingTypeOptions}
                                                    value={currentMapping.map_type}
                                                    onChange={(value) => handleMappingChange(p.original_tag, 'map_type', value)}
                                                    mb="small" // Looker Component prop
                                                    width="100%"
                                                />
                                                {(currentMapping.map_type === 'standardize_top' || currentMapping.map_type === 'standardize_header') && (
                                                    <>
                                                        <Select
                                                            options={[{label: '--- Select Schema Field ---', value: ''}, ...schemaFieldOptions]}
                                                            value={currentMapping.map_to_schema_field}
                                                            onChange={(value) => handleMappingChange(p.original_tag, 'map_to_schema_field', value)}
                                                            placeholder="Select Schema Field"
                                                            mb="small" // Looker Component prop
                                                            width="100%"
                                                            disabled={!currentMapping.map_type.startsWith('standardize')}
                                                        />
                                                        <FieldText
                                                            label="Fallback Value (if field data is empty)"
                                                            value={currentMapping.fallback_value}
                                                            onChange={(e) => handleMappingChange(p.original_tag, 'fallback_value', e.target.value)}
                                                            fontSize="small"
                                                            detail="Optional: Text to use if the mapped schema field has no value."
                                                            width="100%"
                                                        />
                                                    </>
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
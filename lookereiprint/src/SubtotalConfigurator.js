// src/SubtotalConfigurator.js
import React, { useState } from 'react';
import {
    Box,
    Button,
    FieldText,
    Select,
    Heading,
    IconButton,
    Text
} from '@looker/components';
import { Add, Delete } from '@styled-icons/material'; // Using material icons
import { v4 as uuidv4 } from 'uuid'; // For generating unique IDs

const CALCULATION_TYPE_OPTIONS = [
    { value: 'SUM', label: 'Sum' },
    { value: 'AVERAGE', label: 'Average' },
    { value: 'COUNT', label: 'Count (all in group)' },
    { value: 'COUNT_DISTINCT', label: 'Count Distinct (of target field)' },
    { value: 'MIN', label: 'Minimum' },
    { value: 'MAX', label: 'Maximum' },
];

// Re-using from FieldDisplayConfigurator or define locally if preferred
const NUMERIC_TYPES = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"];
const ALIGNMENT_OPTIONS = [
    { value: '', label: 'Default' },
    { value: 'left', label: 'Left' },
    { value: 'center', label: 'Center' },
    { value: 'right', label: 'Right' },
];
const NUMBER_FORMAT_OPTIONS = [
    { value: '', label: 'Default (As Is)' },
    { value: 'INTEGER', label: 'Integer (0 decimals)' },
    { value: 'DECIMAL_2', label: 'Number (2 decimals)' },
    { value: 'USD', label: 'Currency (USD $x,xxx.xx)' },
    { value: 'EUR', label: 'Currency (EUR €x,xxx.xx)' },
    { value: 'PERCENT_2', label: 'Percentage (x.xx%)' },
];


function SubtotalConfigurator({ schema, subtotalConfigs, setSubtotalConfigs }) {

    const schemaFieldsForGrouping = schema.filter(field => field.type.toUpperCase() === 'STRING').map(field => ({ value: field.name, label: field.name }));
    const schemaFieldsForCalculation = schema.map(field => ({ value: field.name, label: field.name }));
    const numericSchemaFieldsForCalculation = schema.filter(field => NUMERIC_TYPES.includes(field.type.toUpperCase())).map(field => ({ value: field.name, label: field.name }));


    const handleAddSubtotalGroup = () => {
        setSubtotalConfigs(prev => [
            ...prev,
            {
                id: uuidv4(),
                groupByFieldName: schemaFieldsForGrouping.length > 0 ? schemaFieldsForGrouping[0].value : '',
                valuesPlaceholderName: `SUBTOTAL_GROUP_${prev.length + 1}_VALUES`, // Auto-generate placeholder
                calculatedValues: [],
            }
        ]);
    };

    const handleRemoveSubtotalGroup = (groupIndex) => {
        setSubtotalConfigs(prev => prev.filter((_, index) => index !== groupIndex));
    };

    const handleSubtotalGroupChange = (groupIndex, key, value) => {
        setSubtotalConfigs(prev => prev.map((group, index) =>
            index === groupIndex ? { ...group, [key]: value } : group
        ));
    };

    const handleAddCalculatedValueToGroup = (groupIndex) => {
        setSubtotalConfigs(prev => prev.map((group, index) => {
            if (index === groupIndex) {
                return {
                    ...group,
                    calculatedValues: [
                        ...group.calculatedValues,
                        {
                            id: uuidv4(),
                            targetFieldName: numericSchemaFieldsForCalculation.length > 0 ? numericSchemaFieldsForCalculation[0].value : (schemaFieldsForCalculation.length > 0 ? schemaFieldsForCalculation[0].value : ''),
                            calculationType: 'SUM',
                            numberFormat: '',
                            alignment: ''
                        }
                    ]
                };
            }
            return group;
        }));
    };

    const handleRemoveCalculatedValueFromGroup = (groupIndex, valueIndex) => {
        setSubtotalConfigs(prev => prev.map((group, index) => {
            if (index === groupIndex) {
                return {
                    ...group,
                    calculatedValues: group.calculatedValues.filter((_, vIdx) => vIdx !== valueIndex)
                };
            }
            return group;
        }));
    };

    const handleCalculatedValueChangeInGroup = (groupIndex, valueIndex, key, value) => {
        setSubtotalConfigs(prev => prev.map((group, index) => {
            if (index === groupIndex) {
                return {
                    ...group,
                    calculatedValues: group.calculatedValues.map((val, vIdx) =>
                        vIdx === valueIndex ? { ...val, [key]: value } : val
                    )
                };
            }
            return group;
        }));
    };

    return (
        <Box mt="large" pt="medium" borderTop="1px solid" borderColor="ui3">
            <Heading as="h4" mb="medium">Configure Subtotal Rows</Heading>
            {subtotalConfigs.map((group, groupIndex) => (
                <Box key={group.id} mb="xlarge" p="medium" border="1px solid" borderColor="ui2" borderRadius="medium">
                    <Box display="flex" justifyContent="space-between" alignItems="center">
                        <Heading as="h5" mb="small">Subtotal Group {groupIndex + 1}</Heading>
                        <IconButton icon={<Delete />} label="Remove Subtotal Group" onClick={() => handleRemoveSubtotalGroup(groupIndex)} />
                    </Box>
                    <Box display="grid" gridTemplateColumns="1fr 1fr" gap="medium">
                        <FieldText
                            label="Group By Field (String Type)"
                            value={group.groupByFieldName}
                            onChange={(e) => handleSubtotalGroupChange(groupIndex, 'groupByFieldName', e.target.value)}
                            description="Select a string field from your query to group data for subtotals."
                            //@ts-ignore
                            inputElement={<Select options={schemaFieldsForGrouping} />} // Hacky way to use select
                        />
                         <FieldText
                            label="Values Placeholder Name (for GenAI)"
                            value={group.valuesPlaceholderName}
                            onChange={(e) => handleSubtotalGroupChange(groupIndex, 'valuesPlaceholderName', e.target.value)}
                            description="e.g., SUBTOTAL_CATEGORY_VALUES. GenAI will use {{this_name}}."
                        />
                    </Box>

                    <Heading as="h6" mt="medium" mb="small">Calculated Values for this Subtotal Group:</Heading>
                    {group.calculatedValues.map((calcVal, valIndex) => {
                        const targetFieldSchema = schema.find(f => f.name === calcVal.targetFieldName);
                        const isTargetNumeric = targetFieldSchema && NUMERIC_TYPES.includes(targetFieldSchema.type.toUpperCase());
                        return (
                            <Box key={calcVal.id} mb="medium" p="small" border="1px dashed" borderColor="ui1" borderRadius="small"
                                 display="grid" gridTemplateColumns="repeat(auto-fit, minmax(180px, 1fr))" gap="medium">
                                <FieldText
                                    label="Target Field for Calculation"
                                    value={calcVal.targetFieldName}
                                    onChange={(e) => handleCalculatedValueChangeInGroup(groupIndex, valIndex, 'targetFieldName', e.target.value)}
                                     //@ts-ignore
                                    inputElement={<Select options={schemaFieldsForCalculation} />}
                                />
                                <FieldText
                                    label="Calculation Type"
                                    value={calcVal.calculationType}
                                    onChange={(e) => handleCalculatedValueChangeInGroup(groupIndex, valIndex, 'calculationType', e.target.value)}
                                     //@ts-ignore
                                    inputElement={<Select options={CALCULATION_TYPE_OPTIONS} />}
                                />
                                 <FieldText
                                    label="Number Format"
                                    value={calcVal.numberFormat}
                                    onChange={(e) => handleCalculatedValueChangeInGroup(groupIndex, valIndex, 'numberFormat', e.target.value)}
                                    disabled={!isTargetNumeric && calcVal.calculationType !== 'COUNT' && calcVal.calculationType !== 'COUNT_DISTINCT'}
                                     //@ts-ignore
                                    inputElement={<Select options={NUMBER_FORMAT_OPTIONS} />}
                                />
                                <FieldText
                                    label="Alignment"
                                    value={calcVal.alignment}
                                    onChange={(e) => handleCalculatedValueChangeInGroup(groupIndex, valIndex, 'alignment', e.target.value)}
                                     //@ts-ignore
                                    inputElement={<Select options={ALIGNMENT_OPTIONS} />}
                                />
                                <Button
                                    mt="large"
                                    iconBefore={<Delete />}
                                    onClick={() => handleRemoveCalculatedValueFromGroup(groupIndex, valIndex)}
                                    size="small"
                                >
                                    Remove Value
                                </Button>
                            </Box>
                        );
                    })}
                    <Button onClick={() => handleAddCalculatedValueToGroup(groupIndex)} iconBefore={<Add />} mt="small" size="small">
                        Add Calculated Value to Group {groupIndex + 1}
                    </Button>
                </Box>
            ))}
            <Button onClick={handleAddSubtotalGroup} iconBefore={<Add />} mt="medium" >
                Add Subtotal Group
            </Button>
        </Box>
    );
}

export default SubtotalConfigurator;
// src/DynamicFilterUI.js
import React, { useState, useEffect, useContext } from 'react'; // Removed useMemo as it wasn't used
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Box,
    Heading,
    Select,
    FieldText,
    Button,
    // List, // List and ListItem were not used directly for filter display here
    // ListItem,
    Text,
    // Divider, // Not used in the latest version of filter rows
    // IconButton, // Not used directly for filter rows
    // Space, // Not used directly for filter rows
    Spinner
} from '@looker/components';

const OPERATORS_BY_TYPE = {
  STRING: [
    { label: 'Equals to', value: '_eq' },
    { label: 'Does not equal to', value: '_ne' },
    { label: 'Contains (user adds %)', value: '_like' },
    { label: 'Starts with (user adds %)', value: '_like' },
    { label: 'Ends with (user adds %)', value: '_like' },
    { label: 'Is one of (comma-sep)', value: '_in' },
    { label: 'Is not null', value: '_is_not_null' },
    { label: 'Is null', value: '_is_null' },
  ],
  INTEGER: [
    { label: 'Equals to', value: '_eq' },
    { label: 'Does not equal to', value: '_ne' },
    { label: 'Greater than', value: '_gt' },
    { label: 'Greater than or equal to', value: '_gte' },
    { label: 'Less than', value: '_lt' },
    { label: 'Less than or equal to', value: '_lte' },
    { label: 'Is between', value: '_between' },
    { label: 'Is one of (comma-sep)', value: '_in' },
    { label: 'Is not null', value: '_is_not_null' },
    { label: 'Is null', value: '_is_null' },
  ],
  FLOAT: [ // Similar to INTEGER
    { label: 'Equals to', value: '_eq' },
    { label: 'Does not equal to', value: '_ne' },
    { label: 'Greater than', value: '_gt' },
    { label: 'Greater than or equal to', value: '_gte' },
    { label: 'Less than', value: '_lt' },
    { label: 'Less than or equal to', value: '_lte' },
    { label: 'Is between', value: '_between' },
    { label: 'Is not null', value: '_is_not_null' },
    { label: 'Is null', value: '_is_null' },
  ],
  DATE: [
    { label: 'On', value: '_eq' },
    { label: 'Not on', value: '_ne' },
    { label: 'After or on', value: '_gte' },
    { label: 'Before or on', value: '_lte' },
    { label: 'Is between', value: '_between' },
    { label: 'Is not null', value: '_is_not_null' },
    { label: 'Is null', value: '_is_null' },
  ],
  BOOLEAN: [
    { label: 'Is', value: '_eq' }, // Expects 'true' or 'false'
    { label: 'Is not null', value: '_is_not_null' },
    { label: 'Is null', value: '_is_null' },
  ],
};

function DynamicFilterUI({ reportDefinitionName, schema, onApplyAndClose }) {
  const [filters, setFilters] = useState({});
  const [userClientId, setUserClientId] = useState('');
  const [userAttributesLoading, setUserAttributesLoading] = useState(true);
  const [userAttributesError, setUserAttributesError] = useState('');

  const extensionContext = useContext(ExtensionContext);
  const { extensionSDK } = extensionContext;

  useEffect(() => {
    let isMounted = true;
    if (extensionSDK) {
      setUserAttributesLoading(true);
      const fetchId = async () => {
        try {
          const clientIdVal = await extensionSDK.userAttributeGetItem('client_id');
          if (isMounted) {
            const newClientId = (clientIdVal !== null && typeof clientIdVal !== 'undefined') ? String(clientIdVal) : '';
            setUserClientId(newClientId);
            setUserAttributesError(newClientId ? '' : "'client_id' attribute not found for user.");
            console.log("DynamicFilterUI: Client ID fetched:", newClientId || "Not found");
          }
        } catch (error) {
          if (isMounted) setUserAttributesError(`Error fetching client_id: ${error.message}`);
          console.error("DynamicFilterUI: Error fetching client_id:", error);
        } finally {
          if (isMounted) setUserAttributesLoading(false);
        }
      };
      fetchId();
    } else {
        setUserAttributesLoading(false);
        setUserAttributesError("Extension SDK not available for client_id fetch.");
    }
    return () => { isMounted = false; };
  }, [extensionSDK]);

  useEffect(() => {
    console.log(`DynamicFilterUI: Schema or report changed for ${reportDefinitionName}. Resetting filters.`);
    setFilters({});
  }, [schema, reportDefinitionName]);

  const handleOperatorChange = (fieldName, fieldSchemaType, newOperator) => {
    setFilters(prevFilters => ({
      ...prevFilters,
      [fieldName]: {
        type: fieldSchemaType.toUpperCase(), // Store type consistently
        operator: newOperator,
        value: '', // Reset value on operator change
        value2: newOperator === '_between' ? '' : undefined
      }
    }));
  };

  const handleValueChange = (fieldName, newValue, valuePart = 'value') => {
    // console.log(`DynamicFilterUI: handleValueChange for ${fieldName}, part: ${valuePart}, newValue:`, newValue, 'type:', typeof newValue);
    setFilters(prevFilters => {
      const existingFilter = prevFilters[fieldName] || {};
      return {
        ...prevFilters,
        [fieldName]: {
          ...existingFilter,
          [valuePart]: newValue
        }
      };
    });
  };

  const handleExecuteReportWithFilters = () => {
    const dynamicFiltersObject = {};
    for (const fieldName in filters) {
      const filter = filters[fieldName];
      if (filter.operator) {
        if (filter.operator === '_between') {
          if ((filter.value !== undefined && filter.value !== '') && (filter.value2 !== undefined && filter.value2 !== '')) {
            dynamicFiltersObject[`${fieldName}_gte`] = filter.value;
            dynamicFiltersObject[`${fieldName}_lte`] = filter.value2;
          }
        } else if (filter.operator === '_is_null' || filter.operator === '_is_not_null') {
            dynamicFiltersObject[`${fieldName}${filter.operator}`] = 'true'; // Backend just needs the key
        } else if (filter.value !== undefined && filter.value !== '') {
          dynamicFiltersObject[`${fieldName}${filter.operator}`] = filter.value;
        }
      }
    }

    const userAttributesObject = {};
    if (userClientId) {
      userAttributesObject["AccountClientNumber"] = userClientId;
    }

    const filterCriteriaPayload = {
        user_attributes: userAttributesObject,
        dynamic_filters: dynamicFiltersObject
    };

    if (onApplyAndClose) {
        onApplyAndClose(filterCriteriaPayload);
    }
  };

  if (!schema || schema.length === 0) {
    return <Box p="medium"><Text>No schema available to build filters for "{reportDefinitionName}".</Text></Box>;
  }

  return (
    <Box p="xlarge" width="100%">
      <Heading as="h3" mb="xlarge" fontSize="large" fontWeight="semiBold">
        Apply Filters for: {reportDefinitionName}
      </Heading>
      <Text fontSize="small" mb="large">
        Client ID (Context): {userAttributesLoading ? <Spinner size={15} mr="xsmall"/> : (userAttributesError ? <Text color="critical">{userAttributesError}</Text> : (userClientId || "N/A"))}
      </Text>

      <Box display="flex" borderBottom="1px solid" borderColor="ui3" pb="small" mb="medium">
        <Box flex={{ default: 3, small: 2 }} fontWeight="semiBold"><Text>Field</Text></Box>
        <Box flex={{ default: 3, small: 2 }} fontWeight="semiBold" ml="medium"><Text>Operator</Text></Box>
        <Box flex={{ default: 6, small: 3 }} fontWeight="semiBold" ml="medium"><Text>Value(s)</Text></Box>
      </Box>

      {schema.map((field) => {
        const fieldTypeUpper = field.type.toUpperCase();
        const currentFieldOperators = OPERATORS_BY_TYPE[fieldTypeUpper] || OPERATORS_BY_TYPE['STRING']; // Default to STRING ops if type unknown
        const currentFilterConfig = filters[field.name] || {};
        const needsValueInput = currentFilterConfig.operator && currentFilterConfig.operator !== '_is_null' && currentFilterConfig.operator !== '_is_not_null';

        return (
          <Box
            key={field.name}
            display="flex"
            alignItems="flex-start" // Align items to the top for better layout with multi-line text/inputs
            gap="medium"
            mb="medium"
            pb="medium"
            borderBottom={schema.indexOf(field) < schema.length -1 ? "1px solid" : "none"}
            borderColor="ui1"
          >
            <Box flex={{ default: 3, small: 2 }} title={field.name} pt="small"> {/* Added pt for alignment with Select */}
              <Text fontWeight="medium" truncate>{field.label || field.name}</Text>
              <Text fontSize="xsmall" color="text2">({field.type})</Text>
            </Box>
            <Box flex={{ default: 3, small: 2 }}>
              <Select
                options={currentFieldOperators}
                value={currentFilterConfig.operator || ''}
                onChange={(operator) => handleOperatorChange(field.name, field.type, operator)}
                placeholder="Condition..."
                maxHeight={200}
              />
            </Box>
            <Box flex={{ default: 6, small: 3 }} display="flex"
                 flexDirection={currentFilterConfig.operator === '_between' ? "column" : "row"}
                 gap={currentFilterConfig.operator === '_between' ? "xsmall" : "small"}
            >
              <FieldText
                value={currentFilterConfig.value || ''}
                onChange={(event) => handleValueChange(field.name, event.target.value, 'value')} // MODIFIED HERE
                placeholder={currentFilterConfig.operator === '_between' ? "Start value / Min" : "Enter value"}
                type={fieldTypeUpper === 'DATE' ? 'date' : (fieldTypeUpper === 'INTEGER' || fieldTypeUpper === 'FLOAT' ? 'number' : 'text')}
                disabled={!needsValueInput}
              />
              {currentFilterConfig.operator === '_between' && (
                <FieldText
                  value={currentFilterConfig.value2 || ''}
                  onChange={(event) => handleValueChange(field.name, event.target.value, 'value2')} // MODIFIED HERE
                  placeholder="End value / Max"
                  type={fieldTypeUpper === 'DATE' ? 'date' : (fieldTypeUpper === 'INTEGER' || fieldTypeUpper === 'FLOAT' ? 'number' : 'text')}
                  disabled={!needsValueInput}
                />
              )}
            </Box>
          </Box>
        );
      })}

      <Button
        color="key"
        onClick={handleExecuteReportWithFilters}
        width="100%"
        mt="xlarge"
        size="large"
        disabled={userAttributesLoading} // Disable if still loading user attributes
      >
        Apply Filters & Execute Report
      </Button>
    </Box>
  );
}

export default DynamicFilterUI;
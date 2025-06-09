import React, { useState, useEffect, useContext } from 'react';
import styled from 'styled-components';
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Button as LookerButton,
    Spinner,
    Box,
    Heading,
    Space,
    IconButton,
    FieldText,
    TextArea as LookerTextarea,
    Select,
    FieldCheckbox,
    Tabs,
    TabList,
    Tab,
    TabPanels,
    TabPanel,
} from '@looker/components';
import { Add, Delete } from '@styled-icons/material';
import { v4 as uuidv4 } from 'uuid';

import FieldDisplayConfigurator from './FieldDisplayConfigurator';

// --- Styled Components ---
const FormWrapper = styled.div`
  padding: 25px;
  font-family: Arial, sans-serif;
  max-width: 800px;
  margin: 0 auto;
`;

const FormGroup = styled.div`
  margin-bottom: 20px;
`;

const Label = styled.label`
  display: block;
  margin-bottom: 8px;
  font-weight: bold;
  font-size: 14px;
`;

const Input = styled.input`
  width: 100%;
  padding: 10px;
  border: 1px solid #ccc;
  border-radius: 4px;
  box-sizing: border-box;
  font-size: 16px;
`;

const Description = styled.p`
  font-size: 0.85em;
  color: #666;
  margin-top: 5px;
  margin-bottom: 0;
`;

const Button = styled.button`
  width: 100%;
  background-color: #4285F4;
  color: white;
  padding: 12px 15px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 16px;
  font-weight: bold;
  transition: background-color 0.2s ease-in-out;

  &:hover {
    background-color: #357ae8;
  }
  &:disabled {
    background-color: #ccc;
    cursor: not-allowed;
  }
`;

const DynamicSection = styled(Box)`
  border: 1px solid ${({ theme }) => theme.colors.ui2};
  border-radius: ${({ theme }) => theme.radii.medium};
  margin-bottom: ${({ theme }) => theme.space.medium};
  padding: ${({ theme }) => theme.space.medium};
`;
// --- End of Styled Components ---

function ReportForm() {
  // All state remains at the top level, shared across all tabs
  const [reportName, setReportName] = useState('');
  const [imageUrl, setImageUrl] = useState('');
  const [promptText, setPromptText] = useState('');
  const [userAttributeMappings, setUserAttributeMappings] = useState('');
  const [dataTables, setDataTables] = useState([
    { id: uuidv4(), placeholderName: '', sql: '', fieldConfigs: [] }
  ]);
  const [lookConfigs, setLookConfigs] = useState([]);
  const [filterConfigs, setFilterConfigs] = useState([]);
  const [isFieldConfigModalOpen, setIsFieldConfigModalOpen] = useState(false);
  const [configuringTableId, setConfiguringTableId] = useState(null);
  const [currentSchemaForConfig, setCurrentSchemaForConfig] = useState([]);
  const [calculationRows, setCalculationRows] = useState([]);
  const [dryRunLoading, setDryRunLoading] = useState(false);
  const [dryRunError, setDryRunError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitStatus, setSubmitStatus] = useState('');
  const { extensionSDK } = useContext(ExtensionContext);
  const [sdkReady, setSdkReady] = useState(false);

  const backendBaseUrl = 'https://looker-ext-code-17837811141.us-central1.run.app';

  useEffect(() => {
    if (extensionSDK) setSdkReady(true);
  }, [extensionSDK]);

  // All handler functions remain the same
  const handleAddDataTable = () => setDataTables(prev => [...prev, { id: uuidv4(), placeholderName: '', sql: '', fieldConfigs: [] }]);
  const handleRemoveDataTable = (id) => setDataTables(prev => prev.filter(t => t.id !== id));
  const handleDataTableChange = (id, fieldName, value) => setDataTables(prev => prev.map(t => t.id === id ? { ...t, [fieldName]: value } : t));
  const handleAddLookConfig = () => setLookConfigs(prev => [...prev, { id: uuidv4(), lookId: '', placeholderName: '' }]);
  const handleRemoveLookConfig = (id) => setLookConfigs(prev => prev.filter(c => c.id !== id));
  const handleLookConfigChange = (id, fieldName, value) => setLookConfigs(prev => prev.map(c => c.id === id ? { ...c, [fieldName]: value } : c));
  const handleAddFilter = () => setFilterConfigs(prev => [...prev, { id: uuidv4(), ui_filter_key: '', ui_label: '', data_type: 'STRING', is_hidden_from_customer: false, targets: [] }]);
  const handleRemoveFilter = (id) => setFilterConfigs(prev => prev.filter(f => f.id !== id));
  const handleFilterChange = (id, field, value) => setFilterConfigs(prev => prev.map(f => f.id === id ? { ...f, [field]: value } : f));
  const handleAddTarget = (filterId) => setFilterConfigs(prev => prev.map(f => f.id === filterId ? { ...f, targets: [...f.targets, { id: uuidv4(), target_type: 'DATA_TABLE', target_id: '', target_field_name: '' }] } : f));
  const handleRemoveTarget = (filterId, targetId) => setFilterConfigs(prev => prev.map(f => f.id === filterId ? { ...f, targets: f.targets.filter(t => t.id !== targetId) } : f));
  const handleTargetChange = (filterId, targetId, field, value) => setFilterConfigs(prev => prev.map(f => f.id === filterId ? { ...f, targets: f.targets.map(t => t.id === targetId ? { ...t, [field]: value } : t) } : f));
  const handleDryRunAndConfigure = async (tableId, sqlQuery) => {
    if (!sqlQuery.trim()) { alert("SQL query for this table cannot be empty."); return; }
    setDryRunLoading(true); setDryRunError(''); setConfiguringTableId(tableId);
    try {
      const response = await extensionSDK.fetchProxy(`${backendBaseUrl}/dry_run_sql_for_schema`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql_query: sqlQuery }) });
      const data = response.body;
      if (response.ok && data?.schema) {
        setCurrentSchemaForConfig(data.schema);
        setIsFieldConfigModalOpen(true);
      } else { throw new Error(data?.detail || "Dry run failed."); }
    } catch (error) { setDryRunError(`Dry run failed: ${error.message}`);
    } finally { setDryRunLoading(false); }
  };
  const handleApplyFieldConfigs = (configs) => {
    setDataTables(prev => prev.map(t => t.id === configuringTableId ? { ...t, fieldConfigs: configs } : t));
    handleCloseConfigModal();
  };
  const handleCloseConfigModal = () => { setIsFieldConfigModalOpen(false); setConfiguringTableId(null); setCurrentSchemaForConfig([]); };
  const handleSubmitDefinition = async () => {
    setIsSubmitting(true);
    setSubmitStatus('Submitting definition...');

    try {
      // This will now be caught if the JSON is invalid
      const parsedUserAttributeMappings = JSON.parse(userAttributeMappings || '{}');

      const dataTablesPayload = dataTables.filter(dt => dt.placeholderName && dt.sql).map(dt => ({
        table_placeholder_name: dt.placeholderName,
        sql_query: dt.sql,
        field_display_configs: dt.fieldConfigs
      }));

      const looksPayload = lookConfigs.filter(lc => lc.lookId && lc.placeholderName).map(lc => ({
        look_id: parseInt(lc.lookId, 10),
        placeholder_name: lc.placeholderName
      }));

      const filtersPayload = filterConfigs
        .filter(f => f.ui_filter_key && f.ui_label)
        .map(({ id, targets, ...rest }) => ({
          ...rest,
          targets: targets
            .filter(t => t.target_id && t.target_field_name)
            .map(({ id, ...tRest }) => tRest)
        }));

      if (dataTablesPayload.length === 0) {
        throw new Error("You must define at least one data table with a placeholder name and SQL query.");
      }

      const definitionPayload = {
        report_name: reportName,
        image_url: imageUrl,
        prompt: promptText,
        data_tables: dataTablesPayload,
        look_configs: looksPayload,
        filter_configs: filtersPayload,
        user_attribute_mappings: parsedUserAttributeMappings,
        calculation_row_configs: calculationRows,
        subtotal_configs: [],
      };
      
      console.log('--- Frontend Payload Sent ---', JSON.stringify(definitionPayload, null, 2));

      const response = await extensionSDK.fetchProxy(`${backendBaseUrl}/report_definitions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(definitionPayload)
      });

      if (!response.ok) {
        const errorBody = response.body || { detail: `Request failed with status ${response.status}` };
        throw new Error(errorBody.detail);
      }
      
      setSubmitStatus(`Success! Report '${reportName}' submitted for background generation.`);
      // Reset form
      setReportName(''); setImageUrl(''); setPromptText(''); setUserAttributeMappings('');
      setDataTables([{ id: uuidv4(), placeholderName: '', sql: '', fieldConfigs: [] }]);
      setLookConfigs([]); setFilterConfigs([]);

    } catch (error) {
      // This will now catch errors from JSON.parse() or the network request
      const errorMessage = `Failed to submit. Please check your inputs, especially the User Attribute Mappings JSON.\n\nError: ${error.message}`;
      setSubmitStatus(errorMessage);
      alert(errorMessage);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <FormWrapper>
      <Heading as="h1" mb="large">Define New GenAI Report</Heading>
      
      <Tabs>
        <TabList>
          <Tab>Step 1: Details</Tab>
          <Tab>Step 2: Data & Content</Tab>
          <Tab>Step 3: Filters & Mappings</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            <FormGroup>
              <Label htmlFor="reportName">Report Definition Name <span style={{color: 'red'}}>*</span></Label>
              <Input type="text" id="reportName" value={reportName} onChange={(e) => setReportName(e.target.value)} placeholder="e.g., Monthly Sales Performance" disabled={isSubmitting}/>
            </FormGroup>
            <FormGroup>
              <Label htmlFor="imageUrl">Image URL (for styling guidance) <span style={{color: 'red'}}>*</span></Label>
              <Input type="text" id="imageUrl" value={imageUrl} onChange={(e) => setImageUrl(e.target.value)} placeholder="https://example.com/style_image.jpg" disabled={isSubmitting}/>
            </FormGroup>
            <FormGroup>
              <Label htmlFor="promptText">Base Prompt for Gemini <span style={{color: 'red'}}>*</span></Label>
              <LookerTextarea id="promptText" value={promptText} onChange={(e) => setPromptText(e.target.value)} placeholder="e.g., Generate an HTML template with..." rows={5} disabled={isSubmitting}/>
            </FormGroup>
          </TabPanel>
          <TabPanel>
            <FormGroup>
              <Heading as="h2" fontSize="large" fontWeight="semiBold" mb="small">Data Tables</Heading>
              {dataTables.map((table, index) => (
                <DynamicSection key={table.id}>
                  <Box display="flex" justifyContent="space-between" alignItems="center"><Heading as="h3" fontSize="medium">Data Table {index + 1}</Heading><IconButton icon={<Delete />} label="Remove Data Table" onClick={() => handleRemoveDataTable(table.id)} disabled={dataTables.length <= 1}/></Box>
                  <Space><FieldText label="Table Placeholder Name" description="e.g., sales_summary_table" value={table.placeholderName} onChange={(e) => handleDataTableChange(table.id, 'placeholderName', e.target.value)} disabled={isSubmitting}/><LookerButton mt="large" onClick={() => handleDryRunAndConfigure(table.id, table.sql)} disabled={dryRunLoading || isSubmitting || !table.sql.trim()} iconBefore={dryRunLoading && configuringTableId === table.id ? <Spinner size={18}/> : undefined}>Configure Columns</LookerButton></Space>
                  <Box mt="small"><Label htmlFor={`sql-${table.id}`}>SQL Query <span style={{color: 'red'}}>*</span></Label><LookerTextarea id={`sql-${table.id}`} value={table.sql} onChange={(e) => handleDataTableChange(table.id, 'sql', e.target.value)} rows={6} placeholder="SELECT ..." disabled={isSubmitting}/></Box>
                </DynamicSection>
              ))}
              <LookerButton onClick={handleAddDataTable} iconBefore={<Add />} disabled={isSubmitting}>Add Data Table</LookerButton>
              {dryRunError && <p style={{color: 'red', marginTop: '10px'}}>{dryRunError}</p>}
            </FormGroup>
            <FormGroup>
              <Heading as="h2" fontSize="large" fontWeight="semiBold" mb="small">Embed Looks</Heading>
              {lookConfigs.map((config, index) => (
                <DynamicSection key={config.id}>
                  <Box display="flex" justifyContent="space-between" alignItems="center"><Heading as="h3" fontSize="medium">Chart {index + 1}</Heading><IconButton icon={<Delete />} label="Remove Chart" onClick={() => handleRemoveLookConfig(config.id)} /></Box>
                  <Space><FieldText label="Look ID" value={config.lookId} onChange={(e) => handleLookConfigChange(config.id, 'lookId', e.target.value)} placeholder="e.g., 123" type="number" disabled={isSubmitting}/><FieldText label="Placeholder Name" value={config.placeholderName} onChange={(e) => handleLookConfigChange(config.id, 'placeholderName', e.target.value)} placeholder="e.g., sales_trend_chart" description="Use letters, numbers, underscores" disabled={isSubmitting}/></Space>
                </DynamicSection>
              ))}
              <LookerButton onClick={handleAddLookConfig} iconBefore={<Add />}>Add Chart from Look</LookerButton>
            </FormGroup>
          </TabPanel>
          <TabPanel>
            <FormGroup>
              <Heading as="h2" fontSize="large" fontWeight="semiBold" mb="small">Filter Configuration</Heading>
              <Description>Define user-facing filters and map them to data tables and Looks.</Description>
              {filterConfigs.map((filter, index) => (
                <DynamicSection key={filter.id}>
                  <Box display="flex" justifyContent="space-between" alignItems="center"><Heading as="h3" fontSize="medium">Filter {index + 1}</Heading><IconButton icon={<Delete/>} label="Remove Filter" onClick={() => handleRemoveFilter(filter.id)}/></Box>
                  <Space><FieldText label="UI Label" value={filter.ui_label} onChange={e => handleFilterChange(filter.id, 'ui_label', e.target.value)} placeholder="e.g., Select Date Range" /><FieldText label="Filter Key" value={filter.ui_filter_key} onChange={e => handleFilterChange(filter.id, 'ui_filter_key', e.target.value)} placeholder="e.g., date_range_filter" /></Space>
                  <FieldCheckbox label="Hide from Customer View" checked={filter.is_hidden_from_customer} onChange={e => handleFilterChange(filter.id, 'is_hidden_from_customer', e.target.checked)} />
                  <Box mt="medium" pt="small" borderTop="1px solid" borderColor="ui1">
                    <Heading as="h4" fontSize="small" color="text3">Filter Targets</Heading>
                    {filter.targets.map((target) => (
                      <Space key={target.id} my="small" align="flex-end">
                        <Select value={target.target_type} onChange={val => handleTargetChange(filter.id, target.id, 'target_type', val)} options={[{value:'DATA_TABLE', label:'Data Table'}, {value:'LOOK', label:'Look'}]} />
                        <Select value={target.target_id} onChange={val => handleTargetChange(filter.id, target.id, 'target_id', val)} options={target.target_type === 'DATA_TABLE' ? dataTables.map(dt => ({value: dt.placeholderName, label: dt.placeholderName})) : lookConfigs.map(lc => ({value: lc.lookId, label: `Look ${lc.lookId}`}))} placeholder={`Select ${target.target_type.replace('_',' ')}...`} />
                        <FieldText label="Target Field/Filter Name" value={target.target_field_name} onChange={e => handleTargetChange(filter.id, target.id, 'target_field_name', e.target.value)} placeholder="e.g., orders.created_date"/>
                        <IconButton icon={<Delete/>} label="Remove Target" onClick={() => handleRemoveTarget(filter.id, target.id)}/>
                      </Space>
                    ))}
                    <LookerButton onClick={() => handleAddTarget(filter.id)} size="xsmall" iconBefore={<Add/>}>Add Target</LookerButton>
                  </Box>
                </DynamicSection>
              ))}
              <LookerButton onClick={handleAddFilter} iconBefore={<Add/>}>Add Filter</LookerButton>
            </FormGroup>
            <FormGroup>
              <Label htmlFor="userAttributeMappings">User Attribute Mappings (JSON)</Label>
              <LookerTextarea id="userAttributeMappings" value={userAttributeMappings} onChange={e=>setUserAttributeMappings(e.target.value)} placeholder='e.g., {"looker_attribute_name": "bq_column_name"}' rows={3} disabled={isSubmitting} />
            </FormGroup>
          </TabPanel>
        </TabPanels>
      </Tabs>

      <FieldDisplayConfigurator isOpen={isFieldConfigModalOpen} onClose={handleCloseConfigModal} onApply={handleApplyFieldConfigs} schema={currentSchemaForConfig} reportName={reportName} initialConfigs={(configuringTableId && dataTables.find(dt => dt.id === configuringTableId)?.fieldConfigs) || []} />
      
      <FormGroup style={{ marginTop: '30px' }}>
        <Button onClick={handleSubmitDefinition} disabled={isSubmitting || dryRunLoading}>
          {isSubmitting ? "Saving Definition..." : "Create/Update Report Definition"}
        </Button>
      </FormGroup>
      
      {submitStatus && <p style={{color: submitStatus.includes("Error") ? 'red' : 'green'}}>{submitStatus}</p>}
    </FormWrapper>
  );
}

export default ReportForm;
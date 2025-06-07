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
    Textarea as LookerTextarea, // Using Looker's component for consistency
} from '@looker/components';
import { Add, Delete } from '@styled-icons/material';
import { v4 as uuidv4 } from 'uuid';

import FieldDisplayConfigurator from './FieldDisplayConfigurator';
import PlaceholderMappingDialog from './PlaceholderMappingDialog';

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

const DataTableSection = styled(Box)`
  border: 1px solid ${({ theme }) => theme.colors.ui2};
  border-radius: ${({ theme }) => theme.radii.medium};
  margin-bottom: ${({ theme }) => theme.space.medium};
  padding: ${({ theme }) => theme.space.medium};
`;
// --- End of Styled Components ---

function ReportForm() {
  const [reportName, setReportName] = useState('');
  const [imageUrl, setImageUrl] = useState('');
  const [promptText, setPromptText] = useState('');
  const [userAttributeMappings, setUserAttributeMappings] = useState('');
  const [lookConfigs, setLookConfigs] = useState([]);
  
  // --- NEW STATE for multiple data tables ---
  const [dataTables, setDataTables] = useState([
    { id: uuidv4(), placeholderName: '', sql: '', fieldConfigs: [] }
  ]);
  const [configuringTableId, setConfiguringTableId] = useState(null);
  // ---

  const [isFieldConfigModalOpen, setIsFieldConfigModalOpen] = useState(false);
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

  // --- Handlers for multiple Data Tables ---
  const handleAddDataTable = () => {
    setDataTables(prev => [...prev, { id: uuidv4(), placeholderName: '', sql: '', fieldConfigs: [] }]);
  };

  const handleRemoveDataTable = (idToRemove) => {
    setDataTables(prev => prev.filter(table => table.id !== idToRemove));
  };

  const handleDataTableChange = (id, fieldName, value) => {
    setDataTables(prev => prev.map(table =>
      table.id === id ? { ...table, [fieldName]: value } : table
    ));
  };
  // ---

  // --- Handlers for multiple Look configurations ---
  const handleAddLookConfig = () => {
    setLookConfigs(prev => [...prev, { id: uuidv4(), lookId: '', placeholderName: '' }]);
  };

  const handleRemoveLookConfig = (idToRemove) => {
    setLookConfigs(prev => prev.filter(config => config.id !== idToRemove));
  };

  const handleLookConfigChange = (id, fieldName, value) => {
    setLookConfigs(prev => prev.map(config =>
      config.id === id ? { ...config, [fieldName]: value } : config
    ));
  };
  // ---

  // --- MODIFIED Dry Run and Config handlers ---
  const handleDryRunAndConfigure = async (tableId, sqlQuery) => {
    if (!sdkReady) { alert("Looker SDK is not fully initialized."); return; }
    if (!sqlQuery.trim()) { alert("SQL query for this table cannot be empty."); return; }

    setDryRunLoading(true);
    setDryRunError('');
    setSubmitStatus('');
    setConfiguringTableId(tableId); // Keep track of which table we are configuring

    try {
      const dryRunUrl = `${backendBaseUrl}/dry_run_sql_for_schema`;
      const response = await extensionSDK.fetchProxy(dryRunUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql_query: sqlQuery }),
      });
      const responseData = response.body;
      if (response.ok && responseData && responseData.schema) {
        setCurrentSchemaForConfig(responseData.schema);
        setIsFieldConfigModalOpen(true);
      } else {
        throw new Error(responseData?.detail || "Dry run did not return a schema.");
      }
    } catch (error) {
      const errorMessage = error.message || "Unknown API error during dry run.";
      setDryRunError(`Dry run failed for this table: ${errorMessage}`);
    } finally {
      setDryRunLoading(false);
    }
  };

  const handleApplyFieldConfigs = (configs) => {
    setDataTables(prev => prev.map(table =>
      table.id === configuringTableId ? { ...table, fieldConfigs: configs } : table
    ));
    setIsFieldConfigModalOpen(false);
    setSubmitStatus("Field configurations applied to table. Ready to save.");
    setDryRunError('');
  };

  const handleCloseConfigModal = () => {
    setIsFieldConfigModalOpen(false);
    setConfiguringTableId(null);
    setCurrentSchemaForConfig([]);
  };
  // ---

  // --- MODIFIED Submission handler ---
  const handleSubmitDefinition = async () => {
    if (!sdkReady) { alert("Looker SDK not available."); return; }
    if (!reportName || !imageUrl || !promptText) { alert("Report Name, Image URL, and Prompt must be filled."); return; }
    
    let parsedMappings = {};
    if (userAttributeMappings.trim() !== "") {
      try { parsedMappings = JSON.parse(userAttributeMappings); }
      catch (e) { alert(`Error: Invalid JSON in User Attribute Mappings: ${e.message}`); return; }
    }

    setIsSubmitting(true);
    setSubmitStatus('Submitting definition...');

    const finalLookConfigs = lookConfigs
      .filter(lc => lc.lookId && lc.placeholderName.trim())
      .map(lc => ({ look_id: parseInt(lc.lookId, 10), placeholder_name: lc.placeholderName.trim() }));

    // Assemble the new data_tables payload
    const dataTablesPayload = dataTables
      .filter(dt => dt.placeholderName.trim() && dt.sql.trim())
      .map(dt => ({
        table_placeholder_name: dt.placeholderName,
        sql_query: dt.sql,
        field_display_configs: dt.fieldConfigs,
      }));

    if (dataTablesPayload.length === 0) {
      alert("You must define at least one data table with a placeholder name and SQL query.");
      setIsSubmitting(false);
      return;
    }

    const definitionPayload = {
      report_name: reportName,
      image_url: imageUrl,
      prompt: promptText,
      data_tables: dataTablesPayload, // Use new payload structure
      filter_configs: [], // We will add UI for this later
      look_configs: finalLookConfigs,
      user_attribute_mappings: parsedMappings,
      calculation_row_configs: calculationRows,
      subtotal_configs: [],
    };

    const fastapiReportDefsUrl = `${backendBaseUrl}/report_definitions`;

    try {
      const response = await extensionSDK.fetchProxy(fastapiReportDefsUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(definitionPayload),
      });
      const responseData = response.body;
      if (!response.ok) {
        const errorDetail = responseData?.detail || `Error submitting definition: ${response.status}`;
        throw new Error(errorDetail);
      }

      setSubmitStatus(`Success! Report '${reportName}' was submitted. It is being generated in the background and will appear on the 'View All Reports' page shortly.`);
      
      // Reset the form for the next submission
      setReportName(''); setImageUrl(''); setPromptText('');
      setUserAttributeMappings(''); setLookConfigs([]);
      setDataTables([{ id: uuidv4(), placeholderName: '', sql: '', fieldConfigs: [] }]);
      setCalculationRows([]); setDryRunError('');

    } catch (error) {
      setSubmitStatus(`Error submitting definition: ${error.message}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <FormWrapper>
      <Heading as="h1" mb="large">Define New GenAI Report</Heading>
      
      <FormGroup>
        <Label htmlFor="reportName">Report Definition Name <span style={{color: 'red'}}>*</span></Label>
        <Input type="text" id="reportName" value={reportName} onChange={(e) => setReportName(e.target.value)} placeholder="e.g., Monthly Sales Performance" disabled={isSubmitting}/>
      </FormGroup>

      <FormGroup>
        <Label htmlFor="imageUrl">Image URL (for styling guidance) <span style={{color: 'red'}}>*</span></Label>
        <Input type="text" id="imageUrl" value={imageUrl} onChange={(e) => setImageUrl(e.target.value)} placeholder="https://example.com/style_image.jpg" disabled={isSubmitting}/>
      </FormGroup>
      
       <FormGroup>
        <Label htmlFor="promptText">Base Prompt for Gemini (Template Generation) <span style={{color: 'red'}}>*</span></Label>
        <LookerTextarea id="promptText" value={promptText} onChange={(e) => setPromptText(e.target.value)} placeholder="e.g., Generate an HTML template with a title, a summary section, and placeholders for several data tables..." rows={5} disabled={isSubmitting}/>
      </FormGroup>

      {/* --- NEW UI for multiple data tables --- */}
      <FormGroup>
        <Heading as="h2" fontSize="large" fontWeight="semiBold" mb="small">Data Tables</Heading>
        <Description>Define one or more data tables for your report. Each needs a unique placeholder name for the AI to use in the template.</Description>
        
        {dataTables.map((table, index) => (
          <DataTableSection key={table.id}>
            <Box display="flex" justifyContent="space-between" alignItems="center">
              <Heading as="h3" fontSize="medium">Data Table {index + 1}</Heading>
              <IconButton icon={<Delete />} label="Remove Data Table" onClick={() => handleRemoveDataTable(table.id)} disabled={dataTables.length <= 1}/>
            </Box>
            <Space around>
              <FieldText
                label="Table Placeholder Name"
                description="e.g., sales_summary_table"
                value={table.placeholderName}
                onChange={(e) => handleDataTableChange(table.id, 'placeholderName', e.target.value)}
                disabled={isSubmitting}
              />
              <LookerButton
                mt="large"
                onClick={() => handleDryRunAndConfigure(table.id, table.sql)}
                disabled={dryRunLoading || isSubmitting || !table.sql.trim()}
                iconBefore={dryRunLoading && configuringTableId === table.id ? <Spinner size={18}/> : undefined}
              >
                Configure Columns
              </LookerButton>
            </Space>
            <Box mt="small">
              <Label htmlFor={`sql-${table.id}`}>SQL Query <span style={{color: 'red'}}>*</span></Label>
              <LookerTextarea
                id={`sql-${table.id}`}
                value={table.sql}
                onChange={(e) => handleDataTableChange(table.id, 'sql', e.target.value)}
                rows={6}
                placeholder="SELECT ..."
                disabled={isSubmitting}
              />
            </Box>
          </DataTableSection>
        ))}

        <LookerButton onClick={handleAddDataTable} iconBefore={<Add />} disabled={isSubmitting}>
          Add Data Table
        </LookerButton>
        {dryRunError && <p style={{color: 'red', marginTop: '10px'}}>{dryRunError}</p>}
      </FormGroup>
      {/* --- End of new UI --- */}

      <FormGroup>
        {/* UI for Look Configs remains the same... */}
      </FormGroup>

      <FormGroup>
        <Label htmlFor="userAttributeMappings">User Attribute Mappings (JSON)</Label>
        <LookerTextarea id="userAttributeMappings" value={userAttributeMappings} onChange={(e) => setUserAttributeMappings(e.target.value)} placeholder='e.g., {"looker_attribute_name": "bq_column_name"}' rows={3} disabled={isSubmitting} />
      </FormGroup>

      <FieldDisplayConfigurator
        isOpen={isFieldConfigModalOpen}
        onClose={handleCloseConfigModal}
        onApply={handleApplyFieldConfigs}
        schema={currentSchemaForConfig}
        reportName={reportName}
        // Pass the specific configs for the table being configured
        initialConfigs={
            (configuringTableId && dataTables.find(dt => dt.id === configuringTableId)?.fieldConfigs) || []
        }
      />
      
      <FormGroup style={{ marginTop: '30px' }}>
        <Button
            type="button"
            onClick={handleSubmitDefinition}
            disabled={isSubmitting || dryRunLoading}
        >
          {isSubmitting ? "Saving Definition..." : "Create/Update Report Definition"}
        </Button>
      </FormGroup>

      {submitStatus && (
        <FormGroup>
          <p style={{
              color: submitStatus.toLowerCase().includes("error") || submitStatus.toLowerCase().includes("failed") ? 'red' : 'green',
              marginTop: '10px', border: '1px solid', padding: '10px', borderRadius: '4px',
              borderColor: submitStatus.toLowerCase().includes("error") || submitStatus.toLowerCase().includes("failed") ? 'red' : 'green'
            }}>
            {submitStatus}
          </p>
        </FormGroup>
      )}
    </FormWrapper>
  );
}

export default ReportForm;
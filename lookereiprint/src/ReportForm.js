// src/ReportForm.js
import React, { useState, useEffect, useContext } from 'react';
import styled from 'styled-components';
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Button as LookerButton,
    Spinner,
    Box,
    Dialog,
    DialogLayout,
    Heading,
    IconButton,
    Space,
    Select
} from '@looker/components';
import { Close, Add, Delete } from '@styled-icons/material'; // Add/Delete might be for future manual calc rows
import { v4 as uuidv4 } from 'uuid';

import FieldDisplayConfigurator from './FieldDisplayConfigurator';

// --- Styled Components ( 그대로 유지 ) ---
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

const Textarea = styled.textarea`
  width: 100%;
  padding: 10px;
  border: 1px solid #ccc;
  border-radius: 4px;
  box-sizing: border-box;
  min-height: 100px;
  font-size: 16px;
  font-family: Arial, sans-serif;
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

const DebugBox = styled.div`
  background-color: #f0f0f0;
  border: 1px solid #ccc;
  padding: 15px;
  margin-top: 20px;
  margin-bottom: 20px;
  border-radius: 4px;
  font-size: 0.9em;

  h4 { margin-top: 0; margin-bottom: 10px; font-size: 1.1em; }
  strong { font-weight: bold; }
  p { margin: 5px 0; }
  pre {
    white-space: pre-wrap;
    word-wrap: break-word;
    background-color: #fff;
    padding: 10px;
    border-radius: 3px;
    max-height: 150px;
    overflow-y: auto;
  }
`;
// --- End of Styled Components ---

// Constants needed for processing field configurations
const NUMERIC_TYPES = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"];
const DATE_TYPES = ["DATE", "DATETIME", "TIMESTAMP"];

function ReportForm() {
  const [reportName, setReportName] = useState('');
  const [imageUrl, setImageUrl] = useState('');
  const [baseSql, setBaseSql] = useState('');
  const [promptText, setPromptText] = useState('');
  const [userAttributeMappings, setUserAttributeMappings] = useState('');

  const [isFieldConfigModalOpen, setIsFieldConfigModalOpen] = useState(false);
  const [currentSchemaForConfig, setCurrentSchemaForConfig] = useState([]);
  const [fieldDisplayConfigurations, setFieldDisplayConfigurations] = useState([]);
  
  // const [calculationRows, setCalculationRows] = useState([]); // REMOVED - Now driven by "TOTALS" summary_action
  // subtotalConfigs state was already removed

  const [dryRunLoading, setDryRunLoading] = useState(false);
  const [dryRunError, setDryRunError] = useState('');

  const [userClientId, setUserClientId] = useState('');
  const [userAttributesLoading, setUserAttributesLoading] = useState(true);
  const [userAttributesError, setUserAttributesError] = useState('');

  const [manifestConstants, setManifestConstants] = useState(null);
  const [constantsLoading, setConstantsLoading] = useState(true);
  const [constantsError, setConstantsError] = useState('');
  const [configError, setConfigError] = useState('');

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitStatus, setSubmitStatus] = useState('');

  const { extensionSDK } = useContext(ExtensionContext);
  const [sdkReady, setSdkReady] = useState(false);

  const backendBaseUrl = 'https://885d-2001-569-5925-3000-216-3eff-fe9a-a055.ngrok-free.app';

  useEffect(() => {
    // ... (useEffect content remains the same) ...
    let isMounted = true;
    if (extensionSDK) {
        if (extensionSDK.lookerHostData) {
            if(isMounted) setSdkReady(true);
        } else {
            if(isMounted) setSdkReady(false);
        }
    } else {
        if(isMounted) {
            setSdkReady(false);
            setConfigError("Looker SDK not available. Extension may not function correctly.");
        }
    }

    const fetchClientAttribute = async () => {
      if (!isMounted || !extensionSDK || !extensionSDK.userAttributeGetItem) return;
      setUserAttributesLoading(true);
      try {
        const clientIdValue = await extensionSDK.userAttributeGetItem('client_id');
        if (isMounted) {
          const newClientId = clientIdValue ? String(clientIdValue) : '';
          setUserClientId(newClientId);
          setUserAttributesError(newClientId ? '' : "'client_id' user attribute not found.");
        }
      } catch (error) { if (isMounted) setUserAttributesError(`Error fetching client_id: ${error.message}`);
      } finally { if (isMounted) setUserAttributesLoading(false); }
    };

    const loadManifestConstants = () => {
        if (!isMounted || !extensionSDK || !extensionSDK.getContextData) return;
        setConstantsLoading(true); setConstantsError('');
        try {
            const contextData = extensionSDK.getContextData(); let foundConstants = null;
            if (contextData && contextData.constants) { foundConstants = contextData.constants; }
            if (foundConstants && typeof foundConstants === 'object' && Object.keys(foundConstants).length > 0) {
                setManifestConstants(foundConstants);
            } else { setConstantsError("Manifest constants not found or are empty in SDK contextData."); }
        } catch (error) { setConstantsError(`Error loading constants: ${error.message}`);
        } finally { setConstantsLoading(false); }
    };

    if (extensionSDK) { fetchClientAttribute(); loadManifestConstants(); }
    else { if(isMounted) { setUserAttributesLoading(false); setConstantsLoading(false); setUserAttributesError("SDK not ready for UA."); setConstantsError("SDK not ready for MC.");}}
    if (backendBaseUrl === 'YOUR_HARDCODED_NGROK_URL_HERE' || backendBaseUrl === '') {
        if(isMounted) { setConfigError(prev => prev ? prev + " Backend URL placeholder." : "Backend URL placeholder.");}
    }
    return () => { isMounted = false; };
  }, [extensionSDK]);


  const handleDryRunAndConfigure = async () => {
    // ... (handleDryRunAndConfigure content remains the same) ...
    if (!sdkReady) { const errMsg = "Looker SDK is not fully initialized."; setDryRunError(errMsg); alert(errMsg); return; }
    if (!backendBaseUrl || backendBaseUrl === 'YOUR_HARDCODED_NGROK_URL_HERE') { setDryRunError("Backend URL not configured."); alert("Backend URL not configured."); return; }
    if (!baseSql.trim()) { setDryRunError("Base SQL query cannot be empty."); alert("Base SQL query cannot be empty."); return; }
    setDryRunLoading(true); setDryRunError(''); setSubmitStatus('');
    try {
      const dryRunUrl = `${backendBaseUrl}/dry_run_sql_for_schema`;
      const response = await extensionSDK.fetchProxy(dryRunUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ sql_query: baseSql }), });
      const responseData = response.body;
      if (responseData && responseData.schema) {
        setCurrentSchemaForConfig(responseData.schema);
        setIsFieldConfigModalOpen(true);
      } else { throw new Error(responseData?.message || responseData?.detail || "Dry run did not return a schema."); }
    } catch (error) { 
        const errorMessage = error.message || (error.body?.detail) || "Unknown API error."; 
        setDryRunError(`Dry run failed: ${errorMessage}`); 
        alert(`Dry run failed: ${errorMessage}`);
    } finally { setDryRunLoading(false); }
  };

  const handleApplyFieldConfigs = (configs) => { 
    setFieldDisplayConfigurations(configs); 
    setIsFieldConfigModalOpen(false);
    setSubmitStatus("Field configurations applied. Ready to save."); 
    setDryRunError('');
  };
  const handleCloseConfigModal = () => { setIsFieldConfigModalOpen(false); };


  const handleSubmitDefinition = async () => {
    if (!sdkReady && !extensionSDK) { const errMsg = "Looker SDK not available."; setSubmitStatus(`Error: ${errMsg}`); alert(errMsg); return; }
    if (!backendBaseUrl || backendBaseUrl === 'YOUR_HARDCODED_NGROK_URL_HERE') { setSubmitStatus("Error: Backend URL not configured."); alert("Error: Backend URL not configured."); return; }
    if (!reportName || !imageUrl || !baseSql || !promptText) { setSubmitStatus("Error: All required fields must be filled."); alert("Error: All required fields must be filled."); return; }

    let parsedMappings = {};
    if (userAttributeMappings.trim() !== "") {
        try {
            parsedMappings = JSON.parse(userAttributeMappings);
            if (typeof parsedMappings !== 'object' || parsedMappings === null || Array.isArray(parsedMappings)) {
                throw new Error("User Attribute Mappings must be a valid JSON object.");
            }
        } catch (e) { setSubmitStatus(`Error: Invalid JSON: ${e.message}`); alert(`Error: Invalid JSON: ${e.message}`); return; }
    }

    setIsSubmitting(true);
    setSubmitStatus('Submitting definition... This may take several minutes. The UI will wait.');

    const derivedCalculationRowConfigs = [];
    const derivedSubtotalConfigs = [];

    // Helper to find original schema type for a field name
    const getFieldSchemaType = (fieldName) => {
        const fieldSchema = currentSchemaForConfig.find(s => s.name === fieldName);
        return fieldSchema ? fieldSchema.type.toUpperCase() : 'UNKNOWN';
    };

    // Prepare a list of numeric fields and their chosen summary calculations
    const numericFieldsForSummary = fieldDisplayConfigurations
        .filter(fConf => {
            const schemaType = getFieldSchemaType(fConf.field_name);
            return NUMERIC_TYPES.includes(schemaType) && fConf.numeric_summary_calculation && fConf.numeric_summary_calculation !== '';
        })
        .map(fConf => ({
            target_field_name: fConf.field_name,
            calculation_type: fConf.numeric_summary_calculation,
            number_format: fConf.number_format || null,
            alignment: fConf.alignment || null,
        }));

    // Iterate through all field configurations to find those that define a Totals or Subtotals section
    fieldDisplayConfigurations.forEach(config => {
        const fieldSchemaType = getFieldSchemaType(config.field_name);
        const isStringField = fieldSchemaType === 'STRING';
        const isDateField = DATE_TYPES.includes(fieldSchemaType);

        if ((isStringField || isDateField) && config.generated_placeholder_name) {
            if (config.summary_action === 'TOTALS') {
                derivedCalculationRowConfigs.push({
                    row_label: `Totals: ${config.field_name.replace(/_/g, ' ')}`, // Example label
                    values_placeholder_name: config.generated_placeholder_name,
                    calculated_values: numericFieldsForSummary, // Use pre-filtered list of numeric calcs
                });
            } else if (config.summary_action === 'SUBTOTALS') {
                derivedSubtotalConfigs.push({
                    group_by_field_name: config.field_name,
                    values_placeholder_name: config.generated_placeholder_name,
                    calculated_values: numericFieldsForSummary, // Use pre-filtered list of numeric calcs
                });
            }
        }
    });

    const definitionPayload = {
      report_name: reportName,
      image_url: imageUrl,
      sql_query: baseSql,
      prompt: promptText,
      user_attribute_mappings: parsedMappings,
      field_display_configs: fieldDisplayConfigurations,
      calculation_row_configs: derivedCalculationRowConfigs, // Use derived configs
      subtotal_configs: derivedSubtotalConfigs,           // Use derived configs
    };
    console.log('Payload for POST /report_definitions:', JSON.stringify(definitionPayload, null, 2));
    const fastapiReportDefsUrl = `${backendBaseUrl}/report_definitions`;

    try {
      const response = await fetch(fastapiReportDefsUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(definitionPayload),
      });
      const responseText = await response.text();
      if (!response.ok) { 
        let errorDetail = `HTTP error ${response.status}: ${response.statusText}`;
        try { const jsonData = JSON.parse(responseText); errorDetail = jsonData.detail || errorDetail; } 
        catch (e) { if (responseText.length < 200) { errorDetail += ` - Server response: ${responseText}`;}}
        throw new Error(errorDetail); 
      }
      let responseData = {};
      try { responseData = JSON.parse(responseText); }
      catch (e) { throw new Error (`Submitted but failed to parse server response: ${responseText.substring(0,200)}...`);}

      setSubmitStatus(`Success! Report definition '${reportName}' saved. Path: ${responseData.template_html_gcs_path || 'N/A'}`);
      setReportName(''); setImageUrl(''); setBaseSql(''); setPromptText(''); setUserAttributeMappings('');
      setCurrentSchemaForConfig([]); setFieldDisplayConfigurations([]);
      // setCalculationRows([]); // Already removed
      setDryRunError('');
    } catch (error) {
      console.error("Error submitting definition (standard fetch):", error);
      setSubmitStatus(`Error submitting definition: ${error.message}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const overallLoading = userAttributesLoading || constantsLoading;
  const isConfigurationReady = sdkReady && backendBaseUrl && backendBaseUrl !== 'YOUR_HARDCODED_NGROK_URL_HERE';

  return (
    <FormWrapper>
      <h1 style={{ marginBottom: '25px', textAlign: 'left', fontSize: '24px' }}>Define New GenAI Report</h1>
      {!isConfigurationReady && !overallLoading && ( <FormGroup><p style={{color: 'red', border: '1px solid red', padding: '10px', borderRadius: '4px'}}><strong>Configuration Alert:</strong>{ !sdkReady && " SDK not ready."}{ (backendBaseUrl === 'YOUR_HARDCODED_NGROK_URL_HERE' || !backendBaseUrl) && " Backend URL not configured."}{ configError && ` ${configError}`}</p></FormGroup>)}

      {/* Report Name, Image URL, Prompt, User Attribute Mappings, Base SQL FormGroups ... (remain the same) */}
      <FormGroup>
        <Label htmlFor="reportName">Report Definition Name <span style={{color: 'red'}}>*</span></Label>
        <Input type="text" id="reportName" value={reportName} onChange={(e) => setReportName(e.target.value)} placeholder="e.g., Monthly Sales Performance" disabled={!isConfigurationReady || overallLoading || isSubmitting}/>
        <Description>A unique name for this report.</Description>
      </FormGroup>
      <FormGroup>
        <Label htmlFor="imageUrl">Image URL (for styling guidance) <span style={{color: 'red'}}>*</span></Label>
        <Input type="text" id="imageUrl" value={imageUrl} onChange={(e) => setImageUrl(e.target.value)} placeholder="https://example.com/style_image.jpg" disabled={!isConfigurationReady || overallLoading || isSubmitting}/>
      </FormGroup>
       <FormGroup>
        <Label htmlFor="promptText">Base Prompt for Gemini (Template Generation) <span style={{color: 'red'}}>*</span></Label>
        <Textarea id="promptText" value={promptText} onChange={(e) => setPromptText(e.target.value)} placeholder="Generate an HTML template with placeholders..." rows={6} disabled={!isConfigurationReady || overallLoading || isSubmitting}/>
        <Description>Instructions for the AI to generate the HTML template structure.</Description>
      </FormGroup>
      <FormGroup>
        <Label htmlFor="userAttributeMappings">User Attribute Mappings (JSON)</Label>
        <Textarea id="userAttributeMappings" value={userAttributeMappings} onChange={(e) => setUserAttributeMappings(e.target.value)} placeholder='e.g., {"looker_attribute_name": "bq_column_name"}' rows={4} disabled={!isConfigurationReady || overallLoading || isSubmitting} />
        <Description>Map Looker user attributes to BigQuery columns for filtering.</Description>
      </FormGroup>
      <FormGroup>
        <Label htmlFor="baseSql">Base SQL Query <span style={{color: 'red'}}>*</span></Label>
        <Textarea id="baseSql" value={baseSql} onChange={(e) => setBaseSql(e.target.value)} placeholder="SELECT column_a, column_b FROM your_table.users WHERE ..." rows={8} disabled={!isConfigurationReady || overallLoading || isSubmitting}/>
        <Description>Main SQL. After entering, click "Validate & Configure Fields" below.</Description>
      </FormGroup>


      <FormGroup>
        <LookerButton
            onClick={handleDryRunAndConfigure}
            disabled={!isConfigurationReady || dryRunLoading || overallLoading || !baseSql.trim() || isSubmitting}
            width="100%"
            iconBefore={dryRunLoading ? <Spinner size={18}/> : undefined}
        >
          {dryRunLoading ? "Validating SQL..." : "Configure Column Display"}
        </LookerButton>
        {dryRunError && <p style={{color: 'red', marginTop: '10px'}}>{dryRunError}</p>}
      </FormGroup>

      <FieldDisplayConfigurator
        isOpen={isFieldConfigModalOpen}
        onClose={handleCloseConfigModal}
        onApply={handleApplyFieldConfigs}
        schema={currentSchemaForConfig}
        reportName={reportName}
        initialConfigs={fieldDisplayConfigurations}
      />
      
      {currentSchemaForConfig && currentSchemaForConfig.length > 0 && (
        <Box mt="large" pt="medium" borderTop="1px solid" borderColor="ui3">
            <Heading as="h4" mb="small">Subtotal & Total Rows Logic</Heading>
            <p style={{color: 'grey', fontSize: 'small'}}>
                Define "Totals" or "Subtotals" sections using the "Summary Configuration" options for String or Date fields in the table above.
                For numeric fields, you can then specify how they should aggregate (e.g., SUM, AVERAGE) within those defined sections.
            </p>
        </Box>
      )}

      {/* The section for "Overall Calculation Rows" which previously mentioned a future UI for `CalculationRowConfigurator` 
          can now be considered covered by the "Totals" summary_action, or removed if it's redundant.
          For clarity, I'll update its text.
      */}
      {currentSchemaForConfig && currentSchemaForConfig.length > 0 && (
        <Box mt="large" pt="medium" borderTop="1px solid" borderColor="ui3">
            <Heading as="h4" mb="medium">Summary Rows (Totals & Subtotals)</Heading>
            <p style={{color: 'grey', fontSize: 'small'}}>
                Grand Totals and Subtotals are now configured within the "Configure Column Display" section.
                String/Date fields can define a "Totals" or "Subtotals" section, and Numeric fields can specify their aggregation method for these sections.
            </p>
        </Box>
      )}

      <DebugBox>
        <h4>Configuration & User Context:</h4>
        { (userAttributesLoading || constantsLoading) && <Spinner size={20} /> }
        { !userAttributesLoading && userAttributesError && <p style={{color: 'red'}}>Client ID Error: {userAttributesError}</p>}
        { !userAttributesLoading && !userAttributesError && userClientId && ( <p>Client ID: <strong>{userClientId}</strong></p> )}
        { !userAttributesLoading && !userAttributesError && !userClientId && ( <p>Client ID: Not found/set</p> )}
        <div style={{marginTop: '10px'}}>
          <p><strong>Manifest Constants (Example):</strong></p>
          { constantsLoading && <p>Loading constants...</p>}
          { !constantsLoading && constantsError && <p style={{color: 'red'}}>{constantsError}</p>}
          { !constantsLoading && !constantsError && manifestConstants && Object.keys(manifestConstants).length > 0 ? (
            <><p>GCP_PROJECT_ID: <strong>{manifestConstants.GCP_PROJECT_ID || "Not set"}</strong></p></>
          ) : ( !constantsLoading && !constantsError && <p>Manifest constants empty/not found.</p> )}
        </div>
        <p>Backend URL (Hardcoded): <strong>{backendBaseUrl}</strong></p>
        <p>SDK Ready: <strong>{sdkReady ? 'Yes' : 'No'}</strong></p>
        <p>Field Display Configs (Preview):</p>
        <pre>{JSON.stringify(fieldDisplayConfigurations.slice(0,2), null, 2)}</pre>
      </DebugBox>

      <FormGroup style={{ marginTop: '30px' }}>
        <Button
            type="button"
            onClick={handleSubmitDefinition}
            disabled={!isConfigurationReady || isSubmitting || overallLoading || dryRunLoading || fieldDisplayConfigurations.length === 0}
        >
          {isSubmitting ? "Saving Definition..." : "Create/Update Report Definition"}
        </Button>
      </FormGroup>
      
      {submitStatus && (
        <FormGroup>
          <p style={{
              color: submitStatus.toLowerCase().includes("error") || submitStatus.toLowerCase().includes("failed") || submitStatus.toLowerCase().includes("timeout") ? 'red' : 'green',
              marginTop: '10px', border: '1px solid', padding: '10px', borderRadius: '4px',
              borderColor: submitStatus.toLowerCase().includes("error") || submitStatus.toLowerCase().includes("failed") || submitStatus.toLowerCase().includes("timeout") ? 'red' : 'green'
            }}>
            {submitStatus}
          </p>
        </FormGroup>
      )}
    </FormWrapper>
  );
}

export default ReportForm;
// src/ReportForm.js
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

function ReportForm() {
  const [reportName, setReportName] = useState('');
  const [imageUrl, setImageUrl] = useState('');
  const [baseSql, setBaseSql] = useState('');
  const [promptText, setPromptText] = useState('');
  const [userAttributeMappings, setUserAttributeMappings] = useState('');

  // State for multiple Look configurations
  const [lookConfigs, setLookConfigs] = useState([]);

  const [isFieldConfigModalOpen, setIsFieldConfigModalOpen] = useState(false);
  const [currentSchemaForConfig, setCurrentSchemaForConfig] = useState([]);
  const [fieldDisplayConfigurations, setFieldDisplayConfigurations] = useState([]);
  
  const [calculationRows, setCalculationRows] = useState([]);

  const [dryRunLoading, setDryRunLoading] = useState(false);
  const [dryRunError, setDryRunError] = useState('');

  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitStatus, setSubmitStatus] = useState('');

  const [discoveredPlaceholders, setDiscoveredPlaceholders] = useState([]);
  const [isPlaceholderModalOpen, setIsPlaceholderModalOpen] = useState(false);
  const [currentReportForPlaceholders, setCurrentReportForPlaceholders] = useState('');
  const [lastSuccessfullyDefinedReport, setLastSuccessfullyDefinedReport] = useState('');
  const [schemaForLastDefinedReport, setSchemaForLastDefinedReport] = useState([]);


  const { extensionSDK } = useContext(ExtensionContext);
  const [sdkReady, setSdkReady] = useState(false);

  const backendBaseUrl = 'https://looker-ext-code-17837811141.us-central1.run.app';

  useEffect(() => {
    let isMounted = true;
    if (extensionSDK) {
        if (extensionSDK.lookerHostData) {
            if(isMounted) setSdkReady(true);
        }
    }
    return () => { isMounted = false; };
  }, [extensionSDK]);

  // Handlers for multiple Look configurations
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

  const handleDryRunAndConfigure = async () => {
    if (!sdkReady) { alert("Looker SDK is not fully initialized."); return; }
    if (!baseSql.trim()) { alert("Base SQL query cannot be empty."); return; }
    setDryRunLoading(true); setDryRunError(''); setSubmitStatus('');
    try {
      const dryRunUrl = `${backendBaseUrl}/dry_run_sql_for_schema`;
      const response = await extensionSDK.fetchProxy(dryRunUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql_query: baseSql }),
      });
      const responseData = response.body;
      if (response.ok && responseData && responseData.schema) {
        setCurrentSchemaForConfig(responseData.schema);
        setIsFieldConfigModalOpen(true);
      } else {
        throw new Error(responseData?.detail || "Dry run did not return a schema or was unsuccessful.");
      }
    } catch (error) {
        const errorMessage = error.message || "Unknown API error during dry run.";
        setDryRunError(`Dry run failed: ${errorMessage}`);
        alert(`Dry run failed: ${errorMessage}`);
    } finally {
      setDryRunLoading(false);
    }
  };

  const handleApplyFieldConfigs = (configs) => {
    setFieldDisplayConfigurations(configs);
    setIsFieldConfigModalOpen(false);
    setSubmitStatus("Field configurations applied. Ready to save or create report.");
    setDryRunError('');
  };

  const handleCloseConfigModal = () => { setIsFieldConfigModalOpen(false); };

  const fetchAndLogPlaceholders = async (nameOfReport) => {
    if (!sdkReady) {
        setSubmitStatus(prev => prev + " Error: SDK not ready for placeholder discovery.");
        return;
    }
    setSubmitStatus(prev => prev + ` Checking template placeholders for '${nameOfReport}'...`);
    try {
        const discoverUrl = `${backendBaseUrl}/report_definitions/${encodeURIComponent(nameOfReport)}/discover_placeholders`;
        const response = await extensionSDK.fetchProxy(discoverUrl, {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        const data = response.body;
        if (response.ok && data.template_found && data.placeholders) {
            setDiscoveredPlaceholders(data.placeholders);
            setSubmitStatus(prev => prev.replace("Checking template placeholders...", "") + ` Found ${data.placeholders.length} placeholders. Configuration is optional.`);
        } else {
            setSubmitStatus(prev => prev.replace("Checking template placeholders...", "") + ` Warning: ${data.error_message || 'Could not discover placeholders.'}`);
            setDiscoveredPlaceholders([]);
        }
    } catch (error) {
        setSubmitStatus(prev => prev.replace("Checking template placeholders...", "") + ` Error discovering placeholders: ${error.message}`);
        setDiscoveredPlaceholders([]);
    }
  };

  const handlePreviewReport = async (reportNameToPreview) => {
    if (!reportNameToPreview) return;
    setSubmitStatus(prev => prev + ` Attempting to generate preview for '${reportNameToPreview}'...`);
    setIsSubmitting(true);

    try {
        const executionPayload = {
            report_definition_name: reportNameToPreview,
            filter_criteria_json: JSON.stringify({})
        };
        const fastapiExecuteUrl = `${backendBaseUrl}/execute_report`;
        const response = await extensionSDK.fetchProxy(fastapiExecuteUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(executionPayload),
        });
        const responseData = response.body;
        if (!response.ok) {
            throw new Error(responseData.detail || `Preview generation failed: ${response.status}`);
        }
        if (responseData && responseData.report_url_path) {
            const fullReportUrl = backendBaseUrl + responseData.report_url_path;
            extensionSDK.openBrowserWindow(fullReportUrl, '_blank');
            setSubmitStatus(prev => prev.replace("Attempting to generate preview...", "") + ` Preview for '${reportNameToPreview}' opened.`);
        } else {
            throw new Error("Failed to get report URL from backend for preview.");
        }
    } catch (error) {
        setSubmitStatus(prev => prev.replace("Attempting to generate preview...", "") + ` Error generating preview: ${error.message}`);
    } finally {
        setIsSubmitting(false);
    }
  };

  const handleSavePlaceholderMappings = async (reportNameForSave, mappingsToSave) => {
    setSubmitStatus(`Saving placeholder mappings for ${reportNameForSave}...`);
    setIsSubmitting(true);
    try {
        const finalizeUrl = `${backendBaseUrl}/report_definitions/${encodeURIComponent(reportNameForSave)}/finalize_template`;
        const response = await extensionSDK.fetchProxy(finalizeUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ report_name: reportNameForSave, mappings: mappingsToSave })
        });
        const responseData = response.body;
        if (!response.ok) {
            throw new Error(responseData.detail || "Failed to save placeholder mappings.");
        }
        setSubmitStatus(`Placeholder mappings saved successfully for ${reportNameForSave}: ${responseData.message}. Report template has been updated.`);
    } catch (error) {
        setSubmitStatus(`Error saving placeholder mappings: ${error.message}`);
    } finally {
        setIsSubmitting(false);
        setIsPlaceholderModalOpen(false);
    }
  };

  const handleSubmitDefinition = async () => {
    if (!sdkReady) { alert("Looker SDK not available."); return; }
    if (!reportName || !imageUrl || !baseSql || !promptText) { alert("All required fields must be filled."); return; }
    
    let parsedMappings = {};
    if (userAttributeMappings.trim() !== "") {
        try {
            parsedMappings = JSON.parse(userAttributeMappings);
        } catch (e) { alert(`Error: Invalid JSON in User Attribute Mappings: ${e.message}`); return; }
    }

    setIsSubmitting(true);
    setSubmitStatus('Submitting definition...');
    setLastSuccessfullyDefinedReport('');
    setSchemaForLastDefinedReport([]);

    const finalLookConfigs = lookConfigs
      .filter(lc => lc.lookId && lc.placeholderName.trim())
      .map(lc => ({
        look_id: parseInt(lc.lookId, 10),
        placeholder_name: lc.placeholderName.trim()
      }));

    const definitionPayload = {
      report_name: reportName,
      image_url: imageUrl,
      sql_query: baseSql,
      prompt: promptText,
      look_configs: finalLookConfigs,
      user_attribute_mappings: parsedMappings,
      field_display_configs: fieldDisplayConfigurations,
      calculation_row_configs: calculationRows,
      subtotal_configs: [],
    };

    const currentReportNameForNextSteps = reportName;
    const currentSchemaSnapshot = [...currentSchemaForConfig];
    const fastapiReportDefsUrl = `${backendBaseUrl}/report_definitions`;

    try {
      const response = await extensionSDK.fetchProxy(fastapiReportDefsUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(definitionPayload),
      });
      const responseData = response.body;
      if (!response.ok) {
        throw new Error(responseData.detail || `Error submitting definition: ${response.status}`);
      }

      setSubmitStatus(`Success! Report definition '${currentReportNameForNextSteps}' saved. Path: ${responseData.template_html_gcs_path || 'N/A'}.`);
      setLastSuccessfullyDefinedReport(currentReportNameForNextSteps);
      setSchemaForLastDefinedReport(currentSchemaSnapshot);

      await fetchAndLogPlaceholders(currentReportNameForNextSteps);
      await handlePreviewReport(currentReportNameForNextSteps);

      setReportName(''); setImageUrl(''); setBaseSql(''); setPromptText('');
      setUserAttributeMappings(''); setLookConfigs([]); setCurrentSchemaForConfig([]);
      setFieldDisplayConfigurations([]); setCalculationRows([]); setDryRunError('');
    } catch (error) {
      setSubmitStatus(`Error submitting definition: ${error.message}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const openPlaceholderDialogForLastReport = () => {
    if (lastSuccessfullyDefinedReport) {
        if (!schemaForLastDefinedReport || schemaForLastDefinedReport.length === 0) {
            alert("Schema for the selected report is not available. Please 'Configure Column Display' on a new report definition first.");
            return;
        }
        setCurrentReportForPlaceholders(lastSuccessfullyDefinedReport);
        setIsPlaceholderModalOpen(true);
    } else {
        alert("No report has been successfully defined yet in this session.");
    }
  };

  const isConfigurationReady = sdkReady;

  return (
    <FormWrapper>
      <Heading as="h1" mb="large">Define New GenAI Report</Heading>
      
      <FormGroup>
        <Label htmlFor="reportName">Report Definition Name <span style={{color: 'red'}}>*</span></Label>
        <Input type="text" id="reportName" value={reportName} onChange={(e) => setReportName(e.target.value)} placeholder="e.g., Monthly Sales Performance" disabled={!isConfigurationReady || isSubmitting}/>
      </FormGroup>

      <FormGroup>
        <Label htmlFor="imageUrl">Image URL (for styling guidance) <span style={{color: 'red'}}>*</span></Label>
        <Input type="text" id="imageUrl" value={imageUrl} onChange={(e) => setImageUrl(e.target.value)} placeholder="https://example.com/style_image.jpg" disabled={!isConfigurationReady || isSubmitting}/>
      </FormGroup>
      
       <FormGroup>
        <Label htmlFor="promptText">Base Prompt for Gemini (Template Generation) <span style={{color: 'red'}}>*</span></Label>
        <Textarea id="promptText" value={promptText} onChange={(e) => setPromptText(e.target.value)} placeholder="Generate an HTML template with placeholders..." rows={6} disabled={!isConfigurationReady || isSubmitting}/>
      </FormGroup>

      <FormGroup>
        <Label htmlFor="baseSql">Base SQL Query <span style={{color: 'red'}}>*</span></Label>
        <Textarea id="baseSql" value={baseSql} onChange={(e) => setBaseSql(e.target.value)} placeholder="SELECT ..." rows={8} disabled={!isConfigurationReady || isSubmitting}/>
      </FormGroup>

      {/* Multiple Look Configs UI */}
      <FormGroup>
        <Label>Embed Looks as Images (Optional)</Label>
        <Description>Add charts from saved Looks. Provide the Look ID and a unique placeholder name for Gemini to use in the template (e.g., `sales_trend_chart`).</Description>
        <Box mt="medium">
            {lookConfigs.map((config) => (
                <Space key={config.id} mb="small" width="100%" gap="small">
                    <FieldText
                        label="Look ID"
                        value={config.lookId}
                        onChange={(e) => handleLookConfigChange(config.id, 'lookId', e.target.value)}
                        placeholder="e.g., 123"
                        type="number"
                    />
                    <FieldText
                        label="Placeholder Name"
                        value={config.placeholderName}
                        onChange={(e) => handleLookConfigChange(config.id, 'placeholderName', e.target.value)}
                        placeholder="e.g., sales_trend_chart"
                        description="Use letters, numbers, underscores"
                    />
                    <IconButton
                        icon={<Delete />}
                        label="Remove Look"
                        onClick={() => handleRemoveLookConfig(config.id)}
                        mt="large"
                    />
                </Space>
            ))}
        </Box>
        <LookerButton 
            onClick={handleAddLookConfig} 
            iconBefore={<Add />} 
            mt="small"
            size="small"
            disabled={isSubmitting}
        >
            Add Chart from Look
        </LookerButton>
      </FormGroup>

      <FormGroup>
        <Label htmlFor="userAttributeMappings">User Attribute Mappings (JSON)</Label>
        <Textarea id="userAttributeMappings" value={userAttributeMappings} onChange={(e) => setUserAttributeMappings(e.target.value)} placeholder='e.g., {"looker_attribute_name": "bq_column_name"}' rows={3} disabled={!isConfigurationReady || isSubmitting} />
      </FormGroup>

      <FormGroup>
        <LookerButton
            onClick={handleDryRunAndConfigure}
            disabled={!isConfigurationReady || dryRunLoading || !baseSql.trim() || isSubmitting}
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
      
      {isPlaceholderModalOpen && currentReportForPlaceholders && (
          <PlaceholderMappingDialog
              isOpen={isPlaceholderModalOpen}
              onClose={() => {setIsPlaceholderModalOpen(false); setCurrentReportForPlaceholders('');}}
              reportName={currentReportForPlaceholders}
              discoveredPlaceholders={discoveredPlaceholders}
              schema={schemaForLastDefinedReport}
              onApplyMappings={handleSavePlaceholderMappings}
          />
      )}

      <FormGroup style={{ marginTop: '30px' }}>
        <Button
            type="button"
            onClick={handleSubmitDefinition}
            disabled={!isConfigurationReady || isSubmitting || dryRunLoading}
        >
          {isSubmitting ? "Saving Definition..." : "Create/Update Report Definition & Preview"}
        </Button>
      </FormGroup>

      {lastSuccessfullyDefinedReport && (
          <FormGroup style={{ marginTop: '10px' }}>
            <LookerButton
                type="button"
                onClick={openPlaceholderDialogForLastReport}
                disabled={isSubmitting || dryRunLoading || !schemaForLastDefinedReport || schemaForLastDefinedReport.length === 0}
                width="100%"
                color="neutral"
            >
              Configure Placeholders for '{lastSuccessfullyDefinedReport}'
            </LookerButton>
          </FormGroup>
      )}

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
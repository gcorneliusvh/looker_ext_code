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
} from '@looker/components';

import FieldDisplayConfigurator from './FieldDisplayConfigurator';
import PlaceholderMappingDialog from './PlaceholderMappingDialog';

// --- Styled Components (remain the same) ---
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

  const [isFieldConfigModalOpen, setIsFieldConfigModalOpen] = useState(false);
  const [currentSchemaForConfig, setCurrentSchemaForConfig] = useState([]);
  const [fieldDisplayConfigurations, setFieldDisplayConfigurations] = useState([]);

  const [calculationRows, setCalculationRows] = useState([]);

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

  const [discoveredPlaceholders, setDiscoveredPlaceholders] = useState([]);
  const [isPlaceholderModalOpen, setIsPlaceholderModalOpen] = useState(false);
  const [currentReportForPlaceholders, setCurrentReportForPlaceholders] = useState('');
  const [lastSuccessfullyDefinedReport, setLastSuccessfullyDefinedReport] = useState('');
  const [schemaForLastDefinedReport, setSchemaForLastDefinedReport] = useState([]); // New state for schema snapshot


  const { extensionSDK } = useContext(ExtensionContext);
  const [sdkReady, setSdkReady] = useState(false);

  const backendBaseUrl = 'https://looker-ext-code-17837811141.us-central1.run.app';

  useEffect(() => {
    // ... (useEffect for SDK readiness, client attribute, manifest constants - same as before) ...
    let isMounted = true;
    if (extensionSDK) {
        if (extensionSDK.lookerHostData) {
            if(isMounted) setSdkReady(true);
        } else {
            if(isMounted) setSdkReady(false);
            console.warn("ReportForm: SDK object present, but lookerHostData is not yet available.");
        }
    } else {
        if(isMounted) {
            setSdkReady(false);
            setConfigError("Looker SDK not available. Extension may not function correctly.");
        }
        console.error("ReportForm: ExtensionSDK not available.");
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
      } catch (error) { console.error("ReportForm: Error fetching client_id:", error); if (isMounted) setUserAttributesError(`Error fetching client_id: ${error.message}`);
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
        } catch (error) { console.error("ReportForm: Error loading manifest constants:", error); setConstantsError(`Error loading constants: ${error.message}`);
        } finally { setConstantsLoading(false); }
    };

    if (extensionSDK) { fetchClientAttribute(); loadManifestConstants(); }
    else { if(isMounted) { setUserAttributesLoading(false); setConstantsLoading(false); setUserAttributesError("SDK not ready for UA."); setConstantsError("SDK not ready for MC.");}}

    return () => { isMounted = false; };
  }, [extensionSDK]);


  const handleDryRunAndConfigure = async () => {
    if (!sdkReady) { const errMsg = "Looker SDK is not fully initialized."; setDryRunError(errMsg); alert(errMsg); return; }
    if (!baseSql.trim()) { setDryRunError("Base SQL query cannot be empty."); alert("Base SQL query cannot be empty."); return; }
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
        setCurrentSchemaForConfig(responseData.schema); // This is the schema for the *current* SQL input
        setIsFieldConfigModalOpen(true);
      } else {
        throw new Error(responseData?.detail || responseData?.message || responseData?.error || "Dry run did not return a schema or was unsuccessful.");
      }
    } catch (error) {
        console.error("Dry run error (fetchProxy):", error);
        const errorMessage = error.message || (error.body?.detail) || "Unknown API error during dry run.";
        setDryRunError(`Dry run failed: ${errorMessage}`);
        alert(`Dry run failed: ${errorMessage}`);
    } finally { setDryRunLoading(false); }
  };

  const handleApplyFieldConfigs = (configs) => {
    setFieldDisplayConfigurations(configs); setIsFieldConfigModalOpen(false);
    setSubmitStatus("Field configurations applied. Ready to save or create report."); setDryRunError('');
  };
  const handleCloseConfigModal = () => { setIsFieldConfigModalOpen(false); };

  const fetchAndLogPlaceholders = async (nameOfReport) => {
    // ... (same as previous version - updates submitStatus and discoveredPlaceholders) ...
    if (!sdkReady) {
        console.error("ReportForm: SDK not available for fetching placeholders.");
        setSubmitStatus(prev => prev + " Error: SDK not ready for placeholder discovery.");
        return;
    }
    if (!nameOfReport) {
        console.error("ReportForm: Report name is missing for placeholder discovery.");
        setSubmitStatus(prev => prev + " Error: Report name needed for placeholder discovery.");
        return;
    }
    setSubmitStatus(prev => prev + ` Checking template placeholders for '${nameOfReport}'...`);
    try {
        const discoverUrl = `${backendBaseUrl}/report_definitions/${encodeURIComponent(nameOfReport)}/discover_placeholders`;
        const response = await fetch(discoverUrl, {
            method: 'GET',
            headers: { 'Accept': 'application/json', 'ngrok-skip-browser-warning': 'true' }
        });
        const contentType = response.headers.get("content-type");
        if (!response.ok) {
            let errorDetail = `HTTP error ${response.status} fetching placeholders.`;
            if (contentType && contentType.indexOf("application/json") !== -1) {
                const errorData = await response.json().catch(() => ({})); errorDetail = errorData.detail || errorDetail;
            } else {
                const textError = await response.text().catch(() => ""); errorDetail += ` Response not JSON. Body: ${textError.substring(0,100)}...`;
            }
            throw new Error(errorDetail);
        }
        const data = await response.json();
        if (data.template_found && data.placeholders) {
            console.log(`ReportForm: Discovered ${data.placeholders.length} placeholders for report '${nameOfReport}'.`);
            setDiscoveredPlaceholders(data.placeholders);
            setSubmitStatus(prev => prev.replace("Checking template placeholders...", "") + ` Found ${data.placeholders.length} placeholders. Configuration is optional.`);
        } else {
            console.warn(`ReportForm: Template not found or error discovering placeholders for '${nameOfReport}'. Message: ${data.error_message}`);
            setSubmitStatus(prev => prev.replace("Checking template placeholders...", "") + ` Warning: ${data.error_message || 'Could not discover placeholders.'}`);
            setDiscoveredPlaceholders([]);
        }
    } catch (error) {
        console.error("ReportForm: Error fetching placeholders:", error);
        setSubmitStatus(prev => prev.replace("Checking template placeholders...", "") + ` Error discovering placeholders: ${error.message}`);
        setDiscoveredPlaceholders([]);
    }
  };

  const handlePreviewReport = async (reportNameToPreview) => {
    // ... (same as previous version - generates and opens preview) ...
    if (!reportNameToPreview) {
        setSubmitStatus(prev => prev + " Cannot preview: Report name missing.");
        return;
    }
    setSubmitStatus(prev => prev + ` Attempting to generate preview for '${reportNameToPreview}'...`);
    setIsSubmitting(true);

    try {
        const executionPayload = {
            report_definition_name: reportNameToPreview,
            filter_criteria_json: JSON.stringify({})
        };
        const fastapiExecuteUrl = `${backendBaseUrl}/execute_report`;
        const response = await fetch(fastapiExecuteUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'ngrok-skip-browser-warning': 'true' },
            body: JSON.stringify(executionPayload),
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Preview generation failed: ${response.status} ${response.statusText}. Server: ${errorText.substring(0, 300)}`);
        }

        const responseData = await response.json();
        if (responseData && responseData.report_url_path) {
            const fullReportUrl = backendBaseUrl + responseData.report_url_path;
            if (extensionSDK && extensionSDK.openBrowserWindow) {
                extensionSDK.openBrowserWindow(fullReportUrl, '_blank');
                setSubmitStatus(prev => prev.replace("Attempting to generate preview...", "") + ` Preview for '${reportNameToPreview}' opened.`);
            } else {
                throw new Error("Looker SDK not available to open report window.");
            }
        } else {
            throw new Error("Failed to get report URL from backend for preview.");
        }
    } catch (error) {
        console.error("ReportForm: Error generating report preview:", error);
        setSubmitStatus(prev => prev.replace("Attempting to generate preview...", "") + ` Error generating preview: ${error.message}`);
    } finally {
        setIsSubmitting(false);
    }
};


  const handleSavePlaceholderMappings = async (reportNameForSave, mappingsToSave) => {
    // ... (same as previous version - saves mappings) ...
    console.log("ReportForm: Saving placeholder mappings for", reportNameForSave, mappingsToSave);
    setSubmitStatus(`Saving placeholder mappings for ${reportNameForSave}...`);
    setIsSubmitting(true);

    try {
        const finalizeUrl = `${backendBaseUrl}/report_definitions/${encodeURIComponent(reportNameForSave)}/finalize_template`;
        const response = await fetch(finalizeUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'ngrok-skip-browser-warning': 'true' },
            body: JSON.stringify({ report_name: reportNameForSave, mappings: mappingsToSave })
        });
        const contentType = response.headers.get("content-type");
        if (!response.ok) {
            let errorDetail = `HTTP error ${response.status} saving mappings.`;
            if (contentType && contentType.indexOf("application/json") !== -1) {
                const errorData = await response.json().catch(() => ({})); errorDetail = errorData.detail || errorDetail;
            } else {
                const textError = await response.text().catch(() => ""); errorDetail += ` Response not JSON. Body: ${textError.substring(0,100)}...`;
            }
            throw new Error(errorDetail);
        }
        const responseData = await response.json();
        setSubmitStatus(`Placeholder mappings saved successfully for ${reportNameForSave}: ${responseData.message}. Report definition is now finalized.`);
    } catch (error) {
        console.error("ReportForm: Error saving placeholder mappings:", error);
        setSubmitStatus(`Error saving placeholder mappings: ${error.message}`);
    } finally {
        setIsSubmitting(false);
        setIsPlaceholderModalOpen(false);
    }
  };

  const handleSubmitDefinition = async () => {
    if (!sdkReady) { const errMsg = "Looker SDK not available."; setSubmitStatus(`Error: ${errMsg}`); alert(errMsg); return; }
    if (!reportName || !imageUrl || !baseSql || !promptText) { setSubmitStatus("Error: All required fields must be filled."); alert("Error: All required fields must be filled."); return; }

    // Ensure currentSchemaForConfig is populated if fieldDisplayConfigurations are present
    // Or if it's required for placeholder configuration later.
    // A robust check would be that if fieldDisplayConfigurations exist, currentSchemaForConfig must too.
    if (fieldDisplayConfigurations.length > 0 && (!currentSchemaForConfig || currentSchemaForConfig.length === 0)) {
        alert("Field configurations exist, but the schema seems to be missing. Please 'Validate & Configure Fields' first.");
        return;
    }

    let parsedMappings = {};
    // ... (JSON parsing for userAttributeMappings - same as before) ...
    if (userAttributeMappings.trim() !== "") {
        try {
            parsedMappings = JSON.parse(userAttributeMappings);
            if (typeof parsedMappings !== 'object' || parsedMappings === null || Array.isArray(parsedMappings)) {
                throw new Error("User Attribute Mappings must be a valid JSON object.");
            }
        } catch (e) { setSubmitStatus(`Error: Invalid JSON in User Attribute Mappings: ${e.message}`); alert(`Error: Invalid JSON: ${e.message}`); return; }
    }


    setIsSubmitting(true);
    setSubmitStatus('Submitting definition...');
    setLastSuccessfullyDefinedReport('');
    setSchemaForLastDefinedReport([]); // Reset schema for last report

    const definitionPayload = {
      report_name: reportName,
      image_url: imageUrl,
      sql_query: baseSql,
      prompt: promptText,
      user_attribute_mappings: parsedMappings,
      field_display_configs: fieldDisplayConfigurations,
      calculation_row_configs: calculationRows.map(r => ({
          row_label: r.rowLabel, // Ensure your calculationRows state management provides these
          values_placeholder_name: r.valuesPlaceholderName,
          calculated_values: r.calculatedValues.map(cv => ({
              target_field_name: cv.targetFieldName,
              calculation_type: cv.calculationType,
              number_format: cv.numberFormat || null,
              alignment: cv.alignment || null,
          }))
      })),
      subtotal_configs: [],
    };

    const currentReportNameForNextSteps = reportName;
    const currentSchemaSnapshot = [...currentSchemaForConfig]; // Take a snapshot before clearing
    const fastapiReportDefsUrl = `${backendBaseUrl}/report_definitions`;

    try {
      const response = await fetch(fastapiReportDefsUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'ngrok-skip-browser-warning': 'true' },
        body: JSON.stringify(definitionPayload),
      });
      // ... (response error handling - same as before) ...
      const contentType = response.headers.get("content-type");
      if (!response.ok) {
        let errorDetail = `HTTP error ${response.status}: ${response.statusText}`;
        if (contentType && contentType.indexOf("application/json") !== -1) {
            const jsonData = await response.json().catch(() => ({})); errorDetail = jsonData.detail || errorDetail;
        } else {
            const responseTextForError = await response.text().catch(() => ""); errorDetail += responseTextForError ? ` - Server response: ${responseTextForError.substring(0,200)}...` : " - Server response was empty.";
        }
        throw new Error(errorDetail);
      }

      const responseData = await response.json();
      setSubmitStatus(`Success! Report definition '${currentReportNameForNextSteps}' saved. Path: ${responseData.template_html_gcs_path || 'N/A'}.`);
      setLastSuccessfullyDefinedReport(currentReportNameForNextSteps);
      setSchemaForLastDefinedReport(currentSchemaSnapshot); // Set the snapshotted schema

      await fetchAndLogPlaceholders(currentReportNameForNextSteps);
      await handlePreviewReport(currentReportNameForNextSteps);

      setReportName(''); setImageUrl(''); setBaseSql(''); setPromptText(''); setUserAttributeMappings('');
      setCurrentSchemaForConfig([]); // Clear the working schema
      setFieldDisplayConfigurations([]);
      setCalculationRows([]);
      setDryRunError('');

    } catch (error) {
      console.error("Error submitting definition:", error);
      setSubmitStatus(`Error submitting definition: ${error.message}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const openPlaceholderDialogForLastReport = () => {
    if (lastSuccessfullyDefinedReport) {
        // Use the snapshotted schema
        if (!schemaForLastDefinedReport || schemaForLastDefinedReport.length === 0) {
            alert("Schema for the selected report is not available. This may happen if the 'Configure Column Display' step was skipped or failed before defining the report.");
            return; // Prevent opening dialog if schema is missing
        }
        setCurrentReportForPlaceholders(lastSuccessfullyDefinedReport);
        // discoveredPlaceholders state is already populated by fetchAndLogPlaceholders
        setIsPlaceholderModalOpen(true);
    } else {
        alert("No report has been successfully defined yet in this session, or the page was reloaded.");
    }
  };


  const overallLoading = userAttributesLoading || constantsLoading;
  const isConfigurationReady = sdkReady && backendBaseUrl && backendBaseUrl !== 'YOUR_HARDCODED_NGROK_URL_HERE';

  return (
    <FormWrapper>
      <h1 style={{ marginBottom: '25px', textAlign: 'left', fontSize: '24px' }}>Define New GenAI Report</h1>
      {/* ... Config alert ... */}

      {/* ... Form groups for reportName, imageUrl, promptText, userAttributeMappings, baseSql ... */}
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

      {/* ... Summary Rows Logic and Overall Calculation Rows sections ... */}
      {currentSchemaForConfig && currentSchemaForConfig.length > 0 && (
        <Box mt="large" pt="medium" borderTop="1px solid" borderColor="ui3">
            <Heading as="h4" mb="small">Summary Rows Logic</Heading>
            <p style={{color: 'grey', fontSize: 'small'}}>
                Summary actions (e.g., 'Subtotals When Group Changes', 'Grand Totals Only') can be configured per string field in the "Configure Column Display" section.
                Numeric fields can have their aggregation type (Sum, Avg, etc.) set there too. The backend uses these settings to generate summary rows.
            </p>
        </Box>
      )}
      {currentSchemaForConfig && currentSchemaForConfig.length > 0 && (
        <Box mt="large" pt="medium" borderTop="1px solid" borderColor="ui3">
            <Heading as="h4" mb="medium">Overall Calculation Rows</Heading>
            <p style={{color: 'grey', fontSize: 'small'}}>UI for defining *additional* overall calculation rows (e.g., specific custom footers beyond standard totals) is planned for a future update.</p>
        </Box>
      )}


      {isPlaceholderModalOpen && currentReportForPlaceholders && (
          <PlaceholderMappingDialog
              isOpen={isPlaceholderModalOpen}
              onClose={() => {
                  setIsPlaceholderModalOpen(false);
                  setCurrentReportForPlaceholders('');
                  // Consider if discoveredPlaceholders should be cleared here or if it should persist
                  // for the last report until a new definition cycle starts.
                  // setDiscoveredPlaceholders([]);
              }}
              reportName={currentReportForPlaceholders}
              discoveredPlaceholders={discoveredPlaceholders} // Pass the currently discovered placeholders
              schema={schemaForLastDefinedReport} // Pass the snapshotted schema
              onApplyMappings={handleSavePlaceholderMappings}
          />
      )}

      <DebugBox>
        {/* ... DebugBox content ... */}
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
      </DebugBox>

      <FormGroup style={{ marginTop: '30px' }}>
        <Button
            type="button"
            onClick={handleSubmitDefinition}
            disabled={!isConfigurationReady || isSubmitting || overallLoading || dryRunLoading}
        >
          {isSubmitting ? "Saving Definition..." : "Create/Update Report Definition & Preview"}
        </Button>
      </FormGroup>

      {lastSuccessfullyDefinedReport && (
          <FormGroup style={{ marginTop: '10px' }}>
            <LookerButton
                type="button"
                onClick={openPlaceholderDialogForLastReport}
                disabled={isSubmitting || overallLoading || dryRunLoading || !schemaForLastDefinedReport || schemaForLastDefinedReport.length === 0}
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
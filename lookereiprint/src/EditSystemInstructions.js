// src/EditSystemInstructions.js
import React, { useState, useEffect, useContext } from 'react';
import styled from 'styled-components';
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Heading,
    Box,
    Button,
    Spinner,
    Paragraph,
    TextArea,
    Space
} from '@looker/components';

const Wrapper = styled(Box)`
  padding: ${({ theme }) => theme.space.large};
  max-width: 800px;
  margin: 0 auto;
`;

const StyledTextarea = styled(TextArea)`
  width: 100%;
  min-height: 250px; 
  font-family: monospace; 
  font-size: ${({ theme }) => theme.fontSizes.small};
`;

const FeedbackBox = styled(Paragraph)`
  margin-top: ${({ theme }) => theme.space.medium};
  padding: ${({ theme }) => theme.space.small};
  border-radius: ${({ theme }) => theme.radii.medium};
  border: 1px solid transparent;

  &.success {
    border-color: ${({ theme }) => theme.colors.positive};
    background-color: ${({ theme }) => theme.colors.positiveSubtle};
    color: ${({ theme }) => theme.colors.positive};
  }

  &.error {
    border-color: ${({ theme }) => theme.colors.critical};
    background-color: ${({ theme }) => theme.colors.criticalSubtle};
    color: ${({ theme }) => theme.colors.critical};
  }
`;

function EditSystemInstructions() {
  const [instructionText, setInstructionText] = useState('');
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');

  const { extensionSDK } = useContext(ExtensionContext);
  
  // ---------------------------------------------------------------------------
  // CRITICAL: Ensure this is your ACTUAL NGROK or other backend URL
  // ---------------------------------------------------------------------------
  const [backendUrl, setBackendUrl] = useState('https://c530-207-216-175-143.ngrok-free.app'); 
  // ---------------------------------------------------------------------------

  const isBackendUrlPlaceholder = backendUrl === 'YOUR_NGROK_OR_FASTAPI_BACKEND_URL';

  useEffect(() => {
    if (isBackendUrlPlaceholder) {
        const msg = "Backend URL is not configured in EditSystemInstructions.js. Please update it.";
        setError(msg); 
        console.error(msg); 
        setIsLoading(false);
    } else {
        setError(prevError => prevError === "Backend URL is not configured in EditSystemInstructions.js. Please update it." ? "" : prevError);
    }
  }, [isBackendUrlPlaceholder]); 

  useEffect(() => {
    if (isBackendUrlPlaceholder || !extensionSDK) {
      return;
    }
    const fetchInstructions = async () => {
      setIsLoading(true);
      setError(prevError => isBackendUrlPlaceholder ? prevError : "");
      setSuccessMessage('');
      try {
        // ... (fetch logic from Stage 5.1 / previous working version) ...
        const response = await extensionSDK.fetchProxy(`${backendUrl}/system_instruction`, {
          method: 'GET',
          headers: { 'Accept': 'application/json', 'ngrok-skip-browser-warning': 'true' }
        });
        if (!response.ok) { 
          let errorDetail = `HTTP error ${response.status}`;
          try {
            const errBody = response.body || (typeof response.text === 'function' ? await response.text() : null);
            if (typeof errBody === 'object' && errBody.detail) errorDetail = errBody.detail;
            else if (typeof errBody === 'string' && errBody.length > 0) errorDetail = errBody;
          } catch (e) { /* Use default errorDetail */ }
          throw new Error(errorDetail);
        }
        let data;
        if (response.body && typeof response.body === 'object') { data = response.body; }
        else if (typeof response.json === 'function') { data = await response.json(); }
        else if (typeof response.text === 'function') {
            const responseText = await response.text();
            if (!responseText) { data = { system_instruction: "" }; }
            else { try { data = JSON.parse(responseText); } catch (e) { throw new Error(`Server returned non-JSON text: ${responseText.substring(0, 100)}...`); }}
        } else { throw new Error("Received an unexpected response structure."); }
        if (data && typeof data.system_instruction !== 'undefined') { setInstructionText(data.system_instruction); }
        else { setInstructionText(''); }
      } catch (err) {
        setError(prevError => isBackendUrlPlaceholder ? prevError : `Failed to fetch system instructions: ${err.message}`);
      } finally {
        setIsLoading(false);
      }
    };
    fetchInstructions();
  }, [extensionSDK, backendUrl, isBackendUrlPlaceholder]);

  const handleSave = async () => {
    if (isBackendUrlPlaceholder) {
      setError("Backend URL not configured. Cannot save.");
      return;
    }
    // ... (save logic from Stage 5.1 / previous working version) ...
    setIsSaving(true);
    setError(prevError => isBackendUrlPlaceholder ? prevError : "");
    setSuccessMessage('');
    try {
      const response = await extensionSDK.fetchProxy(`${backendUrl}/system_instruction`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'ngrok-skip-browser-warning': 'true'
        },
        body: JSON.stringify({ system_instruction: instructionText }),
      });
      if (!response.ok) {
        let errorDetail = `Save failed: HTTP error ${response.status}`;
        try {
            const errBody = response.body || (typeof response.text === 'function' ? await response.text() : null);
            if (typeof errBody === 'object' && errBody.detail) errorDetail = errBody.detail;
            else if (typeof errBody === 'string' && errBody.length > 0) errorDetail = errBody;
        } catch (e) { /* Use default errorDetail */ }
        throw new Error(errorDetail);
      }
      let saveData;
      if (response.body && typeof response.body === 'object') { saveData = response.body; }
      else if (typeof response.json === 'function') { saveData = await response.json(); }
      else if (typeof response.text === 'function') {
          const saveResponseText = await response.text();
          try { saveData = JSON.parse(saveResponseText); } 
          catch (e) { throw new Error("Save successful, but response was not valid JSON: " + saveResponseText.substring(0,100)); }
      } else { throw new Error("Save successful, but received an unexpected response structure."); }
      setSuccessMessage(saveData.message || 'System instructions saved successfully!');
    } catch (err) {
      setError(prevError => isBackendUrlPlaceholder ? prevError : `Failed to save system instructions: ${err.message}`);
    } finally {
      setIsSaving(false);
    }
  };

  // CORRECTED onChange handler for StyledTextarea
  const handleTextChange = (event) => {
    console.log("--- Textarea onChange ---");
    console.log("Received from StyledTextarea (event object):", event);
    // Ensure event and event.target exist before trying to access event.target.value
    if (event && event.target && typeof event.target.value === 'string') {
      setInstructionText(event.target.value);
    } else {
      // This case should ideally not happen if it's a standard input change event
      console.warn("Received event, but event.target.value is not a string:", event);
    }
  };

  const isDisabled = isSaving || isLoading || isBackendUrlPlaceholder;
  
  // Conditionally log substring only if instructionText is a string
  let instructionPreview = "[instructionText is not a string or is empty]";
  if (typeof instructionText === 'string' && instructionText.length > 0) {
    instructionPreview = instructionText.substring(0, 50);
  } else if (typeof instructionText === 'string') {
    instructionPreview = "[empty string]";
  }

  console.log(
    "Rendering EditSystemInstructions. instructionText type:", typeof instructionText, "Preview:", instructionPreview,
    "| isDisabled:", isDisabled, 
    "| isSaving:", isSaving, 
    "| isLoading:", isLoading, 
    "| isBackendUrlPlaceholder:", isBackendUrlPlaceholder
  );

  if (isLoading && !isBackendUrlPlaceholder && error !== "Backend URL is not configured in EditSystemInstructions.js. Please update it.") { 
    return (
      <Wrapper display="flex" justifyContent="center" alignItems="center" height="300px">
        <Spinner />
      </Wrapper>
    );
  }

  return (
    <Wrapper>
      <Heading mb="large">Edit System Instructions</Heading>
      {isBackendUrlPlaceholder && error && (
        <FeedbackBox className="error" mb="large">
          <strong>Configuration Needed:</strong> {error}
        </FeedbackBox>
      )}
      <Paragraph mb="medium">
        These instructions guide the GenAI model when it generates HTML report templates.
        Changes saved here will be used for new report template generations.
      </Paragraph>

      <StyledTextarea
        value={instructionText} // instructionText should now always be a string
        onChange={handleTextChange} // Using the corrected handler
        disabled={isDisabled}
        placeholder="Enter system instructions for the GenAI model..."
        resize={true}
      />

      <Space mt="medium" between>
        <Button
          onClick={handleSave}
          disabled={isDisabled}
          color="key"
        >
          {isSaving ? <Spinner size={18} /> : 'Save Instructions'}
        </Button>
        {error && !isBackendUrlPlaceholder && ( 
          <FeedbackBox className="error">{error}</FeedbackBox>
        )}
        {successMessage && (
          <FeedbackBox className="success">{successMessage}</FeedbackBox>
        )}
      </Space>
    </Wrapper>
  );
}

export default EditSystemInstructions;
// src/RefineReportDialog.js (New File)
import React, { useState, useContext, useEffect } from 'react';
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Dialog,
    DialogLayout,
    Heading,
    IconButton,
    Button,
    TextArea,
    Box,
    Spinner,
    Paragraph,
    Space,
    CodeBlock, // Good for showing HTML if needed
} from '@looker/components';
import { Close } from '@styled-icons/material';

// This should match the backendBaseUrl in other frontend files
const BACKEND_BASE_URL = 'https://looker-ext-code-17837811141.us-central1.run.app'; // Update this URL

function RefineReportDialog({
    isOpen,
    onClose,
    reportName,
    // onRefinementSuccess, // Callback to notify parent of success, e.g., to refresh views
}) {
    const [refinementInstruction, setRefinementInstruction] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const [refinedHtmlPreview, setRefinedHtmlPreview] = useState(''); // Optional: to show the result
    const { extensionSDK } = useContext(ExtensionContext); // For fetchProxy if preferred

    const handleSubmitRefinement = async () => {
        if (!refinementInstruction.trim()) {
            setError("Please provide refinement instructions.");
            return;
        }
        setIsLoading(true);
        setError('');
        setRefinedHtmlPreview('');

        try {
            const refineUrl = `${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(reportName)}/refine_template`;
            const payload = { refinement_prompt_text: refinementInstruction };

            // Using extensionSDK.fetchProxy is often better for Looker extensions
            // Ensure your backend is configured for it (CORS might not be an issue with proxy)
            // OR use standard fetch with ngrok-skip header if directly calling an ngrok-proxied local backend.
            // For a deployed Cloud Run backend, fetchProxy or standard fetch (with appropriate auth if secured)
            const response = await fetch(refineUrl, { // Or extensionSDK.fetchProxy
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    // 'ngrok-skip-browser-warning': 'true', // If using ngrok and direct fetch
                    // Add Authorization header if your Cloud Run endpoint is secured and not public
                },
                body: JSON.stringify(payload),
            });

            const responseData = await response.json();

            if (!response.ok) {
                throw new Error(responseData.detail || responseData.message || `HTTP error ${response.status}`);
            }

            setRefinedHtmlPreview(responseData.refined_html_content); // Show the new HTML
            alert(`Report '${reportName}' refined successfully! The template in GCS has been updated.`);
            // if (onRefinementSuccess) {
            //     onRefinementSuccess(reportName, responseData.refined_html_content);
            // }
            onClose(); // Close the dialog on success
        } catch (err) {
            console.error("Refinement error:", err);
            setError(`Failed to refine template: ${err.message}`);
        } finally {
            setIsLoading(false);
        }
    };

    // Clear state when dialog opens/closes or reportName changes
    useEffect(() => {
        if (!isOpen) {
            setRefinementInstruction('');
            setError('');
            setRefinedHtmlPreview('');
            setIsLoading(false);
        }
    }, [isOpen]);


    return (
        <Dialog isOpen={isOpen} onClose={onClose} maxWidth="70vw" width="800px">
            <DialogLayout
                header={
                    <Box display="flex" justifyContent="space-between" alignItems="center" p="medium" borderBottom="ui1">
                        <Heading as="h3" mb="none">Refine Report Template: {reportName}</Heading>
                        <IconButton icon={<Close />} label="Close Dialog" onClick={onClose} size="small" />
                    </Box>
                }
                footer={
                    <Space between p="medium" borderTop="ui1">
                        <Button onClick={onClose} disabled={isLoading}>Cancel</Button>
                        <Button color="key" onClick={handleSubmitRefinement} disabled={isLoading}>
                            {isLoading ? <Spinner size={18} /> : 'Submit Refinement'}
                        </Button>
                    </Space>
                }
            >
                <Box p="large" style={{ maxHeight: '60vh', overflowY: 'auto' }}>
                    <Paragraph mb="medium">
                        Enter instructions below to modify the current HTML template for '{reportName}'.
                        The AI will attempt to apply your changes and update the template.
                    </Paragraph>
                    <TextArea
                        placeholder="e.g., 'Change the main table header background to dark blue and make the company logo smaller.'"
                        value={refinementInstruction}
                        onChange={(e) => setRefinementInstruction(e.target.value)}
                        disabled={isLoading}
                        height="150px"
                        width="100%"
                        mb="medium"
                    />
                    {error && <Paragraph color="critical">{error}</Paragraph>}
                    {refinedHtmlPreview && (
                        <Box mt="large">
                            <Heading as="h4" fontSize="small">Refined HTML Preview (excerpt):</Heading>
                            <CodeBlock language="html" fontSize="xsmall">
                                {refinedHtmlPreview.substring(0, 1000) + (refinedHtmlPreview.length > 1000 ? '...' : '')}
                            </CodeBlock>
                        </Box>
                    )}
                </Box>
            </DialogLayout>
        </Dialog>
    );
}

export default RefineReportDialog;
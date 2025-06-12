// src/HtmlEditorView.js
import React, { useState, useEffect, useContext, useRef } from 'react';
import { ExtensionContext } from '@looker/extension-sdk-react';
import { Editor } from '@tinymce/tinymce-react';
import {
    Box,
    Spinner,
    Space,
    Heading,
    Button,
    Flex,
    FlexItem,
    TextArea,
    Fieldset,
} from '@looker/components';
import { Code, ChevronLeft } from '@styled-icons/material';

const BACKEND_BASE_URL = 'https://looker-ext-code-17837811141.us-central1.run.app';

function HtmlEditorView({ report, onComplete }) {
    const [originalHtml, setOriginalHtml] = useState(''); 
    const [bodyContent, setBodyContent] = useState('');
    const [styleContent, setStyleContent] = useState('');
    const [isCssPanelVisible, setIsCssPanelVisible] = useState(false);
    const [isLoading, setIsLoading] = useState(true);
    const [isSaving, setIsSaving] = useState(false);
    const [error, setError] = useState('');
    const [tinymceApiKey, setTinymceApiKey] = useState(null);
    const { extensionSDK } = useContext(ExtensionContext);
    const editorRef = useRef(null);

    useEffect(() => {
        const styleId = 'tinymce-onboarding-fix';
        if (document.getElementById(styleId)) return;
        const style = document.createElement('style');
        style.id = styleId;
        style.innerHTML = `.tox-notification { display: none !important; }`;
        document.head.appendChild(style);
        return () => {
            const styleElement = document.getElementById(styleId);
            if (styleElement) styleElement.remove();
        };
    }, []);

    useEffect(() => {
        if (report) {
            const fetchConfigAndHtml = async () => {
                setIsLoading(true);
                setError('');
                try {
                    const configUrl = `${BACKEND_BASE_URL}/api/public_config`;
                    const configResponse = await extensionSDK.fetchProxy(configUrl);
                    if (!configResponse.ok || !configResponse.body.tinymce_api_key) {
                        throw new Error('TinyMCE API Key could not be retrieved.');
                    }
                    setTinymceApiKey(configResponse.body.tinymce_api_key);

                    const getUrl = `${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(report.ReportName)}/get_html`;
                    const htmlResponse = await extensionSDK.fetchProxy(getUrl);
                    if (!htmlResponse.ok) {
                        throw new Error(htmlResponse.body?.detail || `Error fetching HTML`);
                    }
                    
                    let htmlContent = htmlResponse.body.html_content || '';

                    // --- NEW & FINAL FIX START ---
                    // This regex finds a placeholder, the whitespace after it, and the full table tag that follows.
                    const pattern = /({{TABLE_ROWS_[a-zA-Z0-9_]+}})(\s*)(<table[\s\S]*?<\/table>)/gi;

                    const fixedHtml = htmlContent.replace(pattern, (match, placeholder, whitespace, tableHtml) => {
                        // In here, we operate *only* on the captured table HTML string.
                        // We find the opening tbody tag and insert the placeholder right after it.
                        const fixedTableHtml = tableHtml.replace(/(<tbody[^>]*>)/i, `$1${placeholder}`);
                        
                        // Return the whitespace and the newly modified table.
                        return `${whitespace}${fixedTableHtml}`;
                    });
                    // --- NEW & FINAL FIX END ---
                    
                    setOriginalHtml(fixedHtml);

                    const styleRegex = /<style[^>]*>([\s\S]*?)<\/style>/i;
                    const bodyRegex = /<body[^>]*>([\s\S]*?)<\/body>/i;
                    const styleMatch = fixedHtml.match(styleRegex);
                    const bodyMatch = fixedHtml.match(bodyRegex);

                    if (!bodyMatch) {
                        throw new Error("Could not parse the HTML body from the template. It might be corrupted.");
                    }
                    
                    setStyleContent(styleMatch ? styleMatch[1].trim() : '');
                    setBodyContent(bodyMatch[1].trim());

                } catch (err) {
                    setError(err.message);
                } finally {
                    setIsLoading(false);
                }
            };
            fetchConfigAndHtml();
        }
    }, [report, extensionSDK]);

    const handleSave = async () => {
        if (editorRef.current && originalHtml) {
            setIsSaving(true);
            const newBodyContent = editorRef.current.getContent();
            const newStyleContent = styleContent; 

            let finalHtml = originalHtml;

            const bodyRegex = /(<body[^>]*>)([\s\S]*?)(<\/body>)/i;
            const styleRegex = /(<style[^>]*>)([\s\S]*?)(<\/style>)/i;

            const bodyMatch = finalHtml.match(bodyRegex);
            if (bodyMatch) {
                const newBodyTag = `${bodyMatch[1]}${newBodyContent}${bodyMatch[3]}`;
                finalHtml = finalHtml.replace(bodyMatch[0], newBodyTag);
            }

            const styleMatch = finalHtml.match(styleRegex);
            if (styleMatch) {
                const newStyleTag = `${styleMatch[1]}\n${newStyleContent}\n${styleMatch[3]}`;
                finalHtml = finalHtml.replace(styleMatch[0], newStyleTag);
            }
            
            const saveUrl = `${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(report.ReportName)}/save_html`;
            try {
                const payload = { html_content: finalHtml };
                const response = await extensionSDK.fetchProxy(saveUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                if (!response.ok) throw new Error(response.body?.detail || `Error ${response.status}`);
                alert(response.body.message || 'HTML saved successfully!');
                onComplete();
            } catch (err) {
                alert(`Failed to save HTML: ${err.message}`);
            } finally {
                setIsSaving(false);
            }
        }
    };

    if (!report) {
        return <Box p="large">No report selected for editing.</Box>
    }
    
    return (
        <Box p="large" display="flex" flexDirection="column" width="100%" height="100%" gap="medium">
            <Space between>
                <Heading>HTML Editor: {report.ReportName}</Heading>
                <Space>
                    <Button iconBefore={isCssPanelVisible ? <ChevronLeft /> : <Code />} onClick={() => setIsCssPanelVisible(!isCssPanelVisible)}>
                        {isCssPanelVisible ? 'Hide CSS' : 'View CSS'}
                    </Button>
                    <Button onClick={onComplete} disabled={isSaving}>Cancel</Button>
                    <Button color="key" onClick={handleSave} disabled={isLoading || isSaving}>
                        {isSaving ? <Spinner size={20}/> : "Save as New Version"}
                    </Button>
                </Space>
            </Space>
            
            {isLoading ? (<Space around p="xxxxlarge"><Spinner /></Space>)
            : error ? (<Box p="large" color="critical" border="1px solid" borderColor="critical" borderRadius="medium">{error}</Box>)
            : (
                <Flex flex="1" border="1px solid" borderColor="ui3" borderRadius="medium">
                    {isCssPanelVisible && (
                        <FlexItem width="30%" p="medium" borderRight="1px solid" borderColor="ui3" display="flex" flexDirection="column" backgroundColor="ui1">
                            <Fieldset legend="CSS Styles" flex="1" display="flex" flexDirection="column">
                                <TextArea flex="1" fontFamily="monospace" fontSize="xsmall" value={styleContent} onChange={(e) => setStyleContent(e.target.value)} disabled={isLoading || isSaving} />
                            </Fieldset>
                        </FlexItem>
                    )}
                    <FlexItem flex="1" display="flex" flexDirection="column">
                         {!tinymceApiKey ? (
                            <Box p="large" color="critical">Configuration Error: TinyMCE API Key not found.</Box>
                         ) : (
                            <Editor
                                apiKey={tinymceApiKey}
                                onInit={(evt, editor) => editorRef.current = editor}
                                initialValue={bodyContent}
                                init={{
                                    height: '100%',
                                    resize: false,
                                    menubar: true,
                                    plugins: 'code lists advlist table link help wordcount fullscreen',
                                    toolbar: 'undo redo | blocks | bold italic | bullist numlist | code | fullscreen',
                                    content_style: styleContent,
                                }}
                            />
                         )}
                    </FlexItem>
                </Flex>
            )}
        </Box>
    );
}

export default HtmlEditorView;
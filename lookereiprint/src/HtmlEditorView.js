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
    TextArea
} from '@looker/components';

const BACKEND_BASE_URL = 'https://looker-ext-code-17837811141.us-central1.run.app';

// This is a placeholder for re-combining the HTML parts upon save
const SHELL_REPLACEMENT_STRING = '';

function HtmlEditorView({ report, onComplete }) {
    const [bodyContent, setBodyContent] = useState('');
    const [styleContent, setStyleContent] = useState('');
    const [htmlShell, setHtmlShell] = useState(''); // To store the HTML structure minus the body
    
    const [isLoading, setIsLoading] = useState(true);
    const [isSaving, setIsSaving] = useState(false);
    const [error, setError] = useState('');
    const [tinymceApiKey, setTinymceApiKey] = useState(null);
    const { extensionSDK } = useContext(ExtensionContext);
    const editorRef = useRef(null);

    useEffect(() => {
        if (report) {
            const fetchConfigAndHtml = async () => {
                setIsLoading(true);
                setError('');
                try {
                    // Fetch the API Key from backend
                    const configUrl = `${BACKEND_BASE_URL}/api/public_config`;
                    const configResponse = await extensionSDK.fetchProxy(configUrl, { method: 'GET' });
                    if (!configResponse.ok || !configResponse.body.tinymce_api_key) {
                        throw new Error('TinyMCE API Key could not be retrieved from the server.');
                    }
                    setTinymceApiKey(configResponse.body.tinymce_api_key);

                    // Fetch the full HTML content
                    const getUrl = `${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(report.ReportName)}/get_html`;
                    const htmlResponse = await extensionSDK.fetchProxy(getUrl, { method: 'GET' });
                    if (!htmlResponse.ok) {
                        throw new Error(htmlResponse.body?.detail || `Error fetching HTML`);
                    }
                    const fullHtml = htmlResponse.body.html_content || '';

                    // Split the HTML into parts
                    const styleRegex = /<style[^>]*>([\s\S]*?)<\/style>/i;
                    const bodyRegex = /<body[^>]*>([\s\S]*?)<\/body>/i;

                    const styleMatch = fullHtml.match(styleRegex);
                    const bodyMatch = fullHtml.match(bodyRegex);
                    
                    setStyleContent(styleMatch ? styleMatch[1] : '/* No <style> tag found. */');
                    setBodyContent(bodyMatch ? bodyMatch[1] : '');
                    
                    // Store the shell, replacing the body content with a placeholder
                    let shell = fullHtml;
                    if (bodyMatch) {
                        shell = shell.replace(bodyMatch[1], SHELL_REPLACEMENT_STRING);
                    }
                    if (styleMatch) {
                        // Also replace style content to avoid duplicating it on save
                        shell = shell.replace(styleMatch[1], ''); 
                    }
                    setHtmlShell(shell);

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
        if (editorRef.current) {
            setIsSaving(true);
            const newBodyContent = editorRef.current.getContent();

            // Recombine the HTML parts
            // 1. Put the new body content back into the shell
            let finalHtml = htmlShell.replace(SHELL_REPLACEMENT_STRING, newBodyContent);
            
            // 2. Put the new style content back into the <style> tag
            const styleRegex = /<style[^>]*>([\s\S]*?)<\/style>/i;
            finalHtml = finalHtml.replace(styleRegex, `<style>\n${styleContent}\n</style>`);
            
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
                console.error("Error saving HTML:", err);
                alert(`Failed to save HTML: ${err.message}`);
            } finally {
                setIsSaving(false);
            }
        }
    };

    if (!report) {
        return <Box p="large">No report selected for editing. Please go back to the "View & Run Reports" page.</Box>
    }
    
    return (
        <Box p="large" display="flex" flexDirection="column" height="100%" gap="medium">
            <Space between>
                <Heading>HTML Editor: {report.ReportName}</Heading>
                <Space>
                    <Button onClick={onComplete} disabled={isSaving}>Cancel</Button>
                    <Button color="key" onClick={handleSave} disabled={isLoading || isSaving}>
                        {isSaving ? <Spinner size={20}/> : "Save as New Version"}
                    </Button>
                </Space>
            </Space>
            
            <Flex flex="1" border="1px solid" borderColor="ui3" height="100%">
                <FlexItem width="30%" p="small" borderRight="1px solid" borderColor="ui3" display="flex" flexDirection="column" gap="small">
                    <Heading as="h4" fontSize="small" fontWeight="semiBold">CSS Styles</Heading>
                    <TextArea
                        flex="1"
                        fontFamily="monospace"
                        fontSize="xsmall"
                        value={styleContent}
                        onChange={(e) => setStyleContent(e.target.value)}
                        disabled={isLoading || isSaving}
                    />
                </FlexItem>
                <FlexItem flex="1">
                     {isLoading ? (
                        <Space around p="xxxxlarge"><Spinner /></Space>
                     ) : error ? (
                        <Box p="large" color="critical">{error}</Box>
                     ) : !tinymceApiKey ? (
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
                                content_css: 'default'
                            }}
                        />
                     )}
                </FlexItem>
            </Flex>
        </Box>
    );
}

export default HtmlEditorView;
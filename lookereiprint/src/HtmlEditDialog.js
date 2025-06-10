// src/HtmlEditDialog.js
import React, { useState, useEffect, useContext, useRef } from 'react';
import { ExtensionContext } from '@looker/extension-sdk-react';
import { Editor } from '@tinymce/tinymce-react';
import {
    Dialog,
    DialogLayout,
    Heading,
    Button,
    Box,
    Spinner,
    Space,
} from '@looker/components';

const BACKEND_BASE_URL = 'https://looker-ext-code-17837811141.us-central1.run.app';

function HtmlEditDialog({ isOpen, onClose, reportName, onSave }) {
    const [initialContent, setInitialContent] = useState('');
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState('');
    const { extensionSDK } = useContext(ExtensionContext);
    const editorRef = useRef(null);

    // IMPORTANT: Replace with your own TinyMCE API Key
    const TINYMCE_API_KEY = 'tchq0yfgaosd7z89mcqf5xat1cxo5h7w9y7mufm3gwbhkuv8';

    useEffect(() => {
        if (isOpen && reportName) {
            const fetchHtml = async () => {
                setIsLoading(true);
                setError('');
                try {
                    const getUrl = `${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(reportName)}/get_html`;
                    const response = await extensionSDK.fetchProxy(getUrl, { method: 'GET' });
                    if (!response.ok) {
                        throw new Error(response.body?.detail || `Error ${response.status}`);
                    }
                    setInitialContent(response.body.html_content || '');
                } catch (err) {
                    setError(`Failed to fetch HTML: ${err.message}`);
                } finally {
                    setIsLoading(false);
                }
            };
            fetchHtml();
        }
    }, [isOpen, reportName, extensionSDK]);

    const handleSave = () => {
        if (editorRef.current) {
            const newContent = editorRef.current.getContent();
            onSave(reportName, newContent);
        }
    };

    const handleClose = () => {
        setInitialContent(''); // Clear content on close
        onClose();
    };

    if (!isOpen) return null;

    if (TINYMCE_API_KEY === 'YOUR_TINYMCE_API_KEY') {
        return (
             <Dialog isOpen={isOpen} onClose={handleClose}>
                <DialogLayout header="Configuration Error">
                    <Box p="large">Please add your TinyMCE API key in HtmlEditDialog.js</Box>
                </DialogLayout>
            </Dialog>
        )
    }

    return (
        <Dialog isOpen={isOpen} onClose={handleClose} width="90vw" maxWidth="1600px">
            <DialogLayout
                header={
                    <Heading as="h3" p="medium" borderBottom="ui1">
                        Edit HTML Template: {reportName}
                    </Heading>
                }
                footer={
                    <Space between p="medium" borderTop="ui1">
                        <Button onClick={handleClose}>Cancel</Button>
                        <Button color="key" onClick={handleSave} disabled={isLoading}>
                            Save as New Version
                        </Button>
                    </Space>
                }
            >
                <Box height="80vh" display="flex" flexDirection="column">
                    {isLoading ? (
                        <Space around p="xxxxlarge"><Spinner /></Space>
                    ) : error ? (
                        <Box p="large" color="critical">{error}</Box>
                    ) : (
                        <Editor
                            apiKey={TINYMCE_API_KEY}
                            onInit={(evt, editor) => editorRef.current = editor}
                            initialValue={initialContent}
                            init={{
                                height: '100%',
                                menubar: true,
                                plugins: [
                                    'advlist', 'autolink', 'lists', 'link', 'image', 'charmap', 'preview',
                                    'anchor', 'searchreplace', 'visualblocks', 'code', 'fullscreen',
                                    'insertdatetime', 'media', 'table', 'help', 'wordcount'
                                ],
                                toolbar: 'undo redo | blocks | ' +
                                'bold italic forecolor | alignleft aligncenter ' +
                                'alignright alignjustify | bullist numlist outdent indent | ' +
                                'removeformat | code | help',
                                content_style: 'body { font-family:Helvetica,Arial,sans-serif; font-size:14px }'
                            }}
                        />
                    )}
                </Box>
            </DialogLayout>
        </Dialog>
    );
}

export default HtmlEditDialog;
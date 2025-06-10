// src/RevertDialog.js
import React, { useState, useContext } from 'react';
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Dialog,
    DialogLayout,
    Heading,
    Button,
    FieldText,
    Box,
    Spinner,
    Paragraph,
    Space,
} from '@looker/components';

const BACKEND_BASE_URL = 'https://looker-ext-code-17837811141.us-central1.run.app';

function RevertDialog({ isOpen, onClose, reportName, currentVersion, onRevertSuccess }) {
    const [targetVersion, setTargetVersion] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const { extensionSDK } = useContext(ExtensionContext);

    const handleRevert = async () => {
        const versionNum = parseInt(targetVersion, 10);
        if (isNaN(versionNum) || versionNum <= 0 || versionNum >= currentVersion) {
            setError(`Invalid version. Please enter a number between 1 and ${currentVersion - 1}.`);
            return;
        }

        setIsLoading(true);
        setError('');

        try {
            const revertUrl = `${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(reportName)}/revert`;
            const payload = { target_version: versionNum };

            const response = await extensionSDK.fetchProxy(revertUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            
            const responseData = response.body;

            if (!response.ok) {
                throw new Error(responseData.detail || `HTTP error ${response.status}`);
            }

            alert(responseData.message || 'Report reverted successfully!');
            onRevertSuccess(); // This will trigger a refresh in the parent
            handleClose();

        } catch (err) {
            console.error("Revert error:", err);
            setError(`Failed to revert template: ${err.message}`);
        } finally {
            setIsLoading(false);
        }
    };

    const handleClose = () => {
        setTargetVersion('');
        setError('');
        setIsLoading(false);
        onClose();
    };
    
    if (!isOpen) return null;

    return (
        <Dialog isOpen={isOpen} onClose={handleClose} maxWidth="500px">
            <DialogLayout
                header={
                    <Heading as="h3" p="medium" borderBottom="ui1">
                        Revert Report: {reportName}
                    </Heading>
                }
                footer={
                    <Space between p="medium" borderTop="ui1">
                        <Button onClick={handleClose} disabled={isLoading}>Cancel</Button>
                        <Button color="key" onClick={handleRevert} disabled={isLoading}>
                            {isLoading ? <Spinner size={18} /> : 'Revert to this Version'}
                        </Button>
                    </Space>
                }
            >
                <Box p="large">
                    <Paragraph>
                        Current template version is <strong>{currentVersion}</strong>.
                    </Paragraph>
                    <Paragraph>
                        Enter the version number you want to restore. This will create a new version ({currentVersion + 1}) with the content of the old version.
                    </Paragraph>
                    <FieldText
                        label="Version to Restore"
                        type="number"
                        value={targetVersion}
                        onChange={(e) => setTargetVersion(e.target.value)}
                        placeholder={`Enter version (e.g., 1 to ${currentVersion - 1})`}
                        disabled={isLoading}
                        validationMessage={error ? { type: 'error', message: error } : undefined}
                    />
                </Box>
            </DialogLayout>
        </Dialog>
    );
}

export default RevertDialog;
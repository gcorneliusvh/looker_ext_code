// src/ViewAllReports.js
import React, { useState, useEffect, useContext } from 'react';
import { ExtensionContext } from '@looker/extension-sdk-react';
import {
    Heading,
    Text,
    Box,
    List,
    ListItem,
    Space,
    Dialog,
    DialogLayout,
    IconButton,
    Spinner,
    Icon
} from '@looker/components';
import { FilterList, Close } from '@styled-icons/material';
import DynamicFilterUI from './DynamicFilterUI';

function ViewAllReports({ onSelectReportForFiltering }) {
  const [reports, setReports] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [isExecutingReport, setIsExecutingReport] = useState(false);

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedReportForModal, setSelectedReportForModal] = useState(null);

  const extensionContext = useContext(ExtensionContext);
  const { extensionSDK } = extensionContext;

  // !!! IMPORTANT: Use your current and correct ngrok URL base !!!
  const NGROK_BASE_URL = 'https://885d-2001-569-5925-3000-216-3eff-fe9a-a055.ngrok-free.app';


  useEffect(() => {
    const fetchReportDefinitions = async () => {
      if (!extensionSDK) {
        console.warn("ViewAllReports: extensionSDK not available yet for fetching definitions.");
      }
      console.log("ViewAllReports: Fetching real report definitions...");
      setIsLoading(true);
      setError('');
      
      const backendUrl = `${NGROK_BASE_URL}/report_definitions`;

      try {
        const response = await fetch(backendUrl, {
          method: 'GET',
          headers: {
            'Accept': 'application/json',
            'ngrok-skip-browser-warning': 'true'
          },
        });
        const responseText = await response.text();
        const contentType = response.headers.get('Content-Type');
        if (!response.ok) {
          console.error("ViewAllReports: Error fetching definitions - Response not OK", response.status, responseText.substring(0,500));
          throw new Error(`HTTP error fetching definitions! status: ${response.status} - ${response.statusText}. Response: ${responseText.substring(0, 200)}...`);
        }
        if (contentType && contentType.includes('application/json')) {
          const data = JSON.parse(responseText);
          const reportsWithParsedSchemas = data.map(report => ({
            ...report,
            schema: report.BaseQuerySchemaJSON ? JSON.parse(report.BaseQuerySchemaJSON) : []
          }));
          setReports(reportsWithParsedSchemas);
          console.log("ViewAllReports: Real report definitions loaded.");
        } else {
          console.error("ViewAllReports: Error fetching definitions - Expected JSON", contentType, responseText.substring(0,500));
          throw new Error(`Expected JSON response for definitions, but received '${contentType || 'unknown content type'}'.`);
        }
      } catch (err) {
        console.error("ViewAllReports: Error fetching report definitions:", err);
        setError(`Failed to load report definitions: ${err.message}`);
      } finally {
        setIsLoading(false);
      }
    };
    fetchReportDefinitions();
  }, [extensionSDK]); // Rerun if extensionSDK instance changes (NGROK_BASE_URL is constant within component lifecycle)

  const openFilterModal = (report) => {
    if (!report.schema || !Array.isArray(report.schema) || report.schema.length === 0) {
        alert(`Schema is missing or empty for report: ${report.ReportName}. Cannot apply filters.`);
        return;
    }
    setSelectedReportForModal(report);
    setIsModalOpen(true);
  };

  const closeFilterModal = () => {
    setIsModalOpen(false);
    setSelectedReportForModal(null);
  };

  const handleFiltersAppliedAndExecute = async (filterCriteriaFromModal) => {
    if (!selectedReportForModal) {
      alert("Error: No report selected for execution.");
      return;
    }

    console.log(`ViewAllReports: Filters received for ${selectedReportForModal.ReportName}:`, filterCriteriaFromModal);
    setIsExecutingReport(true);

    const executionPayload = {
        report_definition_name: selectedReportForModal.ReportName,
        filter_criteria_json: JSON.stringify(filterCriteriaFromModal, null, 2)
    };

    console.log("ViewAllReports: Final payload for POST /execute_report:", executionPayload);
    const fastapiExecuteUrl = `${NGROK_BASE_URL}/execute_report`;

    try {
      const response = await fetch(fastapiExecuteUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'ngrok-skip-browser-warning': 'true'
        },
        body: JSON.stringify(executionPayload),
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error("Error executing report from backend:", response.status, response.statusText, errorText);
        alert(`Error executing report: ${response.status} ${response.statusText}. \nServer said: ${errorText.substring(0, 300)}...`);
        // No return here, allow finally to run
      } else {
        const responseData = await response.json(); // Expecting JSON like {"report_url_path": "/view_generated_report/some-id"}
        if (responseData && responseData.report_url_path) {
          const fullReportUrl = NGROK_BASE_URL + responseData.report_url_path;
          console.log("ViewAllReports: Attempting to open report URL with extensionSDK.openBrowserWindow:", fullReportUrl);

          if (extensionSDK && extensionSDK.openBrowserWindow) {
            try {
              extensionSDK.openBrowserWindow(fullReportUrl, '_blank');
            } catch (sdkError) {
              console.error("Error using extensionSDK.openBrowserWindow with URL:", sdkError);
              alert(`Failed to open report window using SDK: ${sdkError.message}. Check console.`);
            }
          } else {
            console.warn("ViewAllReports: ExtensionSDK or openBrowserWindow not available. Cannot open report URL.");
            alert("Looker SDK is not available to open the report window.");
          }
        } else {
          console.error("ViewAllReports: Invalid response from /execute_report. Expected 'report_url_path'. Got:", responseData);
          alert("Failed to get report URL from backend. Response was not in the expected format.");
        }
      }
    } catch (error) {
      console.error("Network or other error executing report:", error);
      alert(`Failed to execute report: ${error.message}`);
    } finally {
      setIsExecutingReport(false);
      closeFilterModal();
    }
  };

  if (isLoading) {
    return <Box p="large" display="flex" justifyContent="center"><Spinner /></Box>;
  }
  if (error) {
    return <Box p="large"><Text color="critical">Error: {error}</Text></Box>;
  }

  return (
    <Box p="large" width="100%">
      <Heading as="h1" mb="xlarge" fontWeight="semiBold">
        Available Report Definitions
      </Heading>
      {reports.length === 0 ? (
        <Text>No report definitions found.</Text>
      ) : (
        <List width="100%" mb="large">
          {reports.map((report) => (
            <ListItem
              key={report.ReportName}
              description={`Schema Fields: ${report.schema?.length || 0}. Last Gen: ${report.LastGeneratedTimestamp ? new Date(report.LastGeneratedTimestamp).toLocaleDateString() : 'N/A'}`}
              onClick={() => openFilterModal(report)}
              itemRole="button"
              style={{ cursor: 'pointer' }}
              disabled={isExecutingReport}
            >
              <Space between alignItems="center" width="100%">
                <Text fontSize="medium" fontWeight="medium">{report.ReportName}</Text>
                <Icon icon={<FilterList />} size="small" color="text2" />
              </Space>
            </ListItem>
          ))}
        </List>
      )}

      {isModalOpen && selectedReportForModal && (
        <Dialog isOpen={isModalOpen} onClose={closeFilterModal} maxWidth="80vw" width="800px">
          <DialogLayout
            header={
              <Box display="flex" justifyContent="space-between" alignItems="center" width="100%">
                <Heading as="h3">Apply Filters: {selectedReportForModal.ReportName}</Heading>
                <IconButton
                  icon={<Close />}
                  label="Close Filters"
                  onClick={closeFilterModal}
                  size="small"
                  disabled={isExecutingReport}
                />
              </Box>
            }
          >
            <DynamicFilterUI
              reportDefinitionName={selectedReportForModal.ReportName}
              schema={selectedReportForModal.schema || []}
              onApplyAndClose={handleFiltersAppliedAndExecute}
              isExecuting={isExecutingReport}
            />
          </DialogLayout>
        </Dialog>
      )}
    </Box>
  );
}

export default ViewAllReports;
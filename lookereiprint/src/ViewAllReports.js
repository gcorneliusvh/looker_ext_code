// src/ViewAllReports.js
import React, { useState, useEffect, useMemo, useContext } from 'react';
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
    InputText,
    FieldSelect,
    Flex,
    FlexItem,
    Button,
    Paragraph,
} from '@looker/components';
import { FilterList, Close, Edit, Delete, Restore, Build, Link, Code } from '@styled-icons/material';
import DynamicFilterUI from './DynamicFilterUI';
import RefineReportDialog from './RefineReportDialog';
import RevertDialog from './RevertDialog';
import PlaceholderMappingDialog from './PlaceholderMappingDialog';

function ViewAllReports({ onEditReport, onHtmlEdit }) {
  const [reports, setReports] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState('');
  const [isExecutingReport, setIsExecutingReport] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [isDiscovering, setIsDiscovering] = useState(false);

  const [isConfirmOpen, setIsConfirmOpen] = useState(false);
  const [reportToDelete, setReportToDelete] = useState(null);
  const [isFilterModalOpen, setIsFilterModalOpen] = useState(false);
  const [selectedReportForModal, setSelectedReportForModal] = useState(null);
  const [isRefineModalOpen, setIsRefineModalOpen] = useState(false);
  const [reportNameToRefine, setReportNameToRefine] = useState('');
  const [isRevertModalOpen, setIsRevertModalOpen] = useState(false);
  const [reportToRevert, setReportToRevert] = useState(null);
  const [isMappingModalOpen, setIsMappingModalOpen] = useState(false);
  const [reportToMap, setReportToMap] = useState(null);
  const [discoveredPlaceholders, setDiscoveredPlaceholders] = useState([]);

  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ type: 'alphabetical', direction: 'asc' });
  const { extensionSDK } = useContext(ExtensionContext);
  const BACKEND_BASE_URL = 'https://looker-ext-code-17837811141.us-central1.run.app';

  const fetchReportDefinitions = async () => {
    if (!extensionSDK) {
      setIsLoading(false);
      setError("Extension SDK not available.");
      return;
    }
    setIsLoading(true);
    setError('');
    try {
      const response = await extensionSDK.fetchProxy(`${BACKEND_BASE_URL}/report_definitions`, { method: 'GET', headers: { 'Accept': 'application/json' } });
      const data = response.body;
      if (!response.ok) {
        throw new Error(data?.detail || `HTTP error ${response.status}`);
      }
      const reportsWithParsedData = data.map(report => {
        try {
            const dataTables = JSON.parse(report.SQL || '[]');
            const schemaForFilter = JSON.parse(report.BaseQuerySchemaJSON || '{}');
            let combinedSchema = [];
            if (typeof schemaForFilter === 'object' && schemaForFilter !== null) {
                Object.values(schemaForFilter).forEach(tableSchema => {
                    if (Array.isArray(tableSchema)) combinedSchema.push(...tableSchema);
                });
                const uniqueFields = new Map();
                combinedSchema.forEach(field => {
                    if (!uniqueFields.has(field.name)) uniqueFields.set(field.name, field);
                });
                combinedSchema = Array.from(uniqueFields.values());
            }
            return {
                ReportName: report.ReportName, Prompt: report.Prompt, ScreenshotURL: report.ScreenshotURL,
                TemplateURL: report.TemplateURL, LatestTemplateVersion: report.LatestTemplateVersion,
                LastGeneratedTimestamp: report.LastGeneratedTimestamp, DataTables: dataTables,
                LookConfigs: JSON.parse(report.LookConfigsJSON || '[]'),
                FilterConfigs: JSON.parse(report.FilterConfigsJSON || '[]'),
                UserAttributeMappings: JSON.parse(report.UserAttributeMappingsJSON || '{}'),
                schemaForFilterUI: combinedSchema,
            };
        } catch (e) {
            console.error(`Failed to parse JSON for report ${report.ReportName}:`, e);
            return null;
        }
      }).filter(Boolean);
      setReports(reportsWithParsedData);
    } catch (err) {
      setError(`Failed to load report definitions: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchReportDefinitions();
  }, [extensionSDK]);

  const handleCancelDelete = () => { setIsConfirmOpen(false); setReportToDelete(null); };
  const handleDeleteReport = (reportName) => { setReportToDelete(reportName); setIsConfirmOpen(true); };
  const handleConfirmDelete = async () => {
    if (!reportToDelete) return;
    setIsDeleting(true);
    setIsConfirmOpen(false);
    try {
        const response = await extensionSDK.fetchProxy(`${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(reportToDelete)}`, { method: 'DELETE' });
        if (!response.ok) { throw new Error(response.body?.detail || 'Unknown error'); }
        alert(`Report '${reportToDelete}' deleted.`);
        fetchReportDefinitions();
    } catch (err) {
        alert(`Failed to delete report: ${err.message}`);
    } finally {
        setIsDeleting(false);
        setReportToDelete(null);
    }
  };

  const openFilterModal = (report) => {
    if (!report.schemaForFilterUI || report.schemaForFilterUI.length === 0) { alert(`Schema is missing or empty for report: ${report.ReportName}.`); return; }
    setSelectedReportForModal(report);
    setIsFilterModalOpen(true);
  };
  const closeFilterModal = () => { setIsFilterModalOpen(false); setSelectedReportForModal(null); };
  const openRefineModal = (reportName) => { setReportNameToRefine(reportName); setIsRefineModalOpen(true); };
  const closeRefineModal = () => { setIsRefineModalOpen(false); setReportNameToRefine(''); fetchReportDefinitions(); };
  const openRevertModal = (report) => { setReportToRevert(report); setIsRevertModalOpen(true); };
  const closeRevertModal = () => { setIsRevertModalOpen(false); setReportToRevert(null); };

  const handleFiltersAppliedAndExecute = async (filterCriteriaFromModal) => {
    if (!selectedReportForModal) { alert("Error: No report selected."); return; }
    setIsExecutingReport(true);
    const payload = { report_definition_name: selectedReportForModal.ReportName, filter_criteria_json: JSON.stringify(filterCriteriaFromModal) };
    try {
      const response = await extensionSDK.fetchProxy(`${BACKEND_BASE_URL}/execute_report`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const responseData = response.body;
      if (!response.ok) { throw new Error(responseData?.detail || 'Execute report failed.'); }
      if (responseData?.report_url_path) {
        extensionSDK.openBrowserWindow(BACKEND_BASE_URL + responseData.report_url_path, '_blank');
      } else { throw new Error("Malformed response from server."); }
    } catch (error) {
      alert(`Failed to execute report: ${error.message}`);
    } finally {
      setIsExecutingReport(false);
      closeFilterModal();
    }
  };
  
  const handleOpenMappingModal = async (report) => {
    setIsDiscovering(true);
    setReportToMap(report);
    try {
        const response = await extensionSDK.fetchProxy(`${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(report.ReportName)}/discover_placeholders`, { method: 'GET' });
        if (!response.ok) throw new Error(response.body?.detail || `Error ${response.status}`);
        setDiscoveredPlaceholders(response.body?.placeholders || []);
        setIsMappingModalOpen(true);
    } catch (err) {
        alert(`Failed to discover placeholders: ${err.message}`);
        setReportToMap(null);
    } finally {
        setIsDiscovering(false);
    }
  };

  const handleApplyMappings = async (reportName, mappings) => {
    setIsMappingModalOpen(false);
    setIsLoading(true);
    try {
        const payload = { report_name: reportName, mappings: mappings };
        const response = await extensionSDK.fetchProxy(`${BACKEND_BASE_URL}/report_definitions/${encodeURIComponent(reportName)}/finalize_template`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        if (!response.ok) throw new Error(response.body?.detail || `Error ${response.status}`);
        alert('Template finalized successfully!');
        fetchReportDefinitions();
    } catch (err) {
        alert(`Failed to finalize template: ${err.message}`);
    } finally {
        setIsLoading(false);
        setReportToMap(null);
        setDiscoveredPlaceholders([]);
    }
  };
  const handleCloseMappingModal = () => { setIsMappingModalOpen(false); setReportToMap(null); setDiscoveredPlaceholders([]); };

  const processedReports = useMemo(() => {
    let displayReports = [...reports];
    if (sortConfig.type === 'alphabetical') {
      displayReports.sort((a, b) => a.ReportName.toLowerCase().localeCompare(b.ReportName.toLowerCase()) * (sortConfig.direction === 'asc' ? 1 : -1));
    } else if (sortConfig.type === 'date') {
      displayReports.sort((a, b) => (new Date(a.LastGeneratedTimestamp || 0).getTime() - new Date(b.LastGeneratedTimestamp || 0).getTime()) * (sortConfig.direction === 'asc' ? 1 : -1));
    }
    if (searchTerm.trim() !== '') {
      displayReports = displayReports.filter(report => report.ReportName.toLowerCase().includes(searchTerm.toLowerCase()));
    }
    return displayReports;
  }, [reports, searchTerm, sortConfig]);

  if (isLoading) { return <Box p="large" display="flex" justifyContent="center"><Spinner /></Box>; }
  if (error) { return <Box p="large"><Text color="critical">Error: {error}</Text></Box>; }

  return (
    <Box p="large" width="100%">
      <Dialog isOpen={isConfirmOpen} onClose={handleCancelDelete}>
        <DialogLayout
          header={<Heading as="h3" p="medium" borderBottom="ui1">Confirm Deletion</Heading>}
          footer={<Space between p="medium" borderTop="ui1"> <Button onClick={handleCancelDelete}>Cancel</Button> <Button color="critical" onClick={handleConfirmDelete}>Delete</Button> </Space>}
        >
          <Box p="large"> <Paragraph> Are you sure you want to permanently delete the report "{reportToDelete}" and all its associated template versions? </Paragraph> </Box>
        </DialogLayout>
      </Dialog>
      
      {isRevertModalOpen && reportToRevert && (
          <RevertDialog
            isOpen={isRevertModalOpen}
            onClose={closeRevertModal}
            reportName={reportToRevert.ReportName}
            currentVersion={reportToRevert.LatestTemplateVersion}
            onRevertSuccess={() => { closeRevertModal(); fetchReportDefinitions(); }}
          />
      )}
      
      {isFilterModalOpen && selectedReportForModal && (
        <Dialog isOpen={isFilterModalOpen} onClose={closeFilterModal} maxWidth="80vw" width="800px">
          <DialogLayout
            header={
              <Box display="flex" justifyContent="space-between" alignItems="center" width="100%" p="medium" borderBottom="ui1">
                <Heading as="h3">Apply Filters: {selectedReportForModal.ReportName}</Heading>
                <IconButton icon={<Close />} label="Close Filters" onClick={closeFilterModal} size="small" disabled={isExecutingReport}/>
              </Box>
            }
          >
            <DynamicFilterUI
              reportDefinitionName={selectedReportForModal.ReportName}
              schema={selectedReportForModal.schemaForFilterUI || []}
              onApplyAndClose={handleFiltersAppliedAndExecute}
            />
          </DialogLayout>
        </Dialog>
      )}

      {isRefineModalOpen && reportNameToRefine && (
        <RefineReportDialog
            isOpen={isRefineModalOpen}
            onClose={closeRefineModal}
            reportName={reportNameToRefine}
        />
      )}
      
      {isMappingModalOpen && reportToMap && (
        <PlaceholderMappingDialog
            isOpen={isMappingModalOpen}
            onClose={handleCloseMappingModal}
            reportName={reportToMap.ReportName}
            discoveredPlaceholders={discoveredPlaceholders}
            schema={reportToMap.schemaForFilterUI}
            onApplyMappings={handleApplyMappings}
            lookConfigs={reportToMap.LookConfigs}
            filterConfigs={reportToMap.FilterConfigs}
        />
      )}

      <Heading as="h1" mb="xlarge" fontWeight="semiBold"> Available Report Definitions </Heading>
      <Flex mb="large" justifyContent="space-between" alignItems="flex-end" flexWrap="wrap">
        <FlexItem flexBasis="50%" minWidth="250px" mb={{xs: "small", md: "none"}}>
          <InputText placeholder="Search reports by name..." value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} aria-label="Search reports" />
        </FlexItem>
        <Flex alignItems="center">
          <Text fontSize="small" color="text2" mr="xsmall" whiteSpace="nowrap">Sort by:</Text>
          <FieldSelect value={sortConfig.type} options={[{ value: 'alphabetical', label: 'Name' }, { value: 'date', label: 'Last Modified' }]} onChange={(value) => setSortConfig(prev => ({ ...prev, type: value }))} mr="small" width="130px" aria-label="Sort type" />
          <FieldSelect value={sortConfig.direction} options={[{ value: 'asc', label: 'Asc' }, { value: 'desc', label: 'Desc' }]} onChange={(value) => setSortConfig(prev => ({ ...prev, direction: value }))} width="90px" aria-label="Sort direction" />
        </Flex>
      </Flex>

      {processedReports.length === 0 ? (
        <Text mt="large">{searchTerm ? 'No reports match your search.' : 'No report definitions found.'}</Text>
      ) : (
        <List width="100%" mb="large">
          {processedReports.map((report) => (
            <ListItem
              key={report.ReportName}
              description={`Tables: ${report.DataTables?.length || 0} | Charts: ${report.LookConfigs?.length || 0} | Version: ${report.LatestTemplateVersion || 'N/A'} | Last Mod: ${report.LastGeneratedTimestamp ? new Date(report.LastGeneratedTimestamp).toLocaleString() : 'N/A'}`}
              onClick={() => openFilterModal(report)}
              itemRole="button"
              disabled={isExecutingReport || isDeleting || isDiscovering}
              px="small" py="medium"
            >
              <Space between alignItems="center" width="100%">
                <Text fontSize="medium" fontWeight="medium" truncate>{report.ReportName}</Text>
                <Space>
                   <IconButton icon={<Delete />} label="Delete Report" size="small" tooltip="Delete Report Definition" onClick={(e) => { e.stopPropagation(); handleDeleteReport(report.ReportName); }} disabled={isExecutingReport || isDeleting} />
                   <IconButton icon={<Build />} label="Edit Definition" size="small" tooltip="Edit Report Definition" onClick={(e) => { e.stopPropagation(); onEditReport(report); }} disabled={isExecutingReport || isDeleting} />
                   <IconButton icon={<Link />} label="Map Placeholders" size="small" tooltip="Map AI-generated placeholders" onClick={(e) => { e.stopPropagation(); handleOpenMappingModal(report); }} disabled={isExecutingReport || isDeleting || isDiscovering} />
                   <IconButton icon={<Code />} label="Edit HTML" size="small" tooltip="Directly edit HTML template" onClick={(e) => { e.stopPropagation(); onHtmlEdit(report); }} disabled={isExecutingReport || isDeleting} />
                   <IconButton icon={<Restore />} label="Revert Template" size="small" tooltip="Revert to a previous template version" onClick={(e) => { e.stopPropagation(); openRevertModal(report); }} disabled={isExecutingReport || isDeleting || (report.LatestTemplateVersion || 0) <= 1} />
                   <IconButton icon={<Edit />} label="Refine AI Template" size="small" tooltip="Refine AI Generated Template" onClick={(e) => { e.stopPropagation(); openRefineModal(report.ReportName); }} disabled={isExecutingReport || isDeleting} />
                   <IconButton icon={<FilterList />} label="Apply Filters & Run" size="small" tooltip="Apply Filters & Run Report" onClick={(e) => { e.stopPropagation(); openFilterModal(report); }} disabled={isExecutingReport || isDeleting} />
                </Space>
              </Space>
            </ListItem>
          ))}
        </List>
      )}
    </Box>
  );
}

export default ViewAllReports;
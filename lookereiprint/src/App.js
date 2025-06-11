// src/App.js
import React, { useState } from 'react';
import { hot } from 'react-hot-loader/root';
import styled, { css } from 'styled-components';
import {
    ComponentsProvider,
    Layout,
    Page,
    NavList,
    ListItem,
    Aside
} from '@looker/components';
import { ExtensionProvider } from '@looker/extension-sdk-react';
import ReportForm from './ReportForm';
import ViewAllReports from './ViewAllReports';
import EditSystemInstructions from './EditSystemInstructions';
import HtmlEditorView from './HtmlEditorView';

// --- Custom Styled Components for Navigation ---

const CustomStyledAside = styled(Aside)`
    background-color: #f7f9fa;
    border-right: 1px solid #e1e4e8;
`;

const StyledListItem = styled(ListItem)`
  display: flex;
  align-items: center;
  white-space: nowrap;
  padding: 0.5rem 1.25rem;

  margin: 0.25rem 0.75rem;
  border-radius: 4px;
  transition: background-color 0.2s ease-in-out, color 0.2s ease-in-out;
  color: #3c4043;
  font-weight: 500;

  ${(props) =>
    !props.selected &&
    css`
      &:hover {
        background-color: #e8eaed;
      }
    `}

  ${(props) =>
    props.selected &&
    css`
      background-color: transparent;
      color: #1976d2;
      font-weight: 700;
    `}
`;

const App = hot(() => {
    const [activeView, setActiveView] = useState('viewAllReports');
    const [reportToEdit, setReportToEdit] = useState(null);
    const [reportForHtmlEdit, setReportForHtmlEdit] = useState(null);

    const handleSelectView = (view) => {
        setReportToEdit(null);
        setReportForHtmlEdit(null);
        setActiveView(view);
    };

    const handleEditReport = (reportData) => {
        setReportToEdit(reportData);
        setActiveView('defineReport');
    };

    const handleHtmlEdit = (reportData) => {
        setReportForHtmlEdit(reportData);
        setActiveView('htmlEditor');
    };

    const renderActiveView = () => {
        switch (activeView) {
            case 'defineReport':
                return <ReportForm reportToEdit={reportToEdit} onComplete={() => handleSelectView('viewAllReports')} />;
            case 'editSystemInstructions':
                return <EditSystemInstructions />;
            case 'htmlEditor':
                return <HtmlEditorView report={reportForHtmlEdit} onComplete={() => handleSelectView('viewAllReports')} />;
            case 'viewAllReports':
            default:
                return <ViewAllReports onEditReport={handleEditReport} onHtmlEdit={handleHtmlEdit} />;
        }
    };

    // --- UPDATED: Label logic no longer changes for 'htmlEditor' ---
    let defineReportLabel = 'Add a Report';
    if (activeView === 'defineReport' && reportToEdit) {
        defineReportLabel = 'Edit Definition';
    }
    // The 'else if' condition for htmlEditor has been removed.

    return (
        <ExtensionProvider>
            <ComponentsProvider>
                <Page fixed>
                    <Layout hasAside>
                        <CustomStyledAside width="240px">
                            <NavList>
                                {/* --- UPDATED: This item now stays selected during HTML editing --- */}
                                <StyledListItem
                                    selected={activeView === 'viewAllReports' || activeView === 'htmlEditor'}
                                    onClick={() => handleSelectView('viewAllReports')}
                                >
                                    View/Edit All Reports
                                </StyledListItem>

                                {/* --- UPDATED: This item is no longer selected during HTML editing --- */}
                                <StyledListItem
                                    selected={activeView === 'defineReport'}
                                    onClick={() => handleSelectView('defineReport')}
                                >
                                    {defineReportLabel}
                                </StyledListItem>

                                <StyledListItem
                                    selected={activeView === 'editSystemInstructions'}
                                    onClick={() => handleSelectView('editSystemInstructions')}
                                >
                                    Edit System Instructions
                                </StyledListItem>
                            </NavList>
                        </CustomStyledAside>
                        {renderActiveView()}
                    </Layout>
                </Page>
            </ComponentsProvider>
        </ExtensionProvider>
    );
});

export { App };
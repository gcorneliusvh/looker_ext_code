// src/App.js
import React, { useState } from 'react';
import { hot } from 'react-hot-loader/root';
import { ComponentsProvider, Layout, Page, NavList, ListItem } from '@looker/components';
import { ExtensionProvider } from '@looker/extension-sdk-react';
import ReportForm from './ReportForm';
import ViewAllReports from './ViewAllReports'; // CORRECTED: Using the original file name
import EditSystemInstructions from './EditSystemInstructions';
import HtmlEditorView from './HtmlEditorView';

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
                // CORRECTED: Using the original component name
                return <ViewAllReports onEditReport={handleEditReport} onHtmlEdit={handleHtmlEdit} />;
        }
    };

    let defineReportLabel = 'Add New Report';
    if (activeView === 'defineReport' && reportToEdit) {
        defineReportLabel = 'Edit Definition';
    } else if (activeView === 'htmlEditor') {
        defineReportLabel = 'HTML Editor';
    }

    return (
        <ExtensionProvider>
            <ComponentsProvider>
                <Page fixed>
                    <Layout hasAside>
                        <NavList>
                            <ListItem
                                icon="Dashboard"
                                selected={activeView === 'viewAllReports'}
                                onClick={() => handleSelectView('viewAllReports')}
                            >
                                View & Run Reports
                            </ListItem>
                            <ListItem
                                icon={activeView === 'htmlEditor' ? "Code" : "Add"}
                                selected={activeView === 'defineReport' || activeView === 'htmlEditor'}
                                onClick={() => handleSelectView('defineReport')}
                            >
                                {defineReportLabel}
                            </ListItem>
                            <ListItem
                                icon="Settings"
                                selected={activeView === 'editSystemInstructions'}
                                onClick={() => handleSelectView('editSystemInstructions')}
                            >
                                System Instructions
                            </ListItem>
                        </NavList>
                        {renderActiveView()}
                    </Layout>
                </Page>
            </ComponentsProvider>
        </ExtensionProvider>
    );
});

export { App };
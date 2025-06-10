// src/App.js
import React, { useState } from 'react';
import { hot } from 'react-hot-loader/root';
import { ComponentsProvider, Layout, Page, NavList, ListItem } from '@looker/components';
import { ExtensionProvider } from '@looker/extension-sdk-react';
import ReportForm from './ReportForm';
import ViewAllReports from './ViewAllReports';
import EditSystemInstructions from './EditSystemInstructions';

const App = hot(() => {
    const [activeView, setActiveView] = useState('viewAllReports');
    const [reportToEdit, setReportToEdit] = useState(null);

    const handleSelectView = (view) => {
        // When switching back to the form, if it's not via an "edit" click,
        // ensure we are in "create new" mode by clearing the reportToEdit state.
        if (view === 'defineReport' && reportToEdit) {
           // Keep reportToEdit state if we are already on the define page
        } else {
            setReportToEdit(null);
        }
        setActiveView(view);
    };

    const handleEditReport = (reportData) => {
        setReportToEdit(reportData);
        setActiveView('defineReport');
    };

    const renderActiveView = () => {
        switch (activeView) {
            case 'defineReport':
                return <ReportForm reportToEdit={reportToEdit} onComplete={() => handleSelectView('viewAllReports')} />;
            case 'editSystemInstructions':
                return <EditSystemInstructions />;
            case 'viewAllReports':
            default:
                return <ViewAllReports onEditReport={handleEditReport} />;
        }
    };

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
                                icon="Add"
                                selected={activeView === 'defineReport'}
                                onClick={() => handleSelectView('defineReport')}
                            >
                                {reportToEdit ? 'Edit Report Definition' : 'Add New Report'}
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
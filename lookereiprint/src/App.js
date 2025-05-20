// src/App.js
import React, { useState } from 'react';
import { ExtensionProvider } from '@looker/extension-sdk-react';
import { 
    ComponentsProvider, 
    Page,        
    Layout,
    Aside,        
    Section,      
    Heading,      // For content placeholders
    NavList,      // This was correct
    ListItem      // <-- CORRECTED: Import ListItem instead of NavItem
    // You might later import icons from '@looker/icons' e.g. import { Home } from '@looker/icons'
} from '@looker/components';
import { hot } from 'react-hot-loader/root';

import ReportForm from './ReportForm';
import ViewAllReports from './ViewAllReports'; // Make sure this is a valid component
import EditSystemInstructions from './EditSystemInstructions'; // Make sure this is a valid component

const VIEWS = {
  ADD_NEW_REPORT: 'addNewReport',
  VIEW_ALL_REPORTS: 'viewAllReports',
  EDIT_SYSTEM_INSTRUCTIONS: 'editSystemInstructions',
};

export const App = hot(() => {
  const [activeView, setActiveView] = useState(VIEWS.ADD_NEW_REPORT);
  console.log("App.js rendering with NavList and ListItem. Active view:", activeView);

  const renderView = () => {
    switch (activeView) {
      case VIEWS.ADD_NEW_REPORT:
        return <ReportForm />;
      case VIEWS.VIEW_ALL_REPORTS:
        return <ViewAllReports />;
      case VIEWS.EDIT_SYSTEM_INSTRUCTIONS:
        return <EditSystemInstructions />;
      default:
        return <ReportForm />;
    }
  };

  return (
    <ExtensionProvider>
      <ComponentsProvider>
        <Page fixed>
          <Layout hasAside>
            <Aside width="220px" borderRight p="small" paddingTop="large">
              <NavList>
                <ListItem  // <-- CORRECTED to ListItem
                  selected={activeView === VIEWS.ADD_NEW_REPORT}
                  onClick={() => setActiveView(VIEWS.ADD_NEW_REPORT)}
                  icon={<span>📄</span>} // Placeholder icon, replace with Looker icon later
                  // Common ListItem props: itemRole="link", description, detail
                >
                  Add New Report
                </ListItem>
                <ListItem // <-- CORRECTED to ListItem
                  selected={activeView === VIEWS.VIEW_ALL_REPORTS}
                  onClick={() => setActiveView(VIEWS.VIEW_ALL_REPORTS)}
                  icon={<span>📊</span>} // Placeholder icon
                >
                  View All Reports
                </ListItem>
                <ListItem // <-- CORRECTED to ListItem
                  selected={activeView === VIEWS.EDIT_SYSTEM_INSTRUCTIONS}
                  onClick={() => setActiveView(VIEWS.EDIT_SYSTEM_INSTRUCTIONS)}
                  icon={<span>⚙️</span>} // Placeholder icon
                >
                  Edit System Instructions
                </ListItem>
              </NavList>
            </Aside>

            <Section as="main" p="large">
              {renderView()}
            </Section>
          </Layout>
        </Page>
      </ComponentsProvider>
    </ExtensionProvider>
  );
});
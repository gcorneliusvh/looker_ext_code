# `looker_ext_code`

This repository houses the core code for a Looker extension, enabling enhanced reporting capabilities by leveraging a Generative AI (GenAI) API. It consists of a Node.js frontend (the Looker Extension itself) and a Python-based GenAI API backend deployed on Google Cloud Run.

## Table of Contents

* [Project Overview](#project-overview)
* [Repository Structure](#repository-structure)
* [Deployment](#deployment)
    * [GenAI Report API (Backend)](#genai-report-api-backend)
    * [Looker Extension Frontend](#looker-extension-frontend)
    * [Looker Extension Manifest](#looker-extension-manifest)
* [Configuration (Environment Variables)](#configuration-environment-variables)
* [Getting Started (Development)](#getting-started-development)
* [Contributing](#contributing)
* [License](#license)

## Project Overview

This project provides a Looker extension designed to integrate advanced Generative AI functionalities directly within the Looker platform. Users can interact with the extension to process and generate reports or insights, with the heavy lifting performed by an external GenAI API.

## Repository Structure

The repository is organized into the following key directories:

* `genai_report_api/`: Contains all the Docker-related code and source for the Python-based Generative AI API backend. This service is responsible for handling AI processing requests.
* `lookereiprint/`: Contains the Node.js frontend code for the Looker extension. This is the UI that users interact with inside Looker.
* `.gitignore`: Specifies intentionally untracked files that Git should ignore.
* `README.md`: This file.

## Deployment

This project involves deploying two primary components and managing a separate manifest file for the Looker extension.

### GenAI Report API (Backend)

The GenAI Report API is a Dockerized Python application deployed to **Google Cloud Run**.

**Deployment Steps:**

1.  **Navigate to the API directory:**
    ```bash
    cd genai_report_api
    ```
2.  **Build the Docker image:**
    Ensure you are authenticated to Google Cloud. Replace `[YOUR_GCP_PROJECT_ID]` with your actual GCP Project ID (e.g., from your environment variables).
    ```bash
    gcloud builds submit --tag gcr.io/[YOUR_GCP_PROJECT_ID]/genai-report-api
    ```
3.  **Deploy to Cloud Run:**
    ```bash
    gcloud run deploy genai-report-api \
      --image gcr.io/[YOUR_GCP_PROJECT_ID]/genai-report-api \
      --platform managed \
      --region [YOUR_GCP_LOCATION] \
      --allow-unauthenticated \
      --port 8080 \
      --env-vars-file=./env.yaml # Assuming you create this file from your environment variables
    ```
    * `--allow-unauthenticated`: Adjust this based on your security requirements. You may want to configure authentication (e.g., IAP) for production.
    * `--port 8080`: Ensure this matches the port your Dockerized Python application listens on.
    * `--env-vars-file=./env.yaml`: This is a recommended way to pass many environment variables to Cloud Run. Create a `env.yaml` file in your `genai_report_api` directory (or specify its path) with the contents from the `Configuration` section below.

    After deployment, Cloud Run will provide a service URL (e.g., `https://looker-ext-code-[SERVICE_HASH].[REGION].run.app`). This URL will be configured in the Looker Extension frontend and its manifest.

### Looker Extension Frontend

The Looker Extension frontend is a Node.js application. It is compiled into a static `bundle.js` file and hosted on Google Cloud Storage.

**Deployment Steps:**

1.  **Navigate to the frontend directory:**
    ```bash
    cd lookereiprint/
    ```
2.  **Install dependencies:**
    ```bash
    npm install
    ```
3.  **Build the production bundle:**
    This command will compile and bundle your Node.js application for deployment. The output is typically `bundle.js`.
    ```bash
    npm run build # Or `yarn build` if using Yarn
    ```
4.  **Host the static assets on Google Cloud Storage:**
    The generated `bundle.js` needs to be uploaded to a GCS bucket, as referenced in your `manifest.lkml`. Ensure the bucket and objects are publicly readable or configured for access.
    ```bash
    gsutil cp dist/bundle.js gs://[YOUR_GCS_BUCKET_NAME]/looker_frontend/bundle.js
    # Ensure public readability if not already configured
    gsutil acl ch -u AllUsers:R gs://[YOUR_GCS_BUCKET_NAME]/looker_frontend/bundle.js
    ```
    The `[YOUR_GCS_BUCKET_NAME]` will be your `GCS_BUCKET_NAME` environment variable value.

### Looker Extension Manifest

The Looker Extension's manifest file (`manifest.lkml`) is crucial for telling Looker about your extension. **This file is located in a separate GitHub repository** (e.g., your LookML project repository) and is *not* part of this `looker_ext_code` repository.

**Example `manifest.lkml` (Anonymized):**

This example shows the structure and key configuration points for your manifest. The `url` should point to your hosted frontend bundle, and `external_api_urls` must include your Cloud Run service URL and any other external endpoints accessed by your extension.

```lookml
# Example snippet from your manifest.lkml in the separate GitHub repo
# projects/[YOUR_LOOKML_PROJECT]/manifest.lkml

application: lookereiprint {
  label: "lookereiprint"
  url: "[https://storage.googleapis.com/report_screenshots_genaimarketingdemo/looker_frontend/bundle.js](https://storage.googleapis.com/report_screenshots_genaimarketingdemo/looker_frontend/bundle.js)"
  # Optional: For local development, you might temporarily use a local URL like:
  # url: "[https://kb-bean-caroline-barn.trycloudflare.com/bundle.js](https://kb-bean-caroline-barn.trycloudflare.com/bundle.js)"

  entitlements: {
    local_storage: yes
    navigation: yes
    new_window: yes
    new_window_external_urls: [
      "https://[YOUR_CLOUD_RUN_SERVICE_HOST]/*", # e.g., [https://looker-ext-code-17837811141.us-central1.run.app/](https://looker-ext-code-17837811141.us-central1.run.app/)*
      "data:*"
    ]
    use_form_submit: yes
    use_embeds: yes
    core_api_methods: [
      "all_lookml_models",
      "lookml_model_explore",
      "all_user_attributes",
      "all_connections",
      "search_folders",
      "run_inline_query",
      "me",
      "current_user",
      "all_looks",
      "run_look"
    ]
    external_api_urls : [
      "https://[YOUR_LOOKER_EXTENSION_SANDBOX_HOST]", # e.g., [https://83d14716-08b3-4def-bdca-54f4b2af9f19-extensions.cloud.looker.com](https://83d14716-08b3-4def-bdca-54f4b2af9f19-extensions.cloud.looker.com)
      "https://[YOUR_CLOUD_RUN_SERVICE_HOST]", # e.g., [https://looker-ext-code-17837811141.us-central1.run.app](https://looker-ext-code-17837811141.us-central1.run.app)
      "http://localhost:8080",
      "https://localhost:8080",
      "http://localhost:8001",
      "https://localhost:8001",
      "[https://kb-bean-caroline-barn.trycloudflare.com](https://kb-bean-caroline-barn.trycloudflare.com)", # For local dev via Cloudflare Tunnel
      "[https://cdn.tiny.cloud](https://cdn.tiny.cloud)" # If using TinyMCE or similar
    ]
    global_user_attributes: ["client_id"] # Example of a global user attribute
  }
}

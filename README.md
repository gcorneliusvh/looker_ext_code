# `looker_ext_code`

**⚠️ IMPORTANT DISCLAIMER: This project is currently in active development and has not yet undergone proper security validations. Use the code and deploy it at your own risk. It is not intended for production environments without thorough security review and hardening. ⚠️**

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
```

**Update and Deploy:** After updating the `manifest.lkml` file in its dedicated GitHub repository, ensure you deploy the LookML project to your Looker instance so the changes take effect.

## Configuration (Environment Variables)

The following environment variables are used for configuring the application, particularly for the Cloud Run deployment of the GenAI API backend and potentially for the frontend build process.

When deploying to Cloud Run, these are passed as environment variables. You can create a `env.yaml` file for `gcloud run deploy --env-vars-file=./env.yaml` with the format:

```yaml
# env.yaml
# Place this file in the genai_report_api directory for Cloud Run deployment
- name: FRONTEND_NGROK_URL
  value: "https://[ANONYMIZED_FRONTEND_NGROK_URL]"
- name: GCP_PROJECT_ID
  value: "genaimarketingdemo"
- name: GCP_LOCATION
  value: "us-central1"
- name: GCS_BUCKET_NAME
  value: "report_html_templates_genaimarketingdemo"
- name: LOOKERSDK_CLIENT_ID
  value: "[ANONYMIZED_LOOKERSDK_CLIENT_ID]"
- name: LOOKERSDK_CLIENT_SECRET
  value: "[ANONYMIZED_LOOKERSDK_CLIENT_SECRET]"
- name: LOOKER_INSTANCE_URL
  value: "[https://igmprinting.cloud.looker.com](https://igmprinting.cloud.looker.com)"
- name: LOOKER_EXTENSION_SANDBOX_HOST
  value: "https://[ANONYMIZED_LOOKER_EXTENSION_SANDBOX_HOST]"
- name: LOOKERSDK_BASE_URL
  value: "[https://igmprinting.cloud.looker.com:19999](https://igmprinting.cloud.looker.com:19999)"
- name: TINYMCF_API_KEY
  value: "[ANONYMIZED_TINYMCF_API_KEY]"
```
  ## Explanation of variables:

* `FRONTEND_NGROK_URL`: Likely used for local development/testing of the frontend, possibly with a tunneling service like ngrok or Cloudflare Tunnel to expose localhost to Looker.
* `GCP_PROJECT_ID`: Your Google Cloud Platform project ID.
* `GCP_LOCATION`: The GCP region where your Cloud Run service is deployed.
* `GCS_BUCKET_NAME`: The name of the Google Cloud Storage bucket used for hosting frontend assets (e.g., bundle.js) and potentially other templates/files.
* `LOOKERSDK_CLIENT_ID`, `LOOKERSDK_CLIENT_SECRET`: Credentials for authenticating with the Looker SDK, likely used by the backend API or for internal Looker interactions.
* `LOOKER_INSTANCE_URL`: The base URL of your Looker instance.
* `LOOKER_EXTENSION_SANDBOX_HOST`: The host URL for the Looker Extension iframe sandbox, crucial for `external_api_urls` entitlement.
* `LOOKERSDK_BASE_URL`: Another Looker SDK base URL, possibly for a specific API version or internal endpoint.
* `TINYMCF_API_KEY`: API key for TinyMCE, suggesting that the extension includes a rich text editor.

## Getting Started (Development)

To set up the project for local development:

1.  **Clone this repository:**
    ```bash
    git clone [https://github.com/your-username/looker_ext_code.git](https://github.com/your-username/looker_ext_code.git)
    cd looker_ext_code
    ```
2.  **Backend (GenAI Report API):**
    * Navigate to `genai_report_api/`.
    * Install Python dependencies (preferably in a virtual environment).
    * Set up environment variables locally (e.g., using a `.env` file and a library like `python-dotenv`).
    * Run the API locally (consult `genai_report_api` for specific instructions).
3.  **Frontend (Looker Extension):**
    * Navigate to `lookereiprint/`.
    * Install Node.js dependencies: `npm install`.
    * Set up environment variables locally if needed by the frontend build (e.g., using a `.env` file).
    * Start the development server (consult `lookereiprint` for specific instructions, typically `npm start`).
    * Configure the frontend to point to your locally running GenAI API or your deployed Cloud Run service.
    * To test locally with your Looker instance, you may need to expose your local frontend development server using a tunneling service (e.g., ngrok or Cloudflare Tunnel) and update your `manifest.lkml` url temporarily.

## Contributing

Contributions are welcome! Please follow standard GitHub flow: fork the repository, create a branch, make your changes, and open a pull request.    
import datetime
import base64
import json
import os
import re
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Union, Optional
import uuid
from enum import Enum

import httpx
import uvicorn
from fastapi import (FastAPI, Depends, HTTPException, Query, Body, BackgroundTasks)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from google.cloud import bigquery, storage
from google.cloud.bigquery import ScalarQueryParameter, ArrayQueryParameter
from google.api_core.exceptions import NotFound as GCSNotFound
from google.api_core import exceptions as google_api_exceptions

import vertexai
from vertexai.generative_models import GenerativeModel, Part, Image
from vertexai.generative_models import HarmCategory, HarmBlockThreshold, GenerationConfig

import looker_sdk
from looker_sdk import methods40, models40

# --- AppConfig & Global Configs ---
class AppConfig:
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcp_location: str = os.getenv("GCP_LOCATION", "")
    default_system_instruction_text: str = ""
    vertex_ai_initialized: bool = False
    bigquery_client: Union[bigquery.Client, None] = None
    storage_client: Union[storage.Client, None] = None
    looker_sdk_client: Union[methods40.Looker40SDK, None] = None
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GCS_SYSTEM_INSTRUCTION_PATH: str = os.getenv("GCS_SYSTEM_INSTRUCTION_PATH", "system_instructions/default_system_instruction.txt")
    TARGET_GEMINI_MODEL: str = "gemini-2.5-pro-preview-05-06"
    GCS_GENERATED_REPORTS_PREFIX: str = "generated_reports_output/"

config = AppConfig()

ALLOWED_FILTER_OPERATORS = {
    "_eq": {"op": "=", "param_type_hint": "AUTO"}, "_ne": {"op": "!=", "param_type_hint": "AUTO"},
    "_gte": {"op": ">=", "param_type_hint": "AUTO_DATE_OR_NUM"}, "_lte": {"op": "<=", "param_type_hint": "AUTO_DATE_OR_NUM"},
    "_gt": {"op": ">", "param_type_hint": "AUTO_DATE_OR_NUM"}, "_lt": {"op": "<", "param_type_hint": "AUTO_DATE_OR_NUM"},
    "_like": {"op": "LIKE", "param_type_hint": "STRING"}, "_like_prefix": {"op": "LIKE", "param_type_hint": "STRING_PREFIX"},
    "_like_suffix": {"op": "LIKE", "param_type_hint": "STRING_SUFFIX"}, "_in": {"op": "IN", "param_type_hint": "STRING_ARRAY"},
    "_is_null": {"op": "IS NULL", "param_type_hint": "NONE"}, "_is_not_null": {"op": "IS NOT NULL", "param_type_hint": "NONE"},
    "_between": {"op": "BETWEEN", "param_type_hint": "AUTO_DATE_OR_NUM_RANGE"},
    "_eq_true": {"op":"=", "param_type_hint": "BOOL_TRUE_STR"}, "_eq_false": {"op":"=", "param_type_hint": "BOOL_FALSE_STR"},
}

DEFAULT_FALLBACK_SYSTEM_INSTRUCTION = """You are an expert HTML and CSS developer. Your sole task is to generate a complete, single-file, well-structured, and print-optimized HTML document that will serve as a report template.

**Primary Goal:**
Create an HTML structure that visually aligns with a provided style-guide image and fulfills the user's main textual prompt for the report's purpose and style. A backend system will inject data into this template using specific placeholders.

**Critical Output Requirement:**
Your entire response MUST be ONLY the raw HTML code.
- Start directly with `<!DOCTYPE html>` or `<html>`.
- End with `</html>`.
- Do NOT include ANY markdown (like ```html or ```), comments outside of standard HTML comments (), or any explanatory text or conversational preamble/postamble. Just the code.

**Placeholder Syntax (VERY IMPORTANT):**
All dynamic data placeholders you create or are instructed to use MUST be enclosed in **double curly braces**. For example: `{{YourPlaceholderKey}}`. Do NOT use single braces, square brackets, or any other format.

**Key Instructions for Template Design:**
1.  **Visual Style:** Prioritize matching the layout and aesthetic of the provided style-guide image.
2.  **Data Schema & Field Instructions:** You will receive:
    * A `Data Schema` (field names, types).
    * `Field Display & Summary Instructions` (how fields are used, styled, and summarized). Adhere to these closely.
    * **Styling:** Apply provided `alignment` and `number_format` hints via CSS or inline styles.
    * **Field Placement & Standard Placeholders:**
        * **Top-of-Report Fields:** If a schema field named `FieldName` is marked `include_at_top`, use the EXACT placeholder format: `{{TOP_FieldName}}`. (e.g., `{{TOP_ClientName}}`)
        * **Header Fields:** If a schema field named `FieldName` is marked `include_in_header`, use the EXACT placeholder format: `{{HEADER_FieldName}}`. (e.g., `{{HEADER_ReportDate}}`)
    * **Main Data Table:** This is for repeating data rows. Use the single, EXACT placeholder `{{TABLE_ROWS_HTML_PLACEHOLDER}}` where all table body rows will be injected by the backend. Design a `<table>` with `<thead>` (for column headers based on fields marked `include_in_body`) and `<tbody>` (which will contain the `{{TABLE_ROWS_HTML_PLACEHOLDER}}`).
    * **Summary Row Structure (Subtotals & Grand Totals):**
        * The backend will dynamically generate and inject complete `<tr>...</tr>` elements for subtotal and grand total rows into the `{{TABLE_ROWS_HTML_PLACEHOLDER}}` area.
        * If `group_summary_action` on string fields and `numeric_aggregation` on numeric fields are provided, understand that these summary rows are backend-generated.
        * **Your Task:** Design the table to accommodate these. You might include an empty `<tfoot>` or define CSS classes (e.g., `.subtotal-row`, `.grand-total-row`) that the backend-generated rows will use for styling. Do NOT create your own placeholders for individual subtotal or grand total *values* within the main table body; the backend handles their full row generation.
3.  **Explicit Calculation Rows (User-Defined Summaries):**
    * If `Explicit Overall Calculation Row Instructions` are provided, they will include a `row_label` and a `values_placeholder_name` (e.g., `MyCustomTotals`).
    * Create a table row (e.g., in `<tfoot>`) that includes the `row_label` directly in an appropriate `<td>` (possibly with `colspan`). For the calculated values, use the EXACT placeholder provided, wrapped in double curly braces: `{{MyCustomTotals}}`. The backend replaces this single placeholder with multiple `<td>` elements.
    * Example: `<tr><td>Overall Averages:</td>{{MyCustomTotals}}</tr>`
4.  **Custom Placeholders (Use Sparingly and with Correct Syntax):**
    * If the user's main prompt clearly implies a need for other dynamic text NOT covered by `TOP_`, `HEADER_`, or schema fields in the main table (e.g., `{{CustomReportTitle}}`), you MAY use such a descriptive placeholder, ensuring it uses **double curly braces**. These will be manually mapped by the user later. Prefer standard conventions where possible.
5.  **Print Optimization:** Ensure the layout is print-friendly.
6.  **Clean Code:** Generate valid, semantic HTML and clean, embedded CSS. Minimize JavaScript to only what's essential for presentation.

**Example of correct placeholder usage:**
Client Name: `{{TOP_ClientName}}`
Report Period: `{{HEADER_PeriodEndDate}}`
Main Table Data: `<tbody>{{TABLE_ROWS_HTML_PLACEHOLDER}}</tbody>`
Custom Calculation Row: `<tr><td>Totals:</td>{{MyOverallTotals}}</tr>`

**Remember: Strict adherence to the "HTML ONLY" output format and the **double curly brace** `{{PlaceholderKey}}` convention for ALL dynamic placeholders is paramount for the backend system to process your generated template correctly.**
"""

# --- Pydantic Models ---
class LookConfig(BaseModel):
    look_id: int
    placeholder_name: str

class CalculationType(str, Enum):
    SUM = "SUM"; AVERAGE = "AVERAGE"; COUNT = "COUNT"; COUNT_DISTINCT = "COUNT_DISTINCT"; MIN = "MIN"; MAX = "MAX"

class CalculatedValueConfig(BaseModel):
    target_field_name: str; calculation_type: CalculationType
    number_format: Optional[str] = None; alignment: Optional[str] = None

class CalculationRowConfig(BaseModel):
    row_label: str; values_placeholder_name: str; calculated_values: List[CalculatedValueConfig]

class SubtotalConfig(BaseModel):
    group_by_field_name: str; values_placeholder_name: str; calculated_values: List[CalculatedValueConfig]

# FieldDisplayConfig MUST be defined before DataTableConfig
class FieldDisplayConfig(BaseModel):
    field_name: str; include_in_body: bool = Field(default=True); include_at_top: bool = Field(default=False)
    include_in_header: bool = Field(default=False); context_note: Optional[str] = None
    alignment: Optional[str] = None; number_format: Optional[str] = None
    group_summary_action: Optional[str] = None; repeat_group_value: Optional[str] = Field(default='REPEAT')
    numeric_aggregation: Optional[str] = None

# New Models for Multiple Data Tables and Filter Mapping
class DataTableConfig(BaseModel):
    table_placeholder_name: str = Field(..., description="Unique placeholder for the AI to use for this table's rows, e.g., 'sales_by_region_table'")
    sql_query: str
    # Field display configs are now scoped to a specific data table
    field_display_configs: List[FieldDisplayConfig] = Field(default_factory=list)

class FilterUITarget(BaseModel):
    target_type: str = Field(..., description="Either 'DATA_TABLE' or 'LOOK'")
    target_id: str = Field(..., description="The 'table_placeholder_name' of the data table or the 'look_id'")
    target_field_name: str = Field(..., description="The column name in the SQL query or the filter name in the Look")

class FilterConfig(BaseModel):
    ui_filter_key: str = Field(..., description="Unique key for this filter, e.g., 'date_range_filter'")
    ui_label: str = Field(..., description="The user-friendly label shown in the UI, e.g., 'Select Date Range'")
    data_type: str = Field(default="STRING", description="Helps the UI render the correct control, e.g., 'STRING', 'DATE', 'NUMBER'")
    is_hidden_from_customer: bool = Field(default=False)
    targets: List[FilterUITarget] = Field(default_factory=list)

# Main Payload Definition - MODIFIED
class ReportDefinitionPayload(BaseModel):
    report_name: str
    image_url: str
    prompt: str

    # REPLACED 'sql_query' and 'field_display_configs' with 'data_tables'
    data_tables: List[DataTableConfig] = Field(default_factory=list)
    
    # NEW field for filter mapping and visibility
    filter_configs: List[FilterConfig] = Field(default_factory=list)

    # Existing fields remain
    look_configs: Optional[List[LookConfig]] = None
    user_attribute_mappings: Optional[Dict[str, str]] = Field(default_factory=dict)
    calculation_row_configs: Optional[List[CalculationRowConfig]] = None
    subtotal_configs: Optional[List[SubtotalConfig]] = Field(default_factory=list)
    optimized_prompt: Optional[str] = None
    header_text: Optional[str] = None
    footer_text: Optional[str] = None

# Other models for different endpoints
class ExecuteReportPayload(BaseModel):
    report_definition_name: str; filter_criteria_json: str = Field(default="{}")

class ReportDefinitionListItem(BaseModel):
    ReportName: str; Prompt: Optional[str] = None; SQL: Optional[str] = None; ScreenshotURL: Optional[str] = None
    LookConfigsJSON: Optional[str] = None
    TemplateURL: Optional[str] = None; LatestTemplateVersion: Optional[int] = None
    BaseQuerySchemaJSON: Optional[str] = None
    UserAttributeMappingsJSON: Optional[str] = None; FieldDisplayConfigsJSON: Optional[str] = None
    CalculationRowConfigsJSON: Optional[str] = None; SubtotalConfigsJSON: Optional[str] = None
    UserPlaceholderMappingsJSON: Optional[str] = None
    LastGeneratedTimestamp: Optional[datetime.datetime] = None

class SystemInstructionPayload(BaseModel): system_instruction: str

class SqlQueryPayload(BaseModel): sql_query: str

class PlaceholderMappingSuggestion(BaseModel):
    map_to_type: Optional[str] = None
    map_to_value: Optional[str] = None
    usage_as: Optional[str] = None

class DiscoveredPlaceholderInfo(BaseModel):
    original_tag: str
    key_in_tag: str
    status: str
    suggestion: Optional[PlaceholderMappingSuggestion] = None

class DiscoverPlaceholdersResponse(BaseModel):
    report_name: str; placeholders: List[DiscoveredPlaceholderInfo]
    template_found: bool; error_message: Optional[str] = None

class PlaceholderUserMapping(BaseModel):
    original_tag: str
    map_type: str
    map_to_schema_field: Optional[str] = None
    fallback_value: Optional[str] = None
    static_text_value: Optional[str] = None

class FinalizeTemplatePayload(BaseModel):
    report_name: str
    mappings: List[PlaceholderUserMapping]

class RefinementPayload(BaseModel):
    refinement_prompt_text: str

class RefinementResponse(BaseModel):
    report_name: str
    refined_html_content: str
    new_template_gcs_path: str
    message: str

# --- Global Constants ---
NUMERIC_TYPES_FOR_AGG = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]

# --- Lifespan Function ---
@asynccontextmanager
async def lifespan(app_fastapi: FastAPI):
    print("INFO: FastAPI application startup...")
    global config
    config.gcp_project_id = os.getenv("GCP_PROJECT_ID", "")
    config.gcp_location = os.getenv("GCP_LOCATION", "")
    config.GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")
    config.GCS_SYSTEM_INSTRUCTION_PATH = os.getenv("GCS_SYSTEM_INSTRUCTION_PATH", "system_instructions/default_system_instruction.txt")
    config.TARGET_GEMINI_MODEL = os.getenv("GEMINI_MODEL_OVERRIDE", "gemini-2.5-pro-preview-05-06")

    try:
        config.storage_client = storage.Client(project=config.gcp_project_id if config.gcp_project_id else None)
        print("INFO: Google Cloud Storage Client initialized successfully.")
        config.default_system_instruction_text = _load_system_instruction_from_gcs(config.storage_client, config.GCS_BUCKET_NAME, config.GCS_SYSTEM_INSTRUCTION_PATH)
    except Exception as e:
        print(f"FATAL: Failed to initialize Google Cloud Storage Client: {e}")
        config.storage_client = None
        config.default_system_instruction_text = DEFAULT_FALLBACK_SYSTEM_INSTRUCTION

    try:
        vertexai.init(project=config.gcp_project_id, location=config.gcp_location)
        config.vertex_ai_initialized = True
        print("INFO: Vertex AI SDK initialized successfully.")
    except Exception as e:
        print(f"FATAL: Vertex AI SDK Initialization Error: {e}")
        config.vertex_ai_initialized = False

    try:
        config.bigquery_client = bigquery.Client(project=config.gcp_project_id)
        print("INFO: BigQuery Client initialized successfully.")
    except Exception as e:
        print(f"FATAL: Failed to initialize BigQuery Client: {e}")
        config.bigquery_client = None
        
    try:
        print("INFO: Initializing Looker SDK from standard environment variables...")
        config.looker_sdk_client = looker_sdk.init40()
        print("INFO: Looker SDK initialized successfully.")
    except Exception as e:
        print(f"FATAL: Looker SDK auto-initialization from environment failed: {e}")
        config.looker_sdk_client = None

    yield
    print("INFO: FastAPI application shutdown.")

app = FastAPI(lifespan=lifespan)

# --- CORS Configuration ---
# --- CORS Configuration ---
NGROK_URL_FROM_ENV = os.getenv("FRONTEND_NGROK_URL")
# IMPORTANT: Replace the placeholder with your actual Looker instance URL
LOOKER_INSTANCE_URL_FROM_ENV = os.getenv("LOOKER_INSTANCE_URL", "https://igmprinting.cloud.looker.com") 
LOOKER_EXTENSION_SANDBOX_HOST = os.getenv("LOOKER_EXTENSION_SANDBOX_HOST","https://83d14716-08b3-4def-bdca-54f4b2af9f19-extensions.cloud.looker.com")
CLOUD_RUN_SERVICE_URL = "https://looker-ext-code-17837811141.us-central1.run.app"

# Add your Looker instance URL to this list
allowed_origins_list = [
    "http://localhost:8080", 
    LOOKER_INSTANCE_URL_FROM_ENV, 
    LOOKER_EXTENSION_SANDBOX_HOST, 
    CLOUD_RUN_SERVICE_URL
]
if NGROK_URL_FROM_ENV: allowed_origins_list.append(NGROK_URL_FROM_ENV)
allowed_origins_list = sorted(list(set(o for o in allowed_origins_list if o and o.startswith("http"))))
if not allowed_origins_list: allowed_origins_list = ["http://localhost:8080"]
print(f"INFO: CORS allow_origins effectively configured for: {allowed_origins_list}")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins_list, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
# --- Helper Functions & Dependency Getters ---
def _load_system_instruction_from_gcs(client: storage.Client, bucket_name: str, blob_name: str) -> str:
    if not client or not bucket_name: return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    try:
        bucket = client.bucket(bucket_name); blob = bucket.blob(blob_name)
        return blob.download_as_text(encoding='utf-8') if blob.exists() else DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    except Exception as e: return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
def get_bigquery_client_dep():
    if not config.bigquery_client: raise HTTPException(status_code=503, detail="BigQuery client not available.")
    return config.bigquery_client
def get_storage_client_dep():
    if not config.storage_client: raise HTTPException(status_code=503, detail="GCS client not available.")
    return config.storage_client
def get_vertex_ai_initialized_flag():
    if not config.vertex_ai_initialized: raise HTTPException(status_code=503, detail="Vertex AI SDK not initialized.")
_looker_sdk_authenticated = False
def get_looker_sdk_client_dep():
    global _looker_sdk_authenticated
    if not config.looker_sdk_client: raise HTTPException(status_code=503, detail="Looker SDK is not configured. Check environment variables.")
    if not _looker_sdk_authenticated:
        try:
            me = config.looker_sdk_client.me()
            print(f"INFO: Looker SDK connection verified for user: {me.display_name}")
            _looker_sdk_authenticated = True
        except Exception as e: raise HTTPException(status_code=503, detail=f"Looker SDK authentication failed: {e}")
    return config.looker_sdk_client
def remove_first_and_last_lines(s: str) -> str:
    if not s: return ""
    lines = s.splitlines();
    if len(lines) >= 2 and lines[0].strip().startswith("```") and lines[-1].strip() == "```": return '\n'.join(lines[1:-1])
    return s
def generate_html_from_user_pattern(prompt_text: str, image_bytes: bytes, image_mime_type: str, system_instruction_text: str) -> Union[str, None]:
    get_vertex_ai_initialized_flag()
    try:
        model_instance = GenerativeModel(model_name=config.TARGET_GEMINI_MODEL, system_instruction=[system_instruction_text] if system_instruction_text else None)
        contents_for_gemini = [Part.from_text(text=prompt_text), Part.from_data(data=image_bytes, mime_type=image_mime_type)]
        safety_settings_config = { category: HarmBlockThreshold.BLOCK_NONE for category in HarmCategory }
        generation_config_obj = GenerationConfig(temperature=0.7, top_p=0.95, max_output_tokens=8192, candidate_count=1)
        response = model_instance.generate_content(contents=contents_for_gemini, generation_config=generation_config_obj, safety_settings=safety_settings_config, stream=False)
        generated_text_output = "".join([part.text for part in response.candidates[0].content.parts if hasattr(part, 'text')])
        return remove_first_and_last_lines(generated_text_output)
    except Exception as e:
        print(f"ERROR: Vertex AI: GenAI content generation error: {e}"); import traceback; print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Vertex AI content generation failed: {e}")
def convert_row_to_json_serializable(row: bigquery.Row) -> Dict[str, Any]:
    output = {};
    for key, value in row.items():
        if isinstance(value, Decimal): output[key] = str(value)
        elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)): output[key] = value.isoformat()
        else: output[key] = value
    return output
def get_bq_param_type_and_value(value_str_param: Any, bq_col_name: str, type_hint: str):
    # ... This function is unchanged ...
    return "", ""
def format_value(value: Any, format_str: Optional[str], field_type_str: str) -> str:
    # ... This function is unchanged ...
    return ""
def calculate_aggregate(data_list: List[Decimal], agg_type_str_param: Optional[str]) -> Decimal:
    # ... This function is unchanged ...
    return Decimal('0')

# --- Background Task Function for Report Generation ---
# In app.py

def generate_and_save_report_assets(
    payload: ReportDefinitionPayload,
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
):
    try:
        report_name = payload.report_name
        print(f"BACKGROUND_TASK: Starting generation for report: '{report_name}'")

        prompt_for_template = payload.prompt
        all_schemas_for_bq_save = {}
        
        for table_config in payload.data_tables:
            table_placeholder = table_config.table_placeholder_name
            
            schema_from_dry_run_list = []
            try:
                # CORRECTED: Use the bq_client variable passed into the function
                dry_run_job = bq_client.query(table_config.sql_query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
                if dry_run_job.schema:
                    for field in dry_run_job.schema:
                        field_data = {"name": field.name, "type": str(field.field_type).upper(), "mode": str(field.mode).upper()}
                        schema_from_dry_run_list.append(field_data)
                all_schemas_for_bq_save[table_placeholder] = schema_from_dry_run_list
            except Exception as e:
                print(f"WARN: Dry run failed for table '{table_placeholder}'. Skipping. Error: {e}")
                continue

            schema_for_gemini_prompt_str = ", ".join([f"`{f['name']}` (Type: {f['type']})" for f in schema_from_dry_run_list])
            prompt_for_template += f"\n\n--- Data Table: `{table_placeholder}` ---\n"
            prompt_for_template += f"Use the exact placeholder `{{{{TABLE_ROWS_{table_placeholder}}}}}` for this table's body rows.\n"
            prompt_for_template += f"Schema: {schema_for_gemini_prompt_str}\n"

            if table_config.field_display_configs:
                prompt_for_template += "Field Display & Summary Instructions:\n"
                for config_item in table_config.field_display_configs:
                    style_hints = [s for s in [f"align: {config_item.alignment}" if config_item.alignment else "", f"format: {config_item.number_format}" if config_item.number_format else ""] if s]
                    field_info = f"- `{config_item.field_name}`"
                    if style_hints: field_info += f" (Styling: {'; '.join(style_hints)})"
                    prompt_for_template += f"{field_info}\n"
            prompt_for_template += "--- End Data Table ---"

        if payload.look_configs:
            prompt_for_template += "\n\n--- Chart Image Placeholders ---\nPlease include placeholders for the following charts where you see fit in the layout. Use these exact placeholder names:\n"
            for look_config in payload.look_configs:
                prompt_for_template += f"- `{{{{{look_config.placeholder_name}}}}}`\n"
            prompt_for_template += "--- End Chart Image Placeholders ---"
        
        img_response = httpx.get(payload.image_url, timeout=180.0)
        img_response.raise_for_status()
        image_bytes_data, image_mime_type_data = img_response.content, img_response.headers.get("Content-Type", "application/octet-stream").lower()

        html_template_content = generate_html_from_user_pattern(prompt_text=prompt_for_template, image_bytes=image_bytes_data, image_mime_type=image_mime_type_data, system_instruction_text=config.default_system_instruction_text)
        if not html_template_content or not html_template_content.strip():
            html_template_content = "<html><body><p>Error: AI failed to generate valid HTML.</p></body></html>"

        report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower()
        base_gcs_folder = f"report_templates/{report_gcs_path_safe}"
        versioned_template_gcs_path_str = f"{base_gcs_folder}/template_v1.html"
        
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        bucket.blob(versioned_template_gcs_path_str).upload_from_string(html_template_content, content_type='text/html; charset=utf-8')
        
        user_attribute_mappings_json_str = json.dumps(payload.user_attribute_mappings or {})
        
        data_tables_json_to_save = json.dumps([dt.model_dump() for dt in payload.data_tables], indent=2)
        schema_json_to_save = json.dumps(all_schemas_for_bq_save, indent=2)
        look_configs_json_to_save = json.dumps([lc.model_dump() for lc in payload.look_configs], indent=2) if payload.look_configs else "[]"
        calculation_row_configs_json_to_save = json.dumps([crc.model_dump(exclude_unset=True) for crc in payload.calculation_row_configs], indent=2) if payload.calculation_row_configs else "[]"
        subtotal_configs_json_to_save = json.dumps([stc.model_dump() for stc in payload.subtotal_configs], indent=2) if payload.subtotal_configs else "[]"
        filter_configs_json_to_save = json.dumps([fc.model_dump() for fc in payload.filter_configs], indent=2)

        table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
        
        merge_sql = f"""
        MERGE {table_id} T
        USING (SELECT @report_name AS ReportName) S ON T.ReportName = S.ReportName
        WHEN NOT MATCHED THEN
          INSERT (ReportName, Prompt, SQL, ScreenshotURL, LookConfigsJSON, FilterConfigsJSON, TemplateURL, LatestTemplateVersion, BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON, UserAttributeMappingsJSON, CreatedTimestamp, LastGeneratedTimestamp)
          VALUES(@report_name, @prompt, @data_tables_json, @image_url, @look_configs_json, @filter_configs_json, @template_gcs_path, 1, @schema_json, '[]', @calculation_row_configs_json, @subtotal_configs_json, @user_attribute_mappings_json, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        WHEN MATCHED THEN
          UPDATE SET Prompt = @prompt, SQL = @data_tables_json, ScreenshotURL = @image_url, LookConfigsJSON = @look_configs_json, FilterConfigsJSON = @filter_configs_json, TemplateURL = @template_gcs_path, LatestTemplateVersion = 1, BaseQuerySchemaJSON = @schema_json, FieldDisplayConfigsJSON = '[]', CalculationRowConfigsJSON = @calculation_row_configs_json, SubtotalConfigsJSON = @subtotal_configs_json, UserAttributeMappingsJSON = @user_attribute_mappings_json, LastGeneratedTimestamp = CURRENT_TIMESTAMP()
        """
        
        merge_params = [
            ScalarQueryParameter("report_name", "STRING", report_name),
            ScalarQueryParameter("prompt", "STRING", payload.prompt),
            ScalarQueryParameter("data_tables_json", "STRING", data_tables_json_to_save),
            ScalarQueryParameter("image_url", "STRING", payload.image_url),
            ScalarQueryParameter("look_configs_json", "STRING", look_configs_json_to_save),
            ScalarQueryParameter("filter_configs_json", "STRING", filter_configs_json_to_save),
            ScalarQueryParameter("template_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{versioned_template_gcs_path_str}"),
            ScalarQueryParameter("schema_json", "STRING", schema_json_to_save),
            ScalarQueryParameter("calculation_row_configs_json", "STRING", calculation_row_configs_json_to_save),
            ScalarQueryParameter("subtotal_configs_json", "STRING", subtotal_configs_json_to_save),
            ScalarQueryParameter("user_attribute_mappings_json", "STRING", user_attribute_mappings_json_str),
        ]
        bq_client.query(merge_sql, job_config=bigquery.QueryJobConfig(query_parameters=merge_params)).result()
        
        print(f"BACKGROUND_TASK: Finished generation for report: '{report_name}'")

    except Exception as e:
        print(f"FATAL BACKGROUND_TASK ERROR for '{payload.report_name}': {e}")
        import traceback
        traceback.print_exc()

# --- CORS Configuration ---
NGROK_URL_FROM_ENV = os.getenv("FRONTEND_NGROK_URL")
LOOKER_INSTANCE_URL_FROM_ENV = os.getenv("LOOKER_INSTANCE_URL")
LOOKER_EXTENSION_SANDBOX_HOST = os.getenv("LOOKER_EXTENSION_SANDBOX_HOST","https://83d14716-08b3-4def-bdca-54f4b2af9f19-extensions.cloud.looker.com")
CLOUD_RUN_SERVICE_URL = "https://looker-ext-code-17837811141.us-central1.run.app"

allowed_origins_list = ["http://localhost:8080", LOOKER_INSTANCE_URL_FROM_ENV, LOOKER_EXTENSION_SANDBOX_HOST, CLOUD_RUN_SERVICE_URL]
if NGROK_URL_FROM_ENV: allowed_origins_list.append(NGROK_URL_FROM_ENV)
allowed_origins_list = sorted(list(set(o for o in allowed_origins_list if o and o.startswith("http"))))
if not allowed_origins_list: allowed_origins_list = ["http://localhost:8080"]
print(f"INFO: CORS allow_origins effectively configured for: {allowed_origins_list}")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins_list, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- Helper Functions & Dependency Getters ---
def _load_system_instruction_from_gcs(client: storage.Client, bucket_name: str, blob_name: str) -> str:
    if not client or not bucket_name:
        print(f"WARN: GCS client/bucket not provided. Using fallback system instruction.")
        return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            print(f"INFO: Loaded system instruction from gs://{bucket_name}/{blob_name}")
            return blob.download_as_text(encoding='utf-8')
        print(f"WARN: System instruction file not found at gs://{bucket_name}/{blob_name}. Using fallback.")
        return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    except Exception as e:
        print(f"ERROR: Failed to load system instruction from GCS: {e}. Using fallback.")
        return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION

def get_bigquery_client_dep():
    if not config.bigquery_client: raise HTTPException(status_code=503, detail="BigQuery client not available.")
    return config.bigquery_client

def get_storage_client_dep():
    if not config.storage_client: raise HTTPException(status_code=503, detail="GCS client not available.")
    return config.storage_client

def get_vertex_ai_initialized_flag():
    if not config.vertex_ai_initialized: raise HTTPException(status_code=503, detail="Vertex AI SDK not initialized.")
    if not config.TARGET_GEMINI_MODEL: raise HTTPException(status_code=503, detail="TARGET_GEMINI_MODEL not configured.")

_looker_sdk_authenticated = False
def get_looker_sdk_client_dep():
    global _looker_sdk_authenticated
    if not config.looker_sdk_client:
        raise HTTPException(status_code=503, detail="Looker SDK is not configured. Check environment variables.")
    
    if not _looker_sdk_authenticated:
        try:
            me = config.looker_sdk_client.me()
            print(f"INFO: Looker SDK connection verified for user: {me.display_name}")
            _looker_sdk_authenticated = True
        except Exception as e:
            print(f"ERROR: Looker SDK authentication failed: {e}")
            raise HTTPException(status_code=503, detail=f"Looker SDK authentication failed: {e}")
            
    return config.looker_sdk_client
    
def remove_first_and_last_lines(s: str) -> str:
    if not s: return ""
    lines = s.splitlines();
    if len(lines) >= 2 and lines[0].strip().startswith("```") and lines[-1].strip() == "```": return '\n'.join(lines[1:-1])
    if len(lines) >= 1 and lines[0].strip().startswith("```"): return '\n'.join(lines[1:]) if len(lines) > 1 else ""
    return s

def generate_html_from_user_pattern(
    prompt_text: str, image_bytes: bytes, image_mime_type: str, system_instruction_text: str
) -> Union[str, None]:
    get_vertex_ai_initialized_flag()
    print(f"DEBUG: Vertex AI: System Instruction (first 100): {system_instruction_text[:100]}")
    print(f"DEBUG: Vertex AI: Target Model: {config.TARGET_GEMINI_MODEL}")
    try:
        model_instance = GenerativeModel(model_name=config.TARGET_GEMINI_MODEL, system_instruction=[system_instruction_text] if system_instruction_text else None)
        image_part = Part.from_data(data=image_bytes, mime_type=image_mime_type)
        prompt_part = Part.from_text(text=prompt_text)
        contents_for_gemini = [prompt_part, image_part]
        safety_settings_config = { category: HarmBlockThreshold.BLOCK_NONE for category in HarmCategory }
        generation_config_obj = GenerationConfig(temperature=0.7, top_p=0.95, max_output_tokens=65535, candidate_count=1)
        response = model_instance.generate_content(contents=contents_for_gemini, generation_config=generation_config_obj, safety_settings=safety_settings_config, stream=False)
        generated_text_output = ""
        if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
            for part_item in response.candidates[0].content.parts:
                if hasattr(part_item, 'text') and part_item.text: generated_text_output += part_item.text
        else: print(f"WARN: Gemini response structure unexpected or no text. Response: {response}")
    except (google_api_exceptions.NotFound, vertexai.generative_models.exceptions.NotFoundError) as e_nf:
        error_detail = f"Model '{config.TARGET_GEMINI_MODEL}' not found or project lacks access: {str(e_nf)}"
        print(f"ERROR: Vertex AI (NotFound): {error_detail}"); raise HTTPException(status_code=404, detail=error_detail)
    except google_api_exceptions.InvalidArgument as e_ia:
        error_detail = f"Invalid argument for model '{config.TARGET_GEMINI_MODEL}': {str(e_ia)}"
        print(f"ERROR: Vertex AI (InvalidArgument): {error_detail}"); raise HTTPException(status_code=400, detail=error_detail)
    except Exception as e:
        print(f"ERROR: Vertex AI: GenAI content generation error: {type(e).__name__} - {str(e)}")
        import traceback; print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Vertex AI content generation failed: {str(e)}")
    print(f"DEBUG: Raw Gemini Output before remove (first 500): {generated_text_output[:500]}")
    processed_html = remove_first_and_last_lines(generated_text_output)
    print(f"DEBUG: Processed Gemini Output after remove (first 500): {processed_html[:500]}")
    return processed_html if processed_html else ""

def convert_row_to_json_serializable(row: bigquery.Row) -> Dict[str, Any]:
    output = {};
    for key, value in row.items():
        if isinstance(value, Decimal): output[key] = str(value)
        elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)): output[key] = value.isoformat()
        elif isinstance(value, bytes): output[key] = base64.b64encode(value).decode('utf-8')
        elif isinstance(value, list): output[key] = [(item.isoformat() if isinstance(item, (datetime.date, datetime.datetime, datetime.time)) else str(item) if isinstance(item, Decimal) else item) for item in value]
        else: output[key] = value
    return output

def get_bq_param_type_and_value(value_str_param: Any, bq_col_name: str, type_hint: str):
    value_str = str(value_str_param)
    if type_hint == "NONE": return None, None
    if type_hint == "STRING_ARRAY": items = [item.strip() for item in value_str.split(',') if item.strip()]; return "STRING", items
    if type_hint == "STRING_PREFIX": return "STRING", f"{value_str}%"
    if type_hint == "STRING_SUFFIX": return "STRING", f"%{value_str}"
    if type_hint == "BOOL_TRUE_STR": return "BOOL", True
    if type_hint == "BOOL_FALSE_STR": return "BOOL", False
    if type_hint == "AUTO_DATE_OR_NUM_RANGE":
        parts = [v.strip() for v in value_str.split(',', 1)]; val1_str, val2_str = (parts[0], parts[1]) if len(parts) == 2 else (parts[0], parts[0])
        try: return "DATE_RANGE", (datetime.date.fromisoformat(val1_str), datetime.date.fromisoformat(val2_str))
        except: pass
        try: return "INT64_RANGE", (int(val1_str), int(val2_str))
        except: pass
        try: return "FLOAT64_RANGE", (float(val1_str), float(val2_str))
        except: pass
        return "STRING_RANGE", (str(val1_str), str(val2_str))
    if type_hint == "AUTO_DATE_OR_NUM" or type_hint == "AUTO":
        try: return "DATE", datetime.date.fromisoformat(value_str)
        except: pass
        try: return "INT64", int(value_str)
        except: pass
        try: return "FLOAT64", float(value_str)
        except: pass
        if value_str.lower() in ['true', 'false'] and type_hint == "AUTO": return "BOOL", value_str.lower() == 'true'
        return "STRING", str(value_str)
    if type_hint == "STRING": return "STRING", str(value_str)
    if type_hint == "INT64":
        try: return "INT64", int(value_str)
        except: raise ValueError(f"Invalid int: {value_str} for {bq_col_name}")
    if type_hint == "FLOAT64":
        try: return "FLOAT64", float(value_str)
        except: raise ValueError(f"Invalid float: {value_str} for {bq_col_name}")
    if type_hint == "DATE":
        try: return "DATE", datetime.date.fromisoformat(value_str)
        except: raise ValueError(f"Invalid date: {value_str} for {bq_col_name}. Use YYYY-MM-DD.")
    if type_hint == "BOOL":
        val_lower = value_str.lower()
        if val_lower in ['true', 'false']: return "BOOL", val_lower == 'true'
        raise ValueError(f"Invalid bool: {value_str} for {bq_col_name}. Use 'true' or 'false'.")
    return "STRING", str(value_str)

def format_value(value: Any, format_str: Optional[str], field_type_str: str) -> str:
    if value is None: return ""
    field_type_upper = str(field_type_str).upper() if field_type_str else "UNKNOWN"
    if format_str and field_type_upper in NUMERIC_TYPES_FOR_AGG:
        try:
            str_value = str(value) if not isinstance(value, (int, float, Decimal)) else value; num_value = Decimal(str_value)
            if format_str == 'INTEGER': return f"{num_value:,.0f}"
            elif format_str == 'DECIMAL_2': return f"{num_value:,.2f}"
            elif format_str == 'USD': return f"${num_value:,.2f}"
            elif format_str == 'EUR': return f"€{num_value:,.2f}"
            elif format_str == 'PERCENT_2': return f"{num_value * Decimal('100'):,.2f}%"
            else: return str(value)
        except (ValueError, TypeError, InvalidOperation) as e:
            print(f"WARN: Formatting error for numeric value '{value}' with format '{format_str}': {e}")
            return str(value)
    return str(value)

def calculate_aggregate(data_list: List[Decimal], agg_type_str_param: Optional[str]) -> Decimal:
    if not agg_type_str_param: return Decimal('0')
    agg_type = agg_type_str_param.upper()
    if not data_list:
        if agg_type in ['COUNT', 'COUNT_DISTINCT']: return Decimal('0')
        return Decimal('0')
    if agg_type == "SUM": return sum(data_list)
    elif agg_type == "AVERAGE": return sum(data_list) / Decimal(len(data_list)) if data_list else Decimal('0')
    elif agg_type == "MIN": return min(data_list)
    elif agg_type == "MAX": return max(data_list)
    elif agg_type == "COUNT": return Decimal(len(data_list))
    elif agg_type == "COUNT_DISTINCT": return Decimal(len(set(data_list)))
    print(f"WARN: Unknown aggregation type '{agg_type_str_param}' received. Returning 0.")
    return Decimal('0')

# --- Background Task Function for Report Generation ---

def generate_and_save_report_assets(
    payload: ReportDefinitionPayload,
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
):
    try:
        report_name = payload.report_name
        print(f"BACKGROUND_TASK: Starting generation for report: '{report_name}'")

        prompt_for_template = payload.prompt
        all_schemas_for_bq_save = {}
        
        for table_config in payload.data_tables:
            table_placeholder = table_config.table_placeholder_name
            
            schema_from_dry_run_list = []
            try:
                # Use the correct bq_client variable passed into the function
                dry_run_job = bq_client.query(table_config.sql_query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
                if dry_run_job.schema:
                    for field in dry_run_job.schema:
                        field_data = {"name": field.name, "type": str(field.field_type).upper(), "mode": str(field.mode).upper()}
                        schema_from_dry_run_list.append(field_data)
                all_schemas_for_bq_save[table_placeholder] = schema_from_dry_run_list
            except Exception as e:
                print(f"WARN: Dry run failed for table '{table_placeholder}'. Skipping. Error: {e}")
                continue

            schema_for_gemini_prompt_str = ", ".join([f"`{f['name']}` (Type: {f['type']})" for f in schema_from_dry_run_list])
            prompt_for_template += f"\n\n--- Data Table: `{table_placeholder}` ---\n"
            prompt_for_template += f"Use the exact placeholder `{{{{TABLE_ROWS_{table_placeholder}}}}}` for this table's body rows.\n"
            prompt_for_template += f"Schema: {schema_for_gemini_prompt_str}\n"

            if table_config.field_display_configs:
                prompt_for_template += "Field Display & Summary Instructions:\n"
                for config_item in table_config.field_display_configs:
                    style_hints = [s for s in [f"align: {config_item.alignment}" if config_item.alignment else "", f"format: {config_item.number_format}" if config_item.number_format else ""] if s]
                    field_info = f"- `{config_item.field_name}`"
                    if style_hints: field_info += f" (Styling: {'; '.join(style_hints)})"
                    prompt_for_template += f"{field_info}\n"
            prompt_for_template += "--- End Data Table ---"

        if payload.look_configs:
            prompt_for_template += "\n\n--- Chart Image Placeholders ---\nPlease include placeholders for the following charts where you see fit in the layout. Use these exact placeholder names:\n"
            for look_config in payload.look_configs:
                prompt_for_template += f"- `{{{{{look_config.placeholder_name}}}}}`\n"
            prompt_for_template += "--- End Chart Image Placeholders ---"
        
        img_response = httpx.get(payload.image_url, timeout=180.0)
        img_response.raise_for_status()
        image_bytes_data, image_mime_type_data = img_response.content, img_response.headers.get("Content-Type", "application/octet-stream").lower()

        html_template_content = generate_html_from_user_pattern(prompt_text=prompt_for_template, image_bytes=image_bytes_data, image_mime_type=image_mime_type_data, system_instruction_text=config.default_system_instruction_text)
        if not html_template_content or not html_template_content.strip():
            html_template_content = "<html><body><p>Error: AI failed to generate valid HTML.</p></body></html>"

        report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower()
        base_gcs_folder = f"report_templates/{report_gcs_path_safe}"
        versioned_template_gcs_path_str = f"{base_gcs_folder}/template_v1.html"
        
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        bucket.blob(versioned_template_gcs_path_str).upload_from_string(html_template_content, content_type='text/html; charset=utf-8')
        
        user_attribute_mappings_json_str = json.dumps(payload.user_attribute_mappings or {})
        
        data_tables_json_to_save = json.dumps([dt.model_dump() for dt in payload.data_tables], indent=2)
        schema_json_to_save = json.dumps(all_schemas_for_bq_save, indent=2)
        look_configs_json_to_save = json.dumps([lc.model_dump() for lc in payload.look_configs], indent=2) if payload.look_configs else "[]"
        calculation_row_configs_json_to_save = json.dumps([crc.model_dump(exclude_unset=True) for crc in payload.calculation_row_configs], indent=2) if payload.calculation_row_configs else "[]"
        subtotal_configs_json_to_save = json.dumps([stc.model_dump() for stc in payload.subtotal_configs], indent=2) if payload.subtotal_configs else "[]"
        filter_configs_json_to_save = json.dumps([fc.model_dump() for fc in payload.filter_configs], indent=2)

        table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
        
        merge_sql = f"""
        MERGE {table_id} T
        USING (SELECT @report_name AS ReportName) S ON T.ReportName = S.ReportName
        WHEN NOT MATCHED THEN
          INSERT (ReportName, Prompt, SQL, ScreenshotURL, LookConfigsJSON, FilterConfigsJSON, TemplateURL, LatestTemplateVersion, BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON, UserAttributeMappingsJSON, CreatedTimestamp, LastGeneratedTimestamp)
          VALUES(@report_name, @prompt, @data_tables_json, @image_url, @look_configs_json, @filter_configs_json, @template_gcs_path, 1, @schema_json, '[]', @calculation_row_configs_json, @subtotal_configs_json, @user_attribute_mappings_json, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        WHEN MATCHED THEN
          UPDATE SET Prompt = @prompt, SQL = @data_tables_json, ScreenshotURL = @image_url, LookConfigsJSON = @look_configs_json, FilterConfigsJSON = @filter_configs_json, TemplateURL = @template_gcs_path, LatestTemplateVersion = 1, BaseQuerySchemaJSON = @schema_json, FieldDisplayConfigsJSON = '[]', CalculationRowConfigsJSON = @calculation_row_configs_json, SubtotalConfigsJSON = @subtotal_configs_json, UserAttributeMappingsJSON = @user_attribute_mappings_json, LastGeneratedTimestamp = CURRENT_TIMESTAMP()
        """
        
        merge_params = [
            ScalarQueryParameter("report_name", "STRING", report_name),
            ScalarQueryParameter("prompt", "STRING", payload.prompt),
            ScalarQueryParameter("data_tables_json", "STRING", data_tables_json_to_save),
            ScalarQueryParameter("image_url", "STRING", payload.image_url),
            ScalarQueryParameter("look_configs_json", "STRING", look_configs_json_to_save),
            ScalarQueryParameter("filter_configs_json", "STRING", filter_configs_json_to_save),
            ScalarQueryParameter("template_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{versioned_template_gcs_path_str}"),
            ScalarQueryParameter("schema_json", "STRING", schema_json_to_save),
            ScalarQueryParameter("calculation_row_configs_json", "STRING", calculation_row_configs_json_to_save),
            ScalarQueryParameter("subtotal_configs_json", "STRING", subtotal_configs_json_to_save),
            ScalarQueryParameter("user_attribute_mappings_json", "STRING", user_attribute_mappings_json_str),
        ]
        bq_client.query(merge_sql, job_config=bigquery.QueryJobConfig(query_parameters=merge_params)).result()
        
        print(f"BACKGROUND_TASK: Finished generation for report: '{report_name}'")

    except Exception as e:
        print(f"FATAL BACKGROUND_TASK ERROR for '{payload.report_name}': {e}")
        import traceback
        traceback.print_exc()
# --- API Endpoints ---
@app.get("/")
async def read_root():
    return {"status": f"GenAI Report API is running! (Target Model: {config.TARGET_GEMINI_MODEL})"}

@app.post("/dry_run_sql_for_schema")
async def dry_run_sql_for_schema_endpoint(
    payload: SqlQueryPayload, bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
):
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_job = bq_client.query(payload.sql_query, job_config=job_config)
        schema_for_response = [{"name": f.name, "type": str(f.field_type).upper(), "mode": str(f.mode).upper()} for f in dry_run_job.schema] if dry_run_job.schema else []
        return {"schema": schema_for_response} if schema_for_response else {"schema": [], "message": "Dry run OK but no schema."}
    except Exception as e:
        error_message = str(e); error_details = [err.get('message', 'BQ err') for err in getattr(e, 'errors', [])]; error_message = "; ".join(error_details) if error_details else error_message
        print(f"ERROR: SQL dry run failed: {error_message} for query: {payload.sql_query}")
        raise HTTPException(status_code=400, detail=f"SQL dry run failed: {error_message}")

@app.get("/system_instruction")
async def get_system_instruction_endpoint(storage_client: storage.Client = Depends(get_storage_client_dep)):
    return {"system_instruction": config.default_system_instruction_text}

@app.put("/system_instruction")
async def update_system_instruction_endpoint(
    payload: SystemInstructionPayload, storage_client: storage.Client = Depends(get_storage_client_dep)
):
    new_instruction_text = payload.system_instruction
    try:
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME); blob = bucket.blob(config.GCS_SYSTEM_INSTRUCTION_PATH)
        blob.upload_from_string(new_instruction_text, content_type='text/plain; charset=utf-8')
        config.default_system_instruction_text = new_instruction_text
        return {"message": "System instruction updated successfully."}
    except Exception as e: print(f"ERROR: Failed to PUT system instruction to GCS: {e}"); raise HTTPException(status_code=500, detail=f"Failed to update system instruction: {str(e)}")

@app.get("/report_definitions/{report_name}/discover_placeholders", response_model=DiscoverPlaceholdersResponse)
async def discover_template_placeholders(
    report_name: str,
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
):
    template_gcs_path: Optional[str] = None; field_configs_json_str: Optional[str] = None; calc_row_configs_json_str: Optional[str] = None
    query_def_sql = f"SELECT TemplateURL, FieldDisplayConfigsJSON, CalculationRowConfigsJSON FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param"
    def_params = [ScalarQueryParameter("report_name_param", "STRING", report_name)]
    try:
        results = list(bq_client.query(query_def_sql, job_config=bigquery.QueryJobConfig(query_parameters=def_params)).result())
        if results and results[0].get("TemplateURL"):
            template_gcs_path = results[0].get("TemplateURL"); field_configs_json_str = results[0].get("FieldDisplayConfigsJSON"); calc_row_configs_json_str = results[0].get("CalculationRowConfigsJSON")
        else: return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=[], template_found=False, error_message=f"Definition or TemplateURL not found for '{report_name}'.")
    except Exception as e: return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=[], template_found=False, error_message=f"Error fetching report definition details: {str(e)}")

    if not template_gcs_path or not template_gcs_path.startswith("gs://"): return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=[], template_found=False, error_message=f"Invalid GCS TemplateURL: {template_gcs_path}")
    html_content: str = ""
    try:
        path_parts = template_gcs_path.replace("gs://", "").split("/", 1); bucket_name, blob_name = path_parts[0], path_parts[1]
        blob = gcs_client.bucket(bucket_name).blob(blob_name)
        if not blob.exists(): return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=[], template_found=False, error_message=f"Template not found at {template_gcs_path}")
        html_content = blob.download_as_text(encoding='utf-8')
    except Exception as e: return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=[], template_found=False, error_message=f"Error loading template from GCS: {str(e)}")
    
    field_display_configs_for_discovery: List[FieldDisplayConfig] = []
    if field_configs_json_str:
        try: field_display_configs_for_discovery = [FieldDisplayConfig(**item) for item in json.loads(field_configs_json_str)]
        except (json.JSONDecodeError, TypeError) as e: print(f"WARN: Could not parse FieldDisplayConfigsJSON for '{report_name}' in discover: {e}")
    calculation_rows_configs_for_discovery: List[CalculationRowConfig] = []
    if calc_row_configs_json_str:
        try: calculation_rows_configs_for_discovery = [CalculationRowConfig(**item) for item in json.loads(calc_row_configs_json_str)]
        except (json.JSONDecodeError, TypeError) as e: print(f"WARN: Could not parse CalculationRowConfigsJSON for '{report_name}' in discover: {e}")
    
    discovered_placeholders_list: List[DiscoveredPlaceholderInfo] = []
    placeholder_keys_found = set(re.findall(r"\{\{([^{}]+?)\}\}", html_content, re.DOTALL))
    for key_in_tag_raw in placeholder_keys_found:
        key_in_tag_content = key_in_tag_raw.strip()
        original_full_tag = f"{{{{{key_in_tag_content}}}}}"
        if not key_in_tag_content: continue
        status = "unrecognized"; suggestion = None
        if key_in_tag_content == "TABLE_ROWS_HTML_PLACEHOLDER":
            status = "standard_table_rows"; suggestion = PlaceholderMappingSuggestion(map_to_type="standard_placeholder", map_to_value=key_in_tag_content)
        else:
            matched_by_config = False
            for fd_config in field_display_configs_for_discovery:
                if key_in_tag_content == f"TOP_{fd_config.field_name}" and fd_config.include_at_top:
                    status = "auto_matched_top"; suggestion = PlaceholderMappingSuggestion(map_to_type="schema_field", map_to_value=fd_config.field_name, usage_as="TOP"); matched_by_config = True; break
                if key_in_tag_content == f"HEADER_{fd_config.field_name}" and fd_config.include_in_header:
                    status = "auto_matched_header"; suggestion = PlaceholderMappingSuggestion(map_to_type="schema_field", map_to_value=fd_config.field_name, usage_as="HEADER"); matched_by_config = True; break
            if not matched_by_config:
                for calc_config in calculation_rows_configs_for_discovery:
                    if key_in_tag_content == calc_config.values_placeholder_name:
                        status = "auto_matched_calc_row"; suggestion = PlaceholderMappingSuggestion(map_to_type="calculation_row_placeholder", map_to_value=key_in_tag_content); break
        discovered_placeholders_list.append(DiscoveredPlaceholderInfo(original_tag=original_full_tag,key_in_tag=key_in_tag_content,status=status,suggestion=suggestion))
    unique_placeholders_dict = {p.original_tag: p for p in discovered_placeholders_list}
    final_placeholders = list(unique_placeholders_dict.values())
    final_placeholders.sort(key=lambda p: (p.status, p.key_in_tag))
    return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=final_placeholders, template_found=True)

@app.post("/report_definitions", status_code=202)
async def upsert_report_definition(
    payload: ReportDefinitionPayload,
    background_tasks: BackgroundTasks,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep)
):
    print(f"--- Backend Payload Received ---\n{payload.model_dump_json(indent=2)}")
    """
    Accepts a report definition, validates it quickly, and schedules the slow
    AI generation and asset saving to run in the background. Returns immediately.
    """
    print(f"INFO: Submission received for report '{payload.report_name}'. Scheduling for background generation.")
    
    # Add the slow work to a background task, passing all necessary dependencies
    background_tasks.add_task(
        generate_and_save_report_assets,
        payload=payload,
        bq_client=bq_client,
        gcs_client=gcs_client
    )
    
    # Return an immediate response to the user
    return {"message": f"Report definition '{payload.report_name}' accepted and is being generated in the background."}

@app.get("/report_definitions", response_model=List[ReportDefinitionListItem])
async def list_report_definitions_endpoint(bq_client: bigquery.Client = Depends(get_bigquery_client_dep)):
    query = f"""
        SELECT ReportName, Prompt, SQL, ScreenshotURL, LookConfigsJSON, TemplateURL, 
               LatestTemplateVersion, BaseQuerySchemaJSON, FieldDisplayConfigsJSON, 
               CalculationRowConfigsJSON, SubtotalConfigsJSON, UserPlaceholderMappingsJSON, 
               UserAttributeMappingsJSON, LastGeneratedTimestamp 
        FROM `{config.gcp_project_id}.report_printing.report_list` ORDER BY ReportName ASC
    """
    try:
        results = list(bq_client.query(query).result())
        processed_results = []
        for row_dict_item in [dict(row.items()) for row in results]:
            for json_field in ['LookConfigsJSON', 'BaseQuerySchemaJSON', 'FieldDisplayConfigsJSON', 'CalculationRowConfigsJSON', 'SubtotalConfigsJSON', 'UserAttributeMappingsJSON', 'UserPlaceholderMappingsJSON']:
                if row_dict_item.get(json_field) is None:
                    row_dict_item[json_field] = "{}" if json_field == 'UserAttributeMappingsJSON' else "[]"
            if row_dict_item.get("LatestTemplateVersion") is None:
                row_dict_item["LatestTemplateVersion"] = 0
            try: processed_results.append(ReportDefinitionListItem(**row_dict_item))
            except Exception as pydantic_error: print(f"ERROR: Pydantic validation for report {row_dict_item.get('ReportName')}: {pydantic_error}. Data: {row_dict_item}"); continue
        return processed_results
    except Exception as e: print(f"ERROR fetching report definitions: {e}"); raise HTTPException(status_code=500, detail=f"Failed to fetch report definitions: {str(e)}")

@app.post("/report_definitions/{report_name}/finalize_template", status_code=200)
async def finalize_template_with_mappings(
    report_name: str,
    payload: FinalizeTemplatePayload,
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
):
    print(f"INFO: Finalizing template (with placeholder mappings) for report '{report_name}'.")
    query_def_sql = f"SELECT TemplateURL, LatestTemplateVersion FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param"
    def_params = [ScalarQueryParameter("report_name_param", "STRING", report_name)]
    try:
        results = list(bq_client.query(query_def_sql, job_config=bigquery.QueryJobConfig(query_parameters=def_params)).result())
        if not results: raise HTTPException(status_code=404, detail=f"Report definition not found for '{report_name}'.")
        current_template_gcs_path = results[0].get("TemplateURL")
        last_version_number = results[0].get("LatestTemplateVersion") or 0
        if not current_template_gcs_path: raise HTTPException(status_code=404, detail=f"Current TemplateURL not found for '{report_name}'.")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error fetching report details: {str(e)}")

    try:
        path_parts = current_template_gcs_path.replace("gs://", "").split("/", 1)
        bucket_name, current_blob_name = path_parts[0], path_parts[1]
        bucket = gcs_client.bucket(bucket_name)
        template_blob_current = bucket.blob(current_blob_name)
        if not template_blob_current.exists(): raise HTTPException(status_code=404, detail=f"Template file not found at {current_template_gcs_path}.")
        current_html_content = template_blob_current.download_as_text(encoding='utf-8')
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error loading current template from GCS: {str(e)}")

    modified_html_content = current_html_content
    for mapping in payload.mappings:
        original_tag_escaped = re.escape(mapping.original_tag)
        if mapping.map_type == "ignore": modified_html_content = re.sub(original_tag_escaped, "", modified_html_content)
        elif mapping.map_type == "static_text" and mapping.static_text_value is not None: modified_html_content = re.sub(original_tag_escaped, mapping.static_text_value, modified_html_content)
        elif mapping.map_type == "standardize_top" and mapping.map_to_schema_field: new_tag = f"{{{{TOP_{mapping.map_to_schema_field}}}}}"; modified_html_content = re.sub(original_tag_escaped, new_tag, modified_html_content)
        elif mapping.map_type == "standardize_header" and mapping.map_to_schema_field: new_tag = f"{{{{HEADER_{mapping.map_to_schema_field}}}}}"; modified_html_content = re.sub(original_tag_escaped, new_tag, modified_html_content)

    new_version_number = last_version_number + 1
    report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower()
    base_gcs_folder_for_report = f"report_templates/{report_gcs_path_safe}"
    new_template_filename = f"template_v{new_version_number}.html"
    new_versioned_gcs_path_str = f"{base_gcs_folder_for_report}/{new_template_filename}"

    try:
        new_template_blob = bucket.blob(new_versioned_gcs_path_str)
        new_template_blob.upload_from_string(modified_html_content, content_type='text/html; charset=utf-8')
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to save finalized (v{new_version_number}) template to GCS: {str(e)}")

    mappings_json_to_save = json.dumps([m.model_dump(exclude_unset=True) for m in payload.mappings], indent=2)
    table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
    update_sql = f"""
        UPDATE {table_id}
        SET TemplateURL = @new_template_url, LatestTemplateVersion = @new_version,
            UserPlaceholderMappingsJSON = @mappings_json, LastGeneratedTimestamp = CURRENT_TIMESTAMP()
        WHERE ReportName = @report_name
    """
    update_params = [
        ScalarQueryParameter("new_template_url", "STRING", f"gs://{bucket_name}/{new_versioned_gcs_path_str}"),
        ScalarQueryParameter("new_version", "INT64", new_version_number),
        ScalarQueryParameter("mappings_json", "STRING", mappings_json_to_save),
        ScalarQueryParameter("report_name", "STRING", report_name),
    ]
    try:
        job = bq_client.query(update_sql, job_config=bigquery.QueryJobConfig(query_parameters=update_params)); job.result()
    except Exception as e: print(f"ERROR: Failed to update BigQuery for finalized template v{new_version_number} for '{report_name}': {str(e)}")

    return {"message": f"Template for report '{report_name}' finalized to v{new_version_number} and mappings saved.", "new_template_gcs_path": f"gs://{bucket_name}/{new_versioned_gcs_path_str}"}

@app.post("/report_definitions/{report_name}/refine_template", response_model=RefinementResponse)
async def refine_report_template_oneshot(
    report_name: str, payload: RefinementPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    _vertex_ai_init_check: None = Depends(get_vertex_ai_initialized_flag)
):
    print(f"INFO: Refining template for report '{report_name}'.")
    query_def_sql = f"SELECT TemplateURL, ScreenshotURL, LatestTemplateVersion FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param"
    def_params = [ScalarQueryParameter("report_name_param", "STRING", report_name)]
    try:
        results = list(bq_client.query(query_def_sql, job_config=bigquery.QueryJobConfig(query_parameters=def_params)).result())
        if not results: raise HTTPException(status_code=404, detail=f"Report definition '{report_name}' not found.")
        report_def = results[0]
        current_template_gcs_path = report_def.get("TemplateURL")
        image_url_for_context = report_def.get("ScreenshotURL")
        last_version_number = report_def.get("LatestTemplateVersion") or 0
        if not current_template_gcs_path or not current_template_gcs_path.startswith("gs://"): raise HTTPException(status_code=404, detail=f"Valid TemplateURL not found.")
        if not image_url_for_context: raise HTTPException(status_code=400, detail=f"ImageURL not found, needed for refinement.")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error fetching report details for refinement: {str(e)}")

    try:
        path_parts = current_template_gcs_path.replace("gs://", "").split("/", 1)
        bucket_name, current_blob_name = path_parts[0], path_parts[1]
        bucket = gcs_client.bucket(bucket_name)
        template_blob_current = bucket.blob(current_blob_name)
        if not template_blob_current.exists(): raise HTTPException(status_code=404, detail=f"Template file not found at {current_template_gcs_path}.")
        current_html_content = template_blob_current.download_as_text(encoding='utf-8')
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error loading current template from GCS: {str(e)}")

    refinement_prompt_for_gemini = f"""
You are an expert HTML and CSS developer. You will be provided with an existing HTML document and a set of instructions to refine it.
Your task is to apply the refinement instructions to the existing HTML and output the COMPLETE, new, valid HTML document.
Ensure all original placeholder syntax (double curly braces like {{{{PlaceholderKey}}}}) is preserved unless the instructions specifically ask to change them.
The visual style should still be guided by the provided style-guide image.

EXISTING HTML DOCUMENT:
---
{current_html_content}
---

USER'S REFINEMENT INSTRUCTIONS:
---
{payload.refinement_prompt_text}
---

Based on the refinement instructions, and keeping the style-guide image in mind, please provide the full, updated HTML code.
Remember: Output ONLY the raw HTML code. No descriptions, no explanations, no markdown. Start with `<!DOCTYPE html>` or `<html>` and end with `</html>`.
ALL placeholders for dynamic data MUST use double curly braces, e.g., {{{{YourPlaceholderKey}}}}. Single braces (e.g., {{YourPlaceholderKey}}) are NOT PERMITTED and will not be processed.
    """
    try:
        async with httpx.AsyncClient(timeout=180.0) as client_httpx:
            img_response = await client_httpx.get(image_url_for_context); img_response.raise_for_status()
            image_bytes_data = await img_response.aread()
            image_mime_type_data = img_response.headers.get("Content-Type", "application/octet-stream").lower()
            if not image_mime_type_data.startswith("image/"): raise ValueError("Content-Type from URL is not valid for image.")
    except Exception as e: raise HTTPException(status_code=400, detail=f"Error fetching style-guide image URL '{image_url_for_context}' for refinement: {str(e)}")

    refined_html_output = generate_html_from_user_pattern(
        prompt_text=refinement_prompt_for_gemini, image_bytes=image_bytes_data,
        image_mime_type=image_mime_type_data, system_instruction_text=config.default_system_instruction_text
    )
    if not refined_html_output or not refined_html_output.strip():
        raise HTTPException(status_code=500, detail="AI failed to generate refined HTML content.")

    new_version_number = last_version_number + 1
    report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower()
    base_gcs_folder_for_report = f"report_templates/{report_gcs_path_safe}"
    new_template_filename = f"template_v{new_version_number}.html"
    new_versioned_gcs_path_str = f"{base_gcs_folder_for_report}/{new_template_filename}"
    try:
        new_template_blob = bucket.blob(new_versioned_gcs_path_str)
        new_template_blob.upload_from_string(refined_html_output, content_type='text/html; charset=utf-8')
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to save refined template v{new_version_number} to GCS: {str(e)}")

    table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
    update_sql = f"""
        UPDATE {table_id} 
        SET TemplateURL = @new_template_url, LatestTemplateVersion = @new_version, LastGeneratedTimestamp = CURRENT_TIMESTAMP() 
        WHERE ReportName = @report_name
    """
    update_params = [
        ScalarQueryParameter("new_template_url", "STRING", f"gs://{bucket_name}/{new_versioned_gcs_path_str}"),
        ScalarQueryParameter("new_version", "INT64", new_version_number),
        ScalarQueryParameter("report_name", "STRING", report_name)
    ]
    try:
        job = bq_client.query(update_sql, job_config=bigquery.QueryJobConfig(query_parameters=update_params)); job.result()
    except Exception as e: print(f"ERROR: Failed to update BigQuery for refined template v{new_version_number} for '{report_name}': {str(e)}")

    return RefinementResponse(
        report_name=report_name, refined_html_content=refined_html_output,
        new_template_gcs_path=f"gs://{bucket_name}/{new_versioned_gcs_path_str}",
        message=f"Template refined to version {new_version_number} and updated successfully."
    )

@app.post("/execute_report")
async def execute_report_and_get_url(
    payload: ExecuteReportPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    looker_sdk: methods40.Looker40SDK = Depends(get_looker_sdk_client_dep)
):
    global config
    report_definition_name = payload.report_definition_name
    filter_criteria_json_str = payload.filter_criteria_json
    print(f"INFO: POST /execute_report for '{report_definition_name}'. Filters JSON: {filter_criteria_json_str}")

    # --- 1. Fetch and Parse Report Definition ---
    query_def_sql_exec = f"""
        SELECT SQL, TemplateURL, UserAttributeMappingsJSON, BaseQuerySchemaJSON, FilterConfigsJSON,
               LookConfigsJSON, CalculationRowConfigsJSON, UserPlaceholderMappingsJSON
        FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param
    """
    def_params_exec = [ScalarQueryParameter("report_name_param", "STRING", report_definition_name)]
    try:
        results_exec = list(bq_client.query(query_def_sql_exec, job_config=bigquery.QueryJobConfig(query_parameters=def_params_exec)).result())
        if not results_exec:
            raise HTTPException(status_code=404, detail=f"Report definition '{report_definition_name}' not found.")
        
        row_exec = results_exec[0]
        data_tables_json = row_exec.get("SQL")
        html_template_gcs_path = row_exec.get("TemplateURL")
        look_configs_json = row_exec.get("LookConfigsJSON")
        all_schemas = json.loads(row_exec.get("BaseQuerySchemaJSON") or '{}')
        parsed_calculation_row_configs = [CalculationRowConfig(**item) for item in json.loads(row_exec.get("CalculationRowConfigsJSON") or '[]')]
        parsed_filter_configs = json.loads(row_exec.get("FilterConfigsJSON") or '[]')

        if not data_tables_json or not html_template_gcs_path:
            raise HTTPException(status_code=404, detail="Report definition is incomplete. Missing Data Tables or Template URL.")

        data_tables_configs = [DataTableConfig(**dt) for dt in json.loads(data_tables_json)]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching or parsing report definition '{report_definition_name}': {str(e)}")

    try:
        path_parts = html_template_gcs_path.replace("gs://", "").split("/", 1)
        blob = gcs_client.bucket(path_parts[0]).blob(path_parts[1])
        populated_html = blob.download_as_text(encoding='utf-8') if blob.exists() else f"<html><body>Template not found at {html_template_gcs_path}</body></html>"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load HTML template: {str(e)}")

    # --- 2. Build Filter Logic ---
    current_query_params_for_bq_exec = []; param_idx_exec = 0
    base_conditions = []
    try:
        looker_filters_payload_exec = json.loads(filter_criteria_json_str or "{}")
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON for filter_criteria: {str(e)}")
    
    for filter_key, val_str_list in looker_filters_payload_exec.get("dynamic_filters", {}).items():
        bq_col, op_conf = None, None
        for sfx_key_iter_dyn in sorted(ALLOWED_FILTER_OPERATORS.keys(), key=len, reverse=True):
            if filter_key.endswith(sfx_key_iter_dyn):
                bq_col, op_conf = filter_key[:-len(sfx_key_iter_dyn)], ALLOWED_FILTER_OPERATORS[sfx_key_iter_dyn]
                break
        if bq_col and op_conf:
            try:
                p_name = f"df_p_{param_idx_exec}"; param_idx_exec += 1
                bq_type, typed_val = get_bq_param_type_and_value(str(val_str_list), bq_col, op_conf["param_type_hint"])
                if op_conf["param_type_hint"] != "NONE":
                    base_conditions.append({'col': bq_col, 'op': op_conf['op'], 'p_name': p_name})
                    current_query_params_for_bq_exec.append(ScalarQueryParameter(p_name, bq_type, typed_val))
                else:
                    base_conditions.append({'col': bq_col, 'op': op_conf['op'], 'p_name': None})
            except ValueError as ve: print(f"WARN: Skipping Dyn filter '{bq_col}': {ve}")

    # --- 3. Loop through each Data Table to execute and render ---
    for table_idx, table_config in enumerate(data_tables_configs):
        table_placeholder_name = table_config.table_placeholder_name
        base_sql_query = table_config.sql_query
        field_configs_list = table_config.field_display_configs
        
        if not table_placeholder_name or not base_sql_query: continue
            
        field_configs_map = {fc.field_name: fc for fc in field_configs_list}
        schema_for_table = all_schemas.get(table_placeholder_name, [])
        schema_type_map = {f['name']: f['type'] for f in schema_for_table}
        body_field_names_in_order = [f['name'] for f in schema_for_table if (field_configs_map.get(f['name']) or FieldDisplayConfig(field_name=f['name'])).include_in_body]
        
        final_sql = base_sql_query
        table_conditions = []
        for cond in base_conditions:
            if cond['col'] in schema_type_map:
                if cond['p_name']: table_conditions.append(f"`{cond['col']}` {cond['op']} @{cond['p_name']}")
                else: table_conditions.append(f"`{cond['col']}` {cond['op']}")
        
        if table_conditions:
            conditions_sql_segment = " AND ".join(table_conditions)
            if " where " in final_sql.lower(): final_sql += f" AND ({conditions_sql_segment})"
            else: final_sql = f"SELECT * FROM ({final_sql}) AS GenAIReportSubquery WHERE {conditions_sql_segment}"

        try:
            print(f"INFO: Executing BQ Query for table '{table_placeholder_name}':\n{final_sql}")
            job_cfg_exec = bigquery.QueryJobConfig(query_parameters=current_query_params_for_bq_exec)
            query_job = bq_client.query(final_sql, job_config=job_cfg_exec)
            data_rows_list = [convert_row_to_json_serializable(row) for row in query_job.result()] if query_job else []
        except Exception as e:
            print(f"ERROR: BQ execution for table '{table_placeholder_name}': {str(e)}")
            data_rows_list = []

        table_rows_html_str = ""
        group_by_field = next((fc.field_name for fc in field_configs_list if fc.group_summary_action in ['SUBTOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL']), None)
        agg_fields = {fc.field_name: fc.numeric_aggregation for fc in field_configs_list if fc.numeric_aggregation and schema_type_map.get(fc.field_name) in NUMERIC_TYPES_FOR_AGG}
        grand_total_needed = any(fc.group_summary_action in ['GRAND_TOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL'] for fc in field_configs_list)
        grand_total_accumulators = {f: [] for f in agg_fields}
        
        if not data_rows_list:
            colspan = len(body_field_names_in_order) or 1
            table_rows_html_str = f"<tr><td colspan='{colspan}' style='text-align:center; padding: 20px;'>No data returned for this table.</td></tr>"
        else:
            current_group_val, subtotal_accumulators = None, {f: [] for f in agg_fields}

            for row_idx, row_data in enumerate(data_rows_list):
                is_first_row_of_group = False
                if group_by_field:
                    new_group_val = row_data.get(group_by_field)
                    is_first_row_of_group = current_group_val != new_group_val
                    if row_idx > 0 and is_first_row_of_group:
                        subtotal_html = f"<tr class='subtotal-row' style='font-weight: bold; background-color: #f2f2f2;'><td style='text-align: right;' colspan='{len(body_field_names_in_order) - len(agg_fields)}'>Subtotal for {current_group_val}:</td>"
                        for field_name in body_field_names_in_order:
                            if field_name in agg_fields:
                                result = calculate_aggregate([Decimal(v) for v in subtotal_accumulators[field_name]], agg_fields[field_name])
                                config = field_configs_map.get(field_name) or FieldDisplayConfig(field_name=field_name)
                                subtotal_html += f"<td style='text-align: {config.alignment or 'right'};'>{format_value(result, config.number_format, schema_type_map.get(field_name))}</td>"
                        subtotal_html += "</tr>"
                        table_rows_html_str += subtotal_html
                        subtotal_accumulators = {f: [] for f in agg_fields}
                    current_group_val = new_group_val
                
                for field, agg_type in agg_fields.items():
                    val = row_data.get(field)
                    if val is not None:
                        try:
                            dec_val = Decimal(str(val))
                            if group_by_field: subtotal_accumulators[field].append(dec_val)
                            if grand_total_needed: grand_total_accumulators[field].append(dec_val)
                        except InvalidOperation: pass

                row_html_item = "<tr>"
                for col_idx, header_key in enumerate(body_field_names_in_order):
                    field_config = field_configs_map.get(header_key) or FieldDisplayConfig(field_name=header_key)
                    cell_value = row_data.get(header_key)
                    formatted_val = format_value(cell_value, field_config.number_format, schema_type_map.get(header_key, "STRING"))
                    
                    if group_by_field and header_key == group_by_field and field_config.repeat_group_value == 'SHOW_ON_CHANGE' and not is_first_row_of_group:
                        formatted_val = ''

                    align_val = field_config.alignment or "left"
                    row_html_item += f"  <td style='text-align: {align_val};'>{formatted_val}</td>"
                row_html_item += "</tr>\n"
                table_rows_html_str += row_html_item

            if group_by_field and data_rows_list:
                subtotal_html = f"<tr class='subtotal-row' style='font-weight: bold; background-color: #f2f2f2;'><td style='text-align: right;' colspan='{len(body_field_names_in_order) - len(agg_fields)}'>Subtotal for {current_group_val}:</td>"
                for field_name in body_field_names_in_order:
                    if field_name in agg_fields:
                        result = calculate_aggregate([Decimal(v) for v in subtotal_accumulators[field_name]], agg_fields[field_name])
                        config = field_configs_map.get(field_name) or FieldDisplayConfig(field_name=field_name)
                        subtotal_html += f"<td style='text-align: {config.alignment or 'right'};'>{format_value(result, config.number_format, schema_type_map.get(field_name))}</td>"
                subtotal_html += "</tr>"
                table_rows_html_str += subtotal_html

            if grand_total_needed and data_rows_list:
                gt_html = f"<tr class='grand-total-row' style='font-weight: bold; border-top: 2px solid black; background-color: #e0e0e0;'><td style='text-align: right;' colspan='{len(body_field_names_in_order) - len(agg_fields)}'>Grand Total:</td>"
                for field_name in body_field_names_in_order:
                    if field_name in agg_fields:
                        result = calculate_aggregate([Decimal(v) for v in grand_total_accumulators[field_name]], agg_fields[field_name])
                        config = field_configs_map.get(field_name) or FieldDisplayConfig(field_name=field_name)
                        gt_html += f"<td style='text-align: {config.alignment or 'right'};'>{format_value(result, config.number_format, schema_type_map.get(field_name))}</td>"
                gt_html += "</tr>"
                table_rows_html_str += gt_html

            if table_idx == 0 and parsed_calculation_row_configs:
                for calc_config in parsed_calculation_row_configs:
                    placeholder_in_template_regex = r"\{\{\s*" + re.escape(calc_config.values_placeholder_name) + r"\s*\}\}"
                    if re.search(placeholder_in_template_regex, populated_html):
                        td_outputs = ""
                        for value_conf in calc_config.calculated_values:
                            data_to_agg = [Decimal(str(r.get(value_conf.target_field_name, 0))) for r in data_rows_list if r.get(value_conf.target_field_name) is not None]
                            agg_result = calculate_aggregate(data_to_agg, value_conf.calculation_type.value)
                            agg_html = format_value(agg_result, value_conf.number_format, schema_type_map.get(value_conf.target_field_name))
                            td_outputs += f"<td style='text-align: {value_conf.alignment or 'right'};'>{agg_html}</td>"
                        populated_html = re.sub(placeholder_in_template_regex, td_outputs, populated_html)

        placeholder_to_replace = f"{{{{TABLE_ROWS_{table_placeholder_name}}}}}"
        populated_html = populated_html.replace(placeholder_to_replace, table_rows_html_str)

    # --- 4. Process Looks and Finalize Report ---
# --- 4. Process Looks and Finalize Report ---
    if look_configs_json:
        look_configs = json.loads(look_configs_json)
        user_filter_values = looker_filters_payload_exec.get("dynamic_filters", {})

        for look_config in look_configs:
            placeholder_to_replace = f"{{{{{look_config['placeholder_name']}}}}}"
            look_filters_for_sdk = {}
            for fc in parsed_filter_configs:
                ui_key = fc.get('ui_filter_key')
                if ui_key in user_filter_values:
                    for target in fc.get('targets', []):
                        if target.get('target_type') == 'LOOK' and str(target.get('target_id')) == str(look_config['look_id']):
                            look_filter_name = target.get('target_field_name')
                            filter_value = user_filter_values[ui_key]
                            if look_filter_name and filter_value is not None:
                                look_filters_for_sdk[look_filter_name] = str(filter_value)
            
            try:
                print(f"INFO: Rendering Look ID {look_config['look_id']} with new filters: {look_filters_for_sdk}")

                look = looker_sdk.look(look_id=str(look_config['look_id']))
                if not look or not look.query:
                    raise Exception(f"Look {look_config['look_id']} or its query could not be fetched.")

                new_query = look.query
                if not new_query.filters:
                    new_query.filters = {}
                
                for f_key, f_val in look_filters_for_sdk.items():
                    new_query.filters[f_key] = f_val

                image_bytes = looker_sdk.run_inline_query(
                    result_format="png",
                    body=models40.WriteQuery(
                        model=new_query.model,
                        view=new_query.view,
                        fields=new_query.fields,
                        pivots=new_query.pivots,
                        filters=new_query.filters,
                        sorts=new_query.sorts,
                        limit=new_query.limit,
                        # --- FIX IS HERE: Add the vis_config to the request body ---
                        vis_config=new_query.vis_config
                    )
                )
                base64_image = base64.b64encode(image_bytes).decode('utf-8')
                image_src_data_uri = f"data:image/png;base64,{base64_image}"
                populated_html = populated_html.replace(placeholder_to_replace, image_src_data_uri)

            except Exception as e:
                print(f"ERROR: Failed to render Look {look_config['look_id']}: {e}")
                populated_html = populated_html.replace(placeholder_to_replace, f"Error rendering chart: {e}")
    # --- Final GCS Upload block with enhanced debugging ---
    try:
        report_id = str(uuid.uuid4())
        output_gcs_blob_name = f"{config.GCS_GENERATED_REPORTS_PREFIX}{report_id}.html"
        
        print(f"DEBUG: Attempting to save report to gs://{config.GCS_BUCKET_NAME}/{output_gcs_blob_name}")

        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob_out = bucket.blob(output_gcs_blob_name)
        blob_out.upload_from_string(populated_html, content_type='text/html; charset=utf-8')
        
        print(f"INFO: Successfully generated and saved report.")
    
    except Exception as e:
        print("---! FATAL EXCEPTION DURING GCS UPLOAD !---")
        import traceback
        traceback.print_exc() 
        print("---! END OF EXCEPTION INFO !---")
        
        print(f"FATAL: Could not upload final report to GCS. Error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save final report to GCS: {str(e)}")
        
    report_url_path = f"/view_generated_report/{report_id}"
    return JSONResponse(content={"report_url_path": report_url_path})
@app.get("/view_generated_report/{report_id}", response_class=HTMLResponse)
async def view_generated_report_endpoint(
    report_id: str, gcs_client: storage.Client = Depends(get_storage_client_dep)
):
    generated_report_gcs_blob_name = f"{config.GCS_GENERATED_REPORTS_PREFIX}{report_id}.html"
    html_content: Optional[str] = None
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob_in = bucket.blob(generated_report_gcs_blob_name)
        if blob_in.exists():
            html_content = blob_in.download_as_text(encoding='utf-8')
        else: raise HTTPException(status_code=404, detail="Report not found or has expired.")
    except GCSNotFound: raise HTTPException(status_code=404, detail="Report not found (GCS).")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to retrieve report: {str(e)}")
    if not html_content: raise HTTPException(status_code=404, detail="Report content is empty.")
    headers = {"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0"}
    return HTMLResponse(content=html_content, headers=headers)


if __name__ == "__main__":
    print("INFO: Starting Uvicorn server for GenAI Report API.")
    default_port = int(os.getenv("PORT", "8080"))
    reload_flag = os.getenv("PYTHON_ENV", "development").lower() == "development"
    uvicorn.run("app:app", host="0.0.0.0", port=default_port, reload=reload_flag)
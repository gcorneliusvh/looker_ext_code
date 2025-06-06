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
from fastapi import (FastAPI, Depends, HTTPException, Query, Request, Body)
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

import looker_sdk  # <-- THIS LINE IS MOST LIKELY MISSING

# NEW: Import Looker SDK
from looker_sdk import methods40, models40

# --- AppConfig & Global Configs ---
class AppConfig:
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcp_location: str = os.getenv("GCP_LOCATION", "")
    default_system_instruction_text: str = ""
    vertex_ai_initialized: bool = False
    bigquery_client: Union[bigquery.Client, None] = None
    storage_client: Union[storage.Client, None] = None
    # NEW: Add Looker SDK client to config
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
# NEW: Add LookConfig
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
class FieldDisplayConfig(BaseModel):
    field_name: str; include_in_body: bool = Field(default=True); include_at_top: bool = Field(default=False)
    include_in_header: bool = Field(default=False); context_note: Optional[str] = None
    alignment: Optional[str] = None; number_format: Optional[str] = None
    group_summary_action: Optional[str] = None; repeat_group_value: Optional[str] = Field(default='REPEAT')
    numeric_aggregation: Optional[str] = None
class ReportDefinitionPayload(BaseModel):
    report_name: str; image_url: str; sql_query: str; prompt: str
    # NEW: Add look_configs
    look_configs: Optional[List[LookConfig]] = None
    field_display_configs: Optional[List[FieldDisplayConfig]] = None
    user_attribute_mappings: Optional[Dict[str, str]] = Field(default_factory=dict)
    calculation_row_configs: Optional[List[CalculationRowConfig]] = None
    subtotal_configs: Optional[List[SubtotalConfig]] = Field(default_factory=list)
    optimized_prompt: Optional[str] = None; header_text: Optional[str] = None; footer_text: Optional[str] = None
class ExecuteReportPayload(BaseModel):
    report_definition_name: str; filter_criteria_json: str = Field(default="{}")
class ReportDefinitionListItem(BaseModel):
    ReportName: str; Prompt: Optional[str] = None; SQL: Optional[str] = None; ScreenshotURL: Optional[str] = None
    # NEW: Add LookConfigsJSON
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
    config.gcp_project_id = os.getenv("GCP_PROJECT_ID", config.gcp_project_id)
    config.gcp_location = os.getenv("GCP_LOCATION", config.gcp_location)
    config.GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", config.GCS_BUCKET_NAME)
    config.GCS_SYSTEM_INSTRUCTION_PATH = os.getenv("GCS_SYSTEM_INSTRUCTION_PATH", config.GCS_SYSTEM_INSTRUCTION_PATH)
    config.TARGET_GEMINI_MODEL = os.getenv("GEMINI_MODEL_OVERRIDE", config.TARGET_GEMINI_MODEL)
    if not config.TARGET_GEMINI_MODEL:
        config.TARGET_GEMINI_MODEL = "gemini-2.5-pro-preview-05-06"
    print(f"INFO: Target Gemini Model: {config.TARGET_GEMINI_MODEL}")
    if not config.gcp_project_id: print("ERROR: GCP_PROJECT_ID environment variable not set.")
    if not config.gcp_location: print("ERROR: GCP_LOCATION environment variable not set.")
    print(f"INFO: Target GCS Bucket: {config.GCS_BUCKET_NAME or 'NOT SET'}")
    print(f"INFO: System Instruction GCS Path: gs://{config.GCS_BUCKET_NAME}/{config.GCS_SYSTEM_INSTRUCTION_PATH}")

    if not config.GCS_BUCKET_NAME:
        print("ERROR: GCS_BUCKET_NAME environment variable not set. GCS operations may fail.")
        config.storage_client = None
    elif storage:
        try:
            config.storage_client = storage.Client(project=config.gcp_project_id if config.gcp_project_id else None)
            print("INFO: Google Cloud Storage Client initialized successfully.")
        except Exception as e:
            print(f"FATAL: Failed to initialize Google Cloud Storage Client: {e}")
            config.storage_client = None
    else:
        print("ERROR: google.cloud.storage module not available.")
        config.storage_client = None

    if config.storage_client:
        config.default_system_instruction_text = _load_system_instruction_from_gcs(config.storage_client, config.GCS_BUCKET_NAME, config.GCS_SYSTEM_INSTRUCTION_PATH)
    else:
        print("INFO: Using default system instruction due to missing GCS client or bucket name.")
        config.default_system_instruction_text = DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    
    if config.default_system_instruction_text:
        print(f"INFO: System instruction loaded (length: {len(config.default_system_instruction_text)} chars).")
    else:
        print("ERROR: System instruction is empty after attempting to load.")

    if config.gcp_project_id and config.gcp_location:
        try:
            vertexai.init(project=config.gcp_project_id, location=config.gcp_location)
            config.vertex_ai_initialized = True
            print("INFO: Vertex AI SDK initialized successfully.")
        except Exception as e:
            print(f"FATAL: Vertex AI SDK Initialization Error: {e}")
            config.vertex_ai_initialized = False
    else:
        print("ERROR: Vertex AI SDK prerequisites (GCP_PROJECT_ID, GCP_LOCATION) not met.")
        config.vertex_ai_initialized = False

    if bigquery and config.gcp_project_id:
        try:
            config.bigquery_client = bigquery.Client(project=config.gcp_project_id)
            print("INFO: BigQuery Client initialized successfully.")
        except Exception as e:
            print(f"FATAL: Failed to initialize BigQuery Client: {e}")
            config.bigquery_client = None
    else:
        print(f"ERROR: BigQuery prerequisites not met.")
        config.bigquery_client = None
        
    # NEW: Add Looker SDK initialization block
    try:
        print("INFO: Initializing Looker SDK from standard environment variables...")
        
        # The SDK will find LOOKERSDK_BASE_URL, LOOKERSDK_CLIENT_ID, etc.
        # on its own. We don't need to manually check for them.
        config.looker_sdk_client = looker_sdk.init40()
        
        print("INFO: Looker SDK initialized successfully.")
    except Exception as e:
        print(f"FATAL: Looker SDK auto-initialization from environment failed: {e}")
        config.looker_sdk_client = None

    yield
    print("INFO: FastAPI application shutdown.")

app = FastAPI(lifespan=lifespan)

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

# NEW: Add Looker SDK dependency getter
def get_looker_sdk_client_dep():
    if not config.looker_sdk_client:
        raise HTTPException(status_code=503, detail="Looker SDK client not available.")
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

# --- API Endpoints ---
@app.get("/")
async def read_root(request: Request):
    return {"status": f"GenAI Report API is running! (Target Model: {config.TARGET_GEMINI_MODEL})"}

@app.post("/dry_run_sql_for_schema")
async def dry_run_sql_for_schema_endpoint(
    request: Request, payload: SqlQueryPayload, bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
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
    report_name: str, request: Request,
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

@app.post("/report_definitions", status_code=201)
async def upsert_report_definition(
    payload: ReportDefinitionPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    _vertex_ai_init_check: None = Depends(get_vertex_ai_initialized_flag)
):
    report_name = payload.report_name; base_sql_query = payload.sql_query; base_user_prompt = payload.prompt
    image_url = payload.image_url; field_display_configs_from_payload = payload.field_display_configs
    calculation_row_configs_from_payload = payload.calculation_row_configs
    user_attribute_mappings_json_str = json.dumps(payload.user_attribute_mappings or {})
    print(f"INFO: Upserting report definition for: '{report_name}'")
    schema_from_dry_run_list = []; schema_map_for_prompt = {}
    try:
        dry_run_job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_job = bq_client.query(base_sql_query, job_config=dry_run_job_config)
        if dry_run_job.schema:
            for field in dry_run_job.schema:
                field_data = {"name": field.name, "type": str(field.field_type).upper(), "mode": str(field.mode).upper()}
                schema_from_dry_run_list.append(field_data); schema_map_for_prompt[field.name] = field_data["type"]
            schema_for_gemini_prompt_str = "Schema: " + ", ".join([f"`{f['name']}` (Type: {f['type']})" for f in schema_from_dry_run_list])
        else: schema_for_gemini_prompt_str = "Schema: Not determined."
    except Exception as e: raise HTTPException(status_code=400, detail=f"Base SQL query dry run failed: {str(e)}")

    effective_field_display_configs = [FieldDisplayConfig(**fc.model_dump(exclude_unset=True)) for fc in field_display_configs_from_payload] if field_display_configs_from_payload else [FieldDisplayConfig(field_name=f["name"]) for f in schema_from_dry_run_list]
    
    prompt_for_template = base_user_prompt
    prompt_for_template += f"\n\n--- Data Schema ---\n{schema_for_gemini_prompt_str}\n--- End Data Schema ---"
    if effective_field_display_configs:
        prompt_for_template += "\n\n--- Field Display & Summary Instructions ---"
        body_fields_prompt_parts, top_fields_prompt_parts, header_fields_prompt_parts = [], [], []
        for config_item in effective_field_display_configs:
            field_type = schema_map_for_prompt.get(config_item.field_name, "UNKNOWN").upper()
            is_numeric_field = field_type in NUMERIC_TYPES_FOR_AGG; is_string_field = field_type == "STRING"
            style_hints = [s for s in [f"align: {config_item.alignment}" if config_item.alignment else "", f"format: {config_item.number_format}" if config_item.number_format else ""] if s]
            field_info = f"- `{config_item.field_name}`"
            if style_hints: field_info += f" (Styling: {'; '.join(style_hints)})"
            if is_string_field and config_item.group_summary_action: field_info += f" (Group Summary: {config_item.group_summary_action})"
            if is_string_field and config_item.group_summary_action and config_item.repeat_group_value: field_info += f" (Repeat: {config_item.repeat_group_value})"
            if is_numeric_field and config_item.numeric_aggregation: field_info += f" (Numeric Agg: {config_item.numeric_aggregation})"
            if config_item.context_note: field_info += f" (Context: {config_item.context_note})"
            if config_item.include_in_body: body_fields_prompt_parts.append(field_info)
            if config_item.include_at_top: top_fields_prompt_parts.append(field_info + f" -> Use placeholder: {{TOP_{config_item.field_name}}}")
            if config_item.include_in_header: header_fields_prompt_parts.append(field_info + f" -> Use placeholder: {{HEADER_{config_item.field_name}}}")
        if body_fields_prompt_parts: prompt_for_template += "\nBody Fields:\n" + "\n".join(body_fields_prompt_parts)
        if top_fields_prompt_parts: prompt_for_template += "\nTop Fields:\n" + "\n".join(top_fields_prompt_parts)
        if header_fields_prompt_parts: prompt_for_template += "\nHeader Fields:\n" + "\n".join(header_fields_prompt_parts)
        prompt_for_template += "\n--- End Field Instructions ---"

    # NEW: Add Look placeholders to prompt
    if payload.look_configs:
        prompt_for_template += "\n\n--- Chart Image Placeholders ---\nPlease include placeholders for the following charts where you see fit in the layout. Use these exact placeholder names:\n"
        for look_config in payload.look_configs:
            prompt_for_template += f"- `{{{{{look_config.placeholder_name}}}}}`\n"
        prompt_for_template += "--- End Chart Image Placeholders ---"

    if calculation_row_configs_from_payload:
        prompt_for_template += "\n\n--- Explicit Overall Calculation Rows ---"
        for i, calc_row_config in enumerate(calculation_row_configs_from_payload):
            value_descs = [f"{cv.calculation_type.value} of '{cv.target_field_name}'" for cv in calc_row_config.calculated_values]
            prompt_for_template += f"\n- Row {i+1}: Label \"{calc_row_config.row_label}\", Placeholder `{{{{{calc_row_config.values_placeholder_name}}}}}` for: {'; '.join(value_descs)}."
        prompt_for_template += "\n--- End Explicit Calculation Rows ---"
    prompt_for_template += """\n\n--- HTML Template Generation Guidelines (Final Reminder) ---\n1. Output ONLY the raw HTML code. No descriptions, no explanations, no markdown like ```html ... ```.\n2. Start with `<!DOCTYPE html>` or `<html>` and end with `</html>`.\n3. CRITICAL: ALL placeholders for dynamic data MUST use double curly braces, e.g., `{{YourPlaceholderKey}}`. Single braces (e.g., {YourPlaceholderKey}) are NOT PERMITTED and will not be processed."""

    try:
        async with httpx.AsyncClient(timeout=180.0) as client_httpx:
            img_response = await client_httpx.get(image_url); img_response.raise_for_status()
            image_bytes_data = await img_response.aread()
            image_mime_type_data = img_response.headers.get("Content-Type", "application/octet-stream").lower()
            if not image_mime_type_data.startswith("image/"): raise ValueError(f"Content-Type from URL is not valid.")
    except Exception as e: raise HTTPException(status_code=400, detail=f"Error fetching image URL '{image_url}': {str(e)}")

    html_template_content = generate_html_from_user_pattern(prompt_text=prompt_for_template, image_bytes=image_bytes_data, image_mime_type=image_mime_type_data, system_instruction_text=config.default_system_instruction_text)
    if not html_template_content or not html_template_content.strip():
        html_template_content = "<html><body><p>Error: AI failed to generate valid HTML. Placeholders may be missing.</p></body></html>"

    current_version = 1
    report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower()
    base_gcs_folder = f"report_templates/{report_gcs_path_safe}"
    template_gcs_filename = f"template_v{current_version}.html"
    versioned_template_gcs_path_str = f"{base_gcs_folder}/{template_gcs_filename}"
    
    base_sql_gcs_path_str = f"{base_gcs_folder}/base_query.sql"
    schema_gcs_path_str = f"{base_gcs_folder}/schema.json"
    field_configs_gcs_path_str = f"{base_gcs_folder}/field_display_configs.json"
    calc_row_configs_gcs_path_str = f"{base_gcs_folder}/calculation_row_configs.json"
    subtotal_configs_gcs_path_str = f"{base_gcs_folder}/subtotal_configs.json"
    user_placeholder_mappings_gcs_path_str = f"{base_gcs_folder}/user_placeholder_mappings.json"

    schema_json_to_save = json.dumps(schema_from_dry_run_list, indent=2)
    field_display_configs_json_to_save = json.dumps([fc.model_dump(exclude_unset=True) for fc in effective_field_display_configs], indent=2) if effective_field_display_configs else "[]"
    calculation_row_configs_json_to_save = json.dumps([crc.model_dump(exclude_unset=True) for crc in payload.calculation_row_configs], indent=2) if payload.calculation_row_configs else "[]"
    subtotal_configs_json_to_save = json.dumps(payload.subtotal_configs or [], indent=2)
    user_placeholder_mappings_json_to_save = "[]"
    # NEW: Save look configs
    look_configs_json_to_save = json.dumps([lc.model_dump() for lc in payload.look_configs], indent=2) if payload.look_configs else "[]"

    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        bucket.blob(versioned_template_gcs_path_str).upload_from_string(html_template_content, content_type='text/html; charset=utf-8')
        bucket.blob(base_sql_gcs_path_str).upload_from_string(payload.sql_query, content_type='application/sql; charset=utf-8')
        bucket.blob(schema_gcs_path_str).upload_from_string(schema_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(field_configs_gcs_path_str).upload_from_string(field_display_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(calc_row_configs_gcs_path_str).upload_from_string(calculation_row_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(subtotal_configs_gcs_path_str).upload_from_string(subtotal_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(user_placeholder_mappings_gcs_path_str).upload_from_string(user_placeholder_mappings_json_to_save, content_type='application/json; charset=utf-8')
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to save files to GCS: {e}")

    table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
    merge_sql = f"""
    MERGE {table_id} T USING (
        SELECT @report_name AS ReportName, @prompt AS Prompt, @sql_query AS SQL, @image_url AS ScreenshotURL,
                @look_configs_json AS LookConfigsJSON,
                @template_gcs_path AS TemplateURL, @latest_template_version AS LatestTemplateVersion,
                @base_sql_gcs_path AS BaseQueryGCSPath, @schema_gcs_path AS SchemaGCSPath,
                @schema_json AS BaseQuerySchemaJSON, @field_display_configs_json AS FieldDisplayConfigsJSON,
                @calculation_row_configs_json AS CalculationRowConfigsJSON,
                @subtotal_configs_json AS SubtotalConfigsJSON, @user_placeholder_mappings_json AS UserPlaceholderMappingsJSON,
                @user_attribute_mappings_json AS UserAttributeMappingsJSON,
                @optimized_prompt AS OptimizedPrompt, @header_text AS Header, @footer_text AS Footer,
                CURRENT_TIMESTAMP() AS CurrentTs
    ) S ON T.ReportName = S.ReportName
    WHEN MATCHED THEN UPDATE SET
            Prompt = S.Prompt, SQL = S.SQL, ScreenshotURL = S.ScreenshotURL, LookConfigsJSON = S.LookConfigsJSON,
            TemplateURL = S.TemplateURL, LatestTemplateVersion = S.LatestTemplateVersion,
            BaseQueryGCSPath = S.BaseQueryGCSPath, SchemaGCSPath = S.SchemaGCSPath,
            BaseQuerySchemaJSON = S.BaseQuerySchemaJSON, FieldDisplayConfigsJSON = S.FieldDisplayConfigsJSON,
            CalculationRowConfigsJSON = S.CalculationRowConfigsJSON, SubtotalConfigsJSON = S.SubtotalConfigsJSON,
            UserPlaceholderMappingsJSON = S.UserPlaceholderMappingsJSON, UserAttributeMappingsJSON = S.UserAttributeMappingsJSON,
            OptimizedPrompt = S.OptimizedPrompt, Header = S.Header, Footer = S.Footer, LastGeneratedTimestamp = S.CurrentTs
    WHEN NOT MATCHED THEN INSERT (
            ReportName, Prompt, SQL, ScreenshotURL, LookConfigsJSON,
            TemplateURL, LatestTemplateVersion, BaseQueryGCSPath, SchemaGCSPath, 
            BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON, 
            UserPlaceholderMappingsJSON, UserAttributeMappingsJSON, OptimizedPrompt, Header, Footer, 
            CreatedTimestamp, LastGeneratedTimestamp
    ) VALUES (
            S.ReportName, S.Prompt, S.SQL, S.ScreenshotURL, S.LookConfigsJSON,
            S.TemplateURL, S.LatestTemplateVersion, S.BaseQueryGCSPath, S.SchemaGCSPath, 
            S.BaseQuerySchemaJSON, S.FieldDisplayConfigsJSON, S.CalculationRowConfigsJSON, S.SubtotalConfigsJSON, 
            S.UserPlaceholderMappingsJSON, S.UserAttributeMappingsJSON, S.OptimizedPrompt, S.Header, S.Footer, 
            S.CurrentTs, S.CurrentTs
    )"""
    merge_params = [
        ScalarQueryParameter("report_name", "STRING", payload.report_name),
        ScalarQueryParameter("prompt", "STRING", payload.prompt),
        ScalarQueryParameter("sql_query", "STRING", payload.sql_query),
        ScalarQueryParameter("image_url", "STRING", payload.image_url),
        ScalarQueryParameter("look_configs_json", "STRING", look_configs_json_to_save),
        ScalarQueryParameter("template_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{versioned_template_gcs_path_str}"),
        ScalarQueryParameter("latest_template_version", "INT64", current_version),
        ScalarQueryParameter("base_sql_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{base_sql_gcs_path_str}"),
        ScalarQueryParameter("schema_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{schema_gcs_path_str}"),
        ScalarQueryParameter("schema_json", "STRING", schema_json_to_save),
        ScalarQueryParameter("field_display_configs_json", "STRING", field_display_configs_json_to_save),
        ScalarQueryParameter("calculation_row_configs_json", "STRING", calculation_row_configs_json_to_save),
        ScalarQueryParameter("subtotal_configs_json", "STRING", subtotal_configs_json_to_save),
        ScalarQueryParameter("user_placeholder_mappings_json", "STRING", user_placeholder_mappings_json_to_save),
        ScalarQueryParameter("user_attribute_mappings_json", "STRING", user_attribute_mappings_json_str),
        ScalarQueryParameter("optimized_prompt", "STRING", payload.optimized_prompt),
        ScalarQueryParameter("header_text", "STRING", payload.header_text),
        ScalarQueryParameter("footer_text", "STRING", payload.footer_text),
    ]
    try:
        job = bq_client.query(merge_sql, job_config=bigquery.QueryJobConfig(query_parameters=merge_params)); job.result()
    except Exception as e:
        error_message = f"Failed to save report definition to BigQuery: {str(e)}"; bq_errors = [f"Reason: {err.get('reason', 'N/A')}, Message: {err.get('message', 'N/A')}" for err in getattr(e, 'errors', [])]; error_message += f" BigQuery Errors: {'; '.join(bq_errors)}" if bq_errors else ""
        raise HTTPException(status_code=500, detail=error_message)
    return {"message": f"Report definition '{report_name}' upserted, template v{current_version} created.", "template_html_gcs_path": f"gs://{config.GCS_BUCKET_NAME}/{versioned_template_gcs_path_str}"}

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
    # NEW: Add Looker SDK dependency
    looker_sdk: methods40.Looker40SDK = Depends(get_looker_sdk_client_dep)
):
    report_definition_name = payload.report_definition_name
    filter_criteria_json_str = payload.filter_criteria_json
    print(f"INFO: POST /execute_report for '{report_definition_name}'. Filters JSON: {filter_criteria_json_str}")

    query_def_sql_exec = f"""
        SELECT SQL, TemplateURL, UserAttributeMappingsJSON, BaseQuerySchemaJSON,
                FieldDisplayConfigsJSON, CalculationRowConfigsJSON, UserPlaceholderMappingsJSON,
                LookConfigsJSON
        FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param
    """
    def_params_exec = [ScalarQueryParameter("report_name_param", "STRING", report_definition_name)]
    try:
        results_exec = list(bq_client.query(query_def_sql_exec, job_config=bigquery.QueryJobConfig(query_parameters=def_params_exec)).result())
        if not results_exec: raise HTTPException(status_code=404, detail=f"Report definition '{report_definition_name}' not found.")
        row_exec = results_exec[0]
        base_sql_query_from_db = row_exec.get("SQL"); html_template_gcs_path = row_exec.get("TemplateURL")
        user_attr_map_json = row_exec.get("UserAttributeMappingsJSON"); bq_schema_json = row_exec.get("BaseQuerySchemaJSON")
        field_configs_json = row_exec.get("FieldDisplayConfigsJSON"); calculation_row_configs_json = row_exec.get("CalculationRowConfigsJSON")
        user_placeholder_mappings_json = row_exec.get("UserPlaceholderMappingsJSON")
        # NEW: Fetch LookConfigsJSON
        look_configs_json = row_exec.get("LookConfigsJSON")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching report definition '{report_definition_name}': {str(e)}")

    user_mappings_dict: Dict[str, PlaceholderUserMapping] = {}
    if user_placeholder_mappings_json:
        try:
            mappings_list = json.loads(user_placeholder_mappings_json)
            for mapping_data in mappings_list:
                if isinstance(mapping_data, dict) and 'original_tag' in mapping_data:
                    user_mappings_dict[mapping_data['original_tag']] = PlaceholderUserMapping(**mapping_data)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"WARN: Could not parse UserPlaceholderMappingsJSON for '{report_definition_name}': {e}")

    schema_type_map: Dict[str, str] = {}; schema_fields_ordered: List[str] = []
    if bq_schema_json:
        try:
            parsed_schema = json.loads(bq_schema_json)
            for field_info in parsed_schema:
                if 'name' in field_info and 'type' in field_info: schema_type_map[field_info['name']] = str(field_info['type']).upper(); schema_fields_ordered.append(field_info['name'])
        except json.JSONDecodeError: print(f"WARNING: Could not parse BaseQuerySchemaJSON for '{report_definition_name}'.")

    field_configs_map: Dict[str, FieldDisplayConfig] = {}
    if field_configs_json:
        try:
            parsed_field_configs = json.loads(field_configs_json)
            for item_dict in parsed_field_configs:
                if 'subtotal_action' in item_dict and 'group_summary_action' not in item_dict: item_dict['group_summary_action'] = item_dict.pop('subtotal_action')
                item_dict.setdefault('group_summary_action', None); item_dict.setdefault('repeat_group_value', 'REPEAT'); item_dict.setdefault('numeric_aggregation', None)
                item_dict.setdefault('include_in_body', True); item_dict.setdefault('include_at_top', False); item_dict.setdefault('include_in_header', False)
                try: field_configs_map[item_dict['field_name']] = FieldDisplayConfig(**item_dict)
                except Exception as p_error: print(f"ERROR: Pydantic validation for field config item: {item_dict}. Error: {p_error}")
        except (json.JSONDecodeError, TypeError) as e: print(f"WARNING: Could not parse FieldDisplayConfigsJSON for '{report_definition_name}': {e}.")
    
    parsed_calculation_row_configs: List[CalculationRowConfig] = []
    if calculation_row_configs_json:
        try: parsed_calculation_row_configs = [CalculationRowConfig(**item) for item in json.loads(calculation_row_configs_json)]
        except Exception as e: print(f"WARN: Could not parse CalculationRowConfigsJSON for '{report_definition_name}': {e}")

    current_query_params_for_bq_exec = []; current_conditions_exec = []; applied_filter_values_for_template_exec = {}; param_idx_exec = 0
    try: looker_filters_payload_exec = json.loads(filter_criteria_json_str or "{}")
    except json.JSONDecodeError as e: raise HTTPException(status_code=400, detail=f"Invalid JSON for filter_criteria: {str(e)}")
    parsed_user_attribute_mappings_exec: Dict[str, str] = json.loads(user_attr_map_json or '{}')
    for fe_key, val_str in looker_filters_payload_exec.get("user_attributes", {}).items():
        bq_col = parsed_user_attribute_mappings_exec.get(fe_key)
        if bq_col:
            p_name = f"ua_p_{param_idx_exec}"; param_idx_exec += 1
            try:
                bq_type, typed_val = get_bq_param_type_and_value(val_str, bq_col, "AUTO")
                current_conditions_exec.append(f"`{bq_col}` = @{p_name}")
                current_query_params_for_bq_exec.append(ScalarQueryParameter(p_name, bq_type, typed_val))
                fc_filter = field_configs_map.get(bq_col); applied_filter_values_for_template_exec[bq_col] = format_value(typed_val, fc_filter.number_format if fc_filter else None, schema_type_map.get(bq_col, "STRING"))
            except ValueError as ve: print(f"WARN: Skipping UA filter '{bq_col}': {ve}")

    for filter_key, val_str_list in looker_filters_payload_exec.get("dynamic_filters", {}).items():
        bq_col, op_conf, op_sfx = None, None, None
        for sfx_key_iter_dyn in sorted(ALLOWED_FILTER_OPERATORS.keys(), key=len, reverse=True):
            if filter_key.endswith(sfx_key_iter_dyn): bq_col, op_conf, op_sfx = filter_key[:-len(sfx_key_iter_dyn)], ALLOWED_FILTER_OPERATORS[sfx_key_iter_dyn], sfx_key_iter_dyn; break
        if bq_col and op_conf and op_sfx:
            try:
                if op_conf["param_type_hint"] == "NONE": current_conditions_exec.append(f"`{bq_col}` {op_conf['op']}")
                else:
                    p_name = f"df_p_{param_idx_exec}"; param_idx_exec += 1; _val_str_dyn = str(val_str_list)
                    bq_type_rng, val_rng = get_bq_param_type_and_value(_val_str_dyn, bq_col, op_conf["param_type_hint"])
                    if op_sfx == "_between":
                        if isinstance(val_rng, tuple) and len(val_rng) == 2:
                            v1, v2 = val_rng; p1_n, p2_n = f"{p_name}_s", f"{p_name}_e"; act_t = bq_type_rng.split('_RANGE')[0]
                            current_conditions_exec.append(f"`{bq_col}` BETWEEN @{p1_n} AND @{p2_n}")
                            current_query_params_for_bq_exec.extend([ScalarQueryParameter(p1_n, act_t, v1), ScalarQueryParameter(p2_n, act_t, v2)])
                    elif op_conf["op"] == "IN":
                        if isinstance(val_rng, list) and val_rng:
                            el_type = bq_type_rng; current_conditions_exec.append(f"`{bq_col}` IN UNNEST(@{p_name})")
                            current_query_params_for_bq_exec.append(ArrayQueryParameter(p_name, el_type, val_rng))
                    else:
                        current_conditions_exec.append(f"`{bq_col}` {op_conf['op']} @{p_name}")
                        current_query_params_for_bq_exec.append(ScalarQueryParameter(p_name, bq_type_rng, val_rng))
            except ValueError as ve: print(f"WARN: Skipping Dyn filter '{bq_col}': {ve}")

    final_sql = base_sql_query_from_db.strip().rstrip(';');
    if current_conditions_exec:
        conditions_sql_segment = " AND ".join(current_conditions_exec)
        if " where " in final_sql.lower(): final_sql = f"{final_sql} AND ({conditions_sql_segment})"
        else: final_sql = f"SELECT * FROM ({final_sql}) AS GenAIReportSubquery WHERE {conditions_sql_segment}"

    subtotal_trigger_fields = [f_n for f_n, f_c in field_configs_map.items() if f_c.group_summary_action in ['SUBTOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL'] and f_n in schema_type_map]
    if subtotal_trigger_fields:
        order_by_clauses = [f"`{f_n}` ASC" for f_n in subtotal_trigger_fields]
        if "ORDER BY" not in final_sql.upper(): final_sql += " ORDER BY " + ", ".join(order_by_clauses)
        else: final_sql += ", " + ", ".join(order_by_clauses)
        print(f"INFO: ORDER BY for subtotaling by: {', '.join(subtotal_trigger_fields)}")

    job_config_kwargs_exec = {"use_legacy_sql": False};
    if current_query_params_for_bq_exec: job_config_kwargs_exec["query_parameters"] = current_query_params_for_bq_exec
    job_cfg_exec = bigquery.QueryJobConfig(**job_config_kwargs_exec)
    print(f"INFO: Executing BQ Query for report '{report_definition_name}':\n{final_sql}")
    try:
        query_job = bq_client.query(final_sql, job_config=job_cfg_exec);
        data_rows_list = [convert_row_to_json_serializable(row) for row in query_job.result()]
    except Exception as e:
        error_message = str(e); error_details = [err.get('message', 'BQ err') for err in getattr(e, 'errors', [])]; error_message = "; ".join(error_details) if error_details else error_message
        print(f"ERROR: BQ execution: {error_message}"); raise HTTPException(status_code=500, detail=f"BQ Error: {error_message}")

    try:
        if not html_template_gcs_path or not html_template_gcs_path.startswith("gs://"): raise ValueError("Invalid TemplateURL.")
        path_parts = html_template_gcs_path.replace("gs://", "").split("/", 1); blob = gcs_client.bucket(path_parts[0]).blob(path_parts[1])
        if not blob.exists(): raise GCSNotFound(f"HTML Template not found: {html_template_gcs_path}")
        html_template_content = blob.download_as_text(encoding='utf-8')
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to load HTML template: {str(e)}")

    # --- THIS IS THE CORRECT, PROVEN LOGIC FROM YOUR WORKING VERSION ---
    populated_html = html_template_content
    table_rows_html_str = ""
    body_field_names_in_order = [f_n for f_n in schema_fields_ordered if field_configs_map.get(f_n, FieldDisplayConfig(field_name=f_n)).include_in_body]
    if not body_field_names_in_order and data_rows_list: body_field_names_in_order = list(data_rows_list[0].keys())

    grand_total_accumulators_exec = {f_n: {"values": [], "config": f_c} for f_n, f_c in field_configs_map.items() if f_n in body_field_names_in_order and f_c.numeric_aggregation and schema_type_map.get(f_n) in NUMERIC_TYPES_FOR_AGG}
    needs_grand_total_row_processing = any((fc.group_summary_action in ['GRAND_TOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL']) or (fc.numeric_aggregation and fc.field_name in body_field_names_in_order and schema_type_map.get(fc.field_name) in NUMERIC_TYPES_FOR_AGG) for fc_name, fc in field_configs_map.items() if fc)
    current_group_values = {field_name: None for field_name in subtotal_trigger_fields}
    current_group_rows_for_subtotal_calc = []

    if data_rows_list:
        for i_row, row_data in enumerate(data_rows_list):
            group_break_occurred = False
            break_level = -1
            if subtotal_trigger_fields:
                for group_field_name_idx, group_field_name in enumerate(subtotal_trigger_fields):
                    new_group_value = row_data.get(group_field_name)
                    if current_group_values[group_field_name] is not None and new_group_value != current_group_values[group_field_name]:
                        group_break_occurred = True
                        break_level = group_field_name_idx
                        break
                if group_break_occurred and current_group_rows_for_subtotal_calc:
                    group_label_parts = [f"{fn.replace('_', ' ').title()}: {current_group_values[fn]}" for fn_idx, fn in enumerate(subtotal_trigger_fields) if current_group_values[fn] is not None and fn_idx <= break_level]
                    subtotal_html = "<tr class='subtotal-row' style='font-weight:bold; background-color:#f0f0f0;'>\n"
                    subtotal_html += f"  <td style='padding-left: {10 + break_level * 10}px;' colspan='1'>Subtotal ({', '.join(group_label_parts)}):</td>\n"
                    first_data_col_index_for_subtotal_values = 1
                    for header_key_idx, header_key in enumerate(body_field_names_in_order):
                        if header_key_idx < first_data_col_index_for_subtotal_values: continue
                        field_config = field_configs_map.get(header_key)
                        if field_config and field_config.numeric_aggregation and schema_type_map.get(header_key) in NUMERIC_TYPES_FOR_AGG:
                            group_numeric_vals = [Decimal(str(g_r.get(header_key))) for g_r in current_group_rows_for_subtotal_calc if g_r.get(header_key) is not None and str(g_r.get(header_key)).replace('.','',1).replace('-','',1).isdigit()]
                            agg_val = calculate_aggregate(group_numeric_vals, field_config.numeric_aggregation)
                            fmt_val = format_value(agg_val, field_config.number_format, schema_type_map.get(header_key))
                            align = field_config.alignment or ('right' if schema_type_map.get(header_key) in NUMERIC_TYPES_FOR_AGG else 'left')
                            subtotal_html += f"  <td style='text-align: {align};'>{fmt_val}</td>\n"
                        else: subtotal_html += "  <td></td>\n"
                    subtotal_html += "</tr>\n"; table_rows_html_str += subtotal_html
                    current_group_rows_for_subtotal_calc = []
                    for j in range(break_level, len(subtotal_trigger_fields)):
                        current_group_values[subtotal_trigger_fields[j]] = None
            if subtotal_trigger_fields:
                for group_field_name in subtotal_trigger_fields: current_group_values[group_field_name] = row_data.get(group_field_name)
                current_group_rows_for_subtotal_calc.append(row_data)

            row_html_item = "<tr>\n"
            for idx_hk, header_key in enumerate(body_field_names_in_order):
                cell_value = row_data.get(header_key); field_config = field_configs_map.get(header_key); field_type = schema_type_map.get(header_key, "STRING")
                display_value = True
                is_grouping_field_for_display = header_key in subtotal_trigger_fields
                if is_grouping_field_for_display:
                    field_config_for_group_by_display = field_configs_map.get(header_key)
                    # Corrected logic for 'SHOW_ON_CHANGE' to 'SUPPRESS' from the working model
                    if field_config_for_group_by_display and field_config_for_group_by_display.repeat_group_value == 'SUPPRESS':
                        if i_row > 0:
                            same_as_prev_for_this_and_higher_levels = True
                            current_group_field_level_for_display = subtotal_trigger_fields.index(header_key)
                            for k_level_check in range(current_group_field_level_for_display + 1):
                                check_field = subtotal_trigger_fields[k_level_check]
                                if row_data.get(check_field) != data_rows_list[i_row-1].get(check_field):
                                    same_as_prev_for_this_and_higher_levels = False; break
                            if same_as_prev_for_this_and_higher_levels: display_value = False
                formatted_val = format_value(cell_value, field_config.number_format if field_config else None, field_type) if display_value else ""
                default_align = "left"; align_style_str = ""
                if field_type in NUMERIC_TYPES_FOR_AGG : default_align = "right"
                align_val = (field_config.alignment if field_config else None) or default_align
                indent_px = 0
                if is_grouping_field_for_display:
                    try: level = subtotal_trigger_fields.index(header_key); indent_px = 10 + level * 15
                    except ValueError: pass
                align_style_parts = []
                if indent_px > 0: align_style_parts.append(f"padding-left: {indent_px}px;")
                if align_val: align_style_parts.append(f"text-align: {align_val};")
                if align_style_parts: align_style_str = f"style='{' '.join(align_style_parts)}'"
                row_html_item += f"  <td {align_style_str}>{formatted_val}</td>\n"
                if header_key in grand_total_accumulators_exec and cell_value is not None:
                    try: grand_total_accumulators_exec[header_key]["values"].append(Decimal(str(cell_value)))
                    except: pass
            row_html_item += "</tr>\n"; table_rows_html_str += row_html_item
        if subtotal_trigger_fields and current_group_rows_for_subtotal_calc:
            group_label_parts = [f"{fn.replace('_', ' ').title()}: {current_group_values[fn]}" for fn in subtotal_trigger_fields if current_group_values[fn] is not None]
            subtotal_html = "<tr class='subtotal-row' style='font-weight:bold; background-color:#f0f0f0;'>\n"
            last_group_level = len(subtotal_trigger_fields) -1 if subtotal_trigger_fields else 0
            subtotal_html += f"  <td style='padding-left: {10 + last_group_level * 10}px;' colspan='1'>Subtotal ({', '.join(group_label_parts)}):</td>\n"
            first_data_col_index_for_subtotal_values = 1
            for header_key_idx, header_key in enumerate(body_field_names_in_order):
                if header_key_idx < first_data_col_index_for_subtotal_values: continue
                field_config = field_configs_map.get(header_key)
                if field_config and field_config.numeric_aggregation and schema_type_map.get(header_key) in NUMERIC_TYPES_FOR_AGG:
                    group_numeric_vals = [Decimal(str(g_r.get(header_key))) for g_r in current_group_rows_for_subtotal_calc if g_r.get(header_key) is not None and str(g_r.get(header_key)).replace('.','',1).replace('-','',1).isdigit()]
                    agg_val = calculate_aggregate(group_numeric_vals, field_config.numeric_aggregation)
                    fmt_val = format_value(agg_val, field_config.number_format, schema_type_map.get(header_key))
                    align = field_config.alignment or ('right' if schema_type_map.get(header_key) in NUMERIC_TYPES_FOR_AGG else 'left')
                    subtotal_html += f"  <td style='text-align: {align};'>{fmt_val}</td>\n"
                else: subtotal_html += "  <td></td>\n"
            subtotal_html += "</tr>\n"; table_rows_html_str += subtotal_html
        if needs_grand_total_row_processing:
            gt_html = "<tr class='grand-total-row' style='font-weight:bold; background-color:#e0e0e0; border-top: 2px solid #aaa;'>\n  <td style='padding-left: 10px;' colspan='1'>Grand Total:</td>\n"
            first_data_col_index_for_gt_values = 1
            for header_key_idx, header_key in enumerate(body_field_names_in_order):
                if header_key_idx < first_data_col_index_for_gt_values: continue
                if header_key in grand_total_accumulators_exec:
                    acc = grand_total_accumulators_exec[header_key]
                    agg_val = calculate_aggregate(acc["values"], acc["config"].numeric_aggregation)
                    fmt_val = format_value(agg_val, acc["config"].number_format, schema_type_map.get(header_key))
                    align = acc["config"].alignment or ('right' if schema_type_map.get(header_key) in NUMERIC_TYPES_FOR_AGG else 'left')
                    gt_html += f"  <td style='text-align: {align};'>{fmt_val}</td>\n"
                else: gt_html += "  <td></td>\n"
            gt_html += "</tr>\n"; table_rows_html_str += gt_html
    elif not data_rows_list:
        colspan = len(body_field_names_in_order) if body_field_names_in_order else 1
        table_rows_html_str = f"<tr><td colspan='{colspan}' style='text-align:center; padding: 20px;'>No data found for the selected criteria.</td></tr>"
    populated_html = populated_html.replace("{{TABLE_ROWS_HTML_PLACEHOLDER}}", table_rows_html_str)

    if parsed_calculation_row_configs:
        for calc_row_conf_item in parsed_calculation_row_configs:
            calculated_row_html_cells = ""
            for val_conf_item in calc_row_conf_item.calculated_values:
                numeric_col_data_calc = [Decimal(str(rd.get(val_conf_item.target_field_name))) for rd in data_rows_list if rd.get(val_conf_item.target_field_name) is not None and str(rd.get(val_conf_item.target_field_name)).replace('.','',1).replace('-','',1).isdigit()]
                calculated_result_expl = calculate_aggregate(numeric_col_data_calc, val_conf_item.calculation_type.value)
                field_type_calc_expl = schema_type_map.get(val_conf_item.target_field_name, "STRING")
                num_fmt_calc_expl = val_conf_item.number_format if val_conf_item.calculation_type not in [CalculationType.COUNT, CalculationType.COUNT_DISTINCT] else 'INTEGER'
                formatted_calc_value_expl = format_value(calculated_result_expl, num_fmt_calc_expl, field_type_calc_expl)
                align_style_calc_expl = (val_conf_item.alignment if val_conf_item.alignment else ("right" if field_type_calc_expl in NUMERIC_TYPES_FOR_AGG else "left"))
                calculated_row_html_cells += f"<td style='text-align: {align_style_calc_expl};'>{formatted_calc_value_expl}</td>\n"
            populated_html = populated_html.replace(f"{{{{{calc_row_conf_item.values_placeholder_name}}}}}", calculated_row_html_cells)

    for fc_name, fc_config_obj_ph_final in field_configs_map.items():
        val_fmt_ph_final = ""
        if fc_config_obj_ph_final.include_at_top or fc_config_obj_ph_final.include_in_header:
            raw_val_ph_final = None
            if fc_name in applied_filter_values_for_template_exec:
                raw_val_ph_final = applied_filter_values_for_template_exec[fc_name]
            elif data_rows_list and fc_name in data_rows_list[0]:
                raw_val_ph_final = data_rows_list[0][fc_name]
            val_fmt_ph_final = format_value(raw_val_ph_final, fc_config_obj_ph_final.number_format, schema_type_map.get(fc_name, "STRING"))
            if not val_fmt_ph_final:
                target_tag = f"{{{{TOP_{fc_name}}}}}" if fc_config_obj_ph_final.include_at_top else f"{{{{HEADER_{fc_name}}}}}"
                mapping_info = user_mappings_dict.get(target_tag)
                if mapping_info and mapping_info.fallback_value:
                    val_fmt_ph_final = mapping_info.fallback_value
        if fc_config_obj_ph_final.include_at_top:
            populated_html = populated_html.replace(f"{{{{TOP_{fc_name}}}}}", val_fmt_ph_final or "")
        if fc_config_obj_ph_final.include_in_header:
            populated_html = populated_html.replace(f"{{{{HEADER_{fc_name}}}}}", val_fmt_ph_final or "")

    # --- END OF PROVEN DATA LOGIC ---
    
    # NEW: Add Look rendering logic AFTER data has been populated
    look_configs: List[LookConfig] = []
    if look_configs_json:
        try:
            look_configs = [LookConfig(**item) for item in json.loads(look_configs_json)]
        except (json.JSONDecodeError, TypeError) as e:
            print(f"WARN: Could not parse LookConfigsJSON for '{report_definition_name}': {e}")
    
    if look_configs:
        print(f"INFO: Rendering {len(look_configs)} Look(s) as PNGs from Looker.")
        for look_config in look_configs:
            try:
                print(f" - Rendering Look ID {look_config.look_id} for placeholder {{{{{look_config.placeholder_name}}}}}")
                image_bytes = looker_sdk.run_look(look_id=str(look_config.look_id), result_format="png")
                
                base64_image = base64.b64encode(image_bytes).decode('utf-8')
                image_src_string = f"data:image/png;base64,{base64_image}"
                
                # FIX 1: Added quotes around the alt attribute's value
                img_tag = f'<img src="{image_src_string}" alt="Chart from Look {look_config.look_id}" style="width:100%; height:auto; border: 1px solid #ccc;" />'
                
                placeholder_to_replace = f"{{{{{look_config.placeholder_name}}}}}"
                populated_html = populated_html.replace(placeholder_to_replace, img_tag)

            except Exception as e:
                print(f"ERROR: Failed to render Look {look_config.look_id} from Looker API: {e}")
                error_img_tag = f'<div style="border:2px dashed red; padding:20px; text-align:center;">Error: Could not load chart from Look ID {look_config.look_id}.<br/><small>{str(e)}</small></div>'
                placeholder_to_replace = f"{{{{{look_config.placeholder_name}}}}}"
                populated_html = populated_html.replace(placeholder_to_replace, error_img_tag)

    report_id = str(uuid.uuid4())
    generated_report_gcs_blob_name = f"{config.GCS_GENERATED_REPORTS_PREFIX}{report_id}.html"
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob_out = bucket.blob(generated_report_gcs_blob_name)
        blob_out.upload_from_string(populated_html, content_type='text/html; charset=utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to store generated report to GCS: {str(e)}")

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
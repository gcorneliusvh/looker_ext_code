import datetime
import base64
import json
import os
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

# --- AppConfig & Global Configs ---
class AppConfig:
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcp_location: str = os.getenv("GCP_LOCATION", "")
    default_system_instruction_text: str = ""
    vertex_ai_initialized: bool = False
    bigquery_client: Union[bigquery.Client, None] = None
    storage_client: Union[storage.Client, None] = None
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GCS_SYSTEM_INSTRUCTION_PATH: str = os.getenv("GCS_SYSTEM_INSTRUCTION_PATH", "system_instructions/default_system_instruction.txt")
    TARGET_GEMINI_MODEL: str = "gemini-2.5-pro-preview-05-06" # User-specified model

config = AppConfig()
generated_reports_store: Dict[str, str] = {}

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

DEFAULT_FALLBACK_SYSTEM_INSTRUCTION = """You are a helpful assistant that generates HTML templates for reports.
The user will provide a prompt, an image for styling guidance, a data schema, field display instructions (including alignment, number formatting, group summary actions for string fields, and numeric aggregation preferences for numeric fields), and potentially overall calculation row instructions.
Your output should be a complete, well-structured HTML document with CSS and minimal JavaScript (for presentation only, not data fetching).
Use clear placeholders for data injection:
- For table data rows: '{{TABLE_ROWS_HTML_PLACEHOLDER}}'.
- For specific header/summary values: '{{TOP_FieldName}}' or '{{HEADER_FieldName}}'.
- For overall calculation row values sections (if defined): as specified by 'values_placeholder_name' in the instructions (e.g., '{{TOTAL_FEES_VALUES_PLACEHOLDER}}').

When generating CSS or table cell attributes, try to respect styling hints (alignment, number format) provided for fields.
If fields are marked for numeric aggregation (SUM, AVERAGE, MIN, MAX), ensure table footers or relevant sections can accommodate these summary values, often aligning with the respective columns.
If string fields are marked to trigger subtotals or grand totals, design the table structure to allow for insertion of these summary rows. Subtotal rows should appear after each group of data based on the specified grouping field. Grand total rows typically appear at the very end.
Ensure the HTML is clean and adheres to modern web standards.
Your answer should be ONLY CODE. No Descriptions, no explanations. Just HTML, CSS and Javascript code.
"""

# --- Pydantic Models ---
class CalculationType(str, Enum):
    SUM = "SUM"
    AVERAGE = "AVERAGE"
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    MIN = "MIN"
    MAX = "MAX"

class CalculatedValueConfig(BaseModel): # For user-defined calculation rows
    target_field_name: str
    calculation_type: CalculationType
    number_format: Optional[str] = None
    alignment: Optional[str] = None

class CalculationRowConfig(BaseModel): # For explicit overall summary rows
    row_label: str
    values_placeholder_name: str
    calculated_values: List[CalculatedValueConfig]

class SubtotalConfig(BaseModel): # Kept for potential backward compatibility if old JSON data exists
    group_by_field_name: str
    values_placeholder_name: str
    calculated_values: List[CalculatedValueConfig]

class FieldDisplayConfig(BaseModel): # Updated model
    field_name: str
    include_in_body: bool = Field(default=True)
    include_at_top: bool = Field(default=False)
    include_in_header: bool = Field(default=False)
    context_note: Optional[str] = None
    alignment: Optional[str] = None
    number_format: Optional[str] = None
    group_summary_action: Optional[str] = None  # E.g., 'SUBTOTAL_ONLY', 'GRAND_TOTAL_ONLY'
    repeat_group_value: Optional[str] = Field(default='REPEAT')
    numeric_aggregation: Optional[str] = None  # E.g., 'SUM', 'AVERAGE'

class ReportDefinitionPayload(BaseModel):
    report_name: str
    image_url: str
    sql_query: str
    prompt: str
    field_display_configs: Optional[List[FieldDisplayConfig]] = None # Will use updated FieldDisplayConfig
    user_attribute_mappings: Optional[Dict[str, str]] = Field(default_factory=dict)
    calculation_row_configs: Optional[List[CalculationRowConfig]] = None
    subtotal_configs: Optional[List[SubtotalConfig]] = Field(default_factory=list) # Will be [] from new frontend
    optimized_prompt: Optional[str] = None
    header_text: Optional[str] = None
    footer_text: Optional[str] = None

class ExecuteReportPayload(BaseModel):
    report_definition_name: str
    filter_criteria_json: str = Field(default="{}")

class ReportDefinitionListItem(BaseModel):
    ReportName: str
    Prompt: Optional[str] = None
    SQL: Optional[str] = None
    ScreenshotURL: Optional[str] = None
    TemplateURL: Optional[str] = None
    BaseQuerySchemaJSON: Optional[str] = None
    UserAttributeMappingsJSON: Optional[str] = None
    FieldDisplayConfigsJSON: Optional[str] = None # Will store new structure
    CalculationRowConfigsJSON: Optional[str] = None
    SubtotalConfigsJSON: Optional[str] = None # For backward compatibility
    LastGeneratedTimestamp: Optional[datetime.datetime] = None

class SystemInstructionPayload(BaseModel):
    system_instruction: str

class SqlQueryPayload(BaseModel):
    sql_query: str


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
        config.TARGET_GEMINI_MODEL = "gemini-2.5-pro-preview-05-06" # Ensure fallback if somehow empty

    print(f"INFO: Target Gemini Model: {config.TARGET_GEMINI_MODEL}")
    # ... (rest of lifespan as in the previously working version) ...
    if not config.gcp_project_id: print("ERROR: GCP_PROJECT_ID environment variable not set.")
    if not config.gcp_location: print("ERROR: GCP_LOCATION environment variable not set.")
    print(f"INFO: Target GCS Bucket: {config.GCS_BUCKET_NAME or 'NOT SET'}")
    print(f"INFO: System Instruction GCS Path: gs://{config.GCS_BUCKET_NAME}/{config.GCS_SYSTEM_INSTRUCTION_PATH}")

    if not config.GCS_BUCKET_NAME:
        print("ERROR: GCS_BUCKET_NAME environment variable not set. GCS operations will fail.")
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
        config.default_system_instruction_text = _load_system_instruction_from_gcs(
            config.storage_client,
            config.GCS_BUCKET_NAME,
            config.GCS_SYSTEM_INSTRUCTION_PATH
        )
    else:
        print("INFO: Using default system instruction due to missing GCS client or bucket name.")
        config.default_system_instruction_text = DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    
    if config.default_system_instruction_text:
        print(f"INFO: System instruction loaded (length: {len(config.default_system_instruction_text)} chars). Preview: '{config.default_system_instruction_text[:100]}...'")
    else:
        print("ERROR: System instruction is empty after attempting to load.")

    if config.gcp_project_id and config.gcp_location:
        try:
            print(f"INFO: Initializing Vertex AI SDK with project: {config.gcp_project_id}, location: {config.gcp_location}")
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
        print(f"ERROR: BigQuery prerequisites not met (BigQuery module loaded: {bool(bigquery)}, GCP_PROJECT_ID set: {bool(config.gcp_project_id)}).")
        config.bigquery_client = None
    yield
    print("INFO: FastAPI application shutdown.")

app = FastAPI(lifespan=lifespan)

# Explicitly define allowed origins
NGROK_URL_FROM_ENV = os.getenv("FRONTEND_NGROK_URL")
LOOKER_INSTANCE_URL_FROM_ENV = os.getenv("LOOKER_INSTANCE_URL", "https://igmprinting.cloud.looker.com") # Default
LOOKER_EXTENSION_SANDBOX_HOST = "https://83d14716-08b3-4def-bdca-54f4b2af9f19-extensions.cloud.looker.com"

allowed_origins_list = [
    "http://localhost:8080",
    LOOKER_INSTANCE_URL_FROM_ENV,
    LOOKER_EXTENSION_SANDBOX_HOST,
]
if NGROK_URL_FROM_ENV:
    allowed_origins_list.append(NGROK_URL_FROM_ENV)
allowed_origins_list = sorted(list(set(o for o in allowed_origins_list if o and o.startswith("http"))))
if not allowed_origins_list: allowed_origins_list = ["http://localhost:8080"]

print(f"INFO: CORS allow_origins effectively configured for: {allowed_origins_list}")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _load_system_instruction_from_gcs(client: storage.Client, bucket_name: str, blob_name: str) -> str:
    # ... (same as before) ...
    if not client or not bucket_name:
        print(f"WARN: GCS client or bucket name not provided. Using fallback default system instruction for GCS load.")
        return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            instruction_text = blob.download_as_text(encoding='utf-8')
            print(f"INFO: System instruction loaded successfully from gs://{bucket_name}/{blob_name}")
            return instruction_text
        else:
            print(f"WARN: System instruction file not found at gs://{bucket_name}/{blob_name}. Using fallback default.")
            return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    except Exception as e:
        print(f"ERROR: Failed to load system instruction from GCS (gs://{bucket_name}/{blob_name}): {e}. Using fallback default.")
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

def remove_first_and_last_lines(s: str) -> str:
    # ... (same as before) ...
    if not s: return ""
    lines = s.splitlines()
    if len(lines) >= 2 and lines[0].strip().startswith("```") and lines[-1].strip() == "```":
        return '\n'.join(lines[1:-1])
    if len(lines) >= 1 and lines[0].strip().startswith("```"):
        return '\n'.join(lines[1:]) if len(lines) > 1 else ""
    return s

def generate_html_from_user_pattern(
    prompt_text: str, image_bytes: bytes, image_mime_type: str, system_instruction_text: str
) -> Union[str, None]:
    # ... (same as before, using config.TARGET_GEMINI_MODEL) ...
    get_vertex_ai_initialized_flag() 

    print(f"DEBUG: Vertex AI: Using System Instruction (first 100 chars): {system_instruction_text[:100]}")
    print(f"DEBUG: Vertex AI: Target Model for generation: {config.TARGET_GEMINI_MODEL}")
    try:
        model_instance = GenerativeModel(
            model_name=config.TARGET_GEMINI_MODEL,
            system_instruction=[system_instruction_text] if system_instruction_text else None
        )
        image_part = Part.from_data(data=image_bytes, mime_type=image_mime_type)
        prompt_part = Part.from_text(text=prompt_text)
        contents_for_gemini = [prompt_part, image_part]
        safety_settings_config = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }
        generation_config_obj = GenerationConfig(
            temperature=0.8, top_p=0.95, max_output_tokens=65535, candidate_count=1
        )
        response = model_instance.generate_content(
            contents=contents_for_gemini, generation_config=generation_config_obj,
            safety_settings=safety_settings_config, stream=False
        )
        generated_text_output = ""
        if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
            for part_item in response.candidates[0].content.parts:
                if hasattr(part_item, 'text') and part_item.text:
                    generated_text_output += part_item.text
        else:
             print(f"WARN: Gemini response structure unexpected or no text content. Response: {response}")
    except (google_api_exceptions.NotFound, vertexai.generative_models.exceptions.NotFoundError) as e_nf:
        error_detail = f"Model '{config.TARGET_GEMINI_MODEL}' not found or project lacks access: {str(e_nf)}"
        print(f"ERROR: Vertex AI (NotFound): {error_detail}")
        raise HTTPException(status_code=404, detail=error_detail)
    except google_api_exceptions.InvalidArgument as e_ia:
        error_detail = f"Invalid argument for model '{config.TARGET_GEMINI_MODEL}', possibly an empty or malformed model name: {str(e_ia)}"
        print(f"ERROR: Vertex AI (InvalidArgument): {error_detail}")
        raise HTTPException(status_code=400, detail=error_detail)
    except Exception as e:
        print(f"ERROR: Vertex AI: GenAI content generation error: {type(e).__name__} - {str(e)}")
        import traceback
        print(traceback.format_exc())
        error_detail = f"Vertex AI content generation failed: {str(e)}"
        raise HTTPException(status_code=500, detail=error_detail)
    return remove_first_and_last_lines(generated_text_output) if generated_text_output else ""

def convert_row_to_json_serializable(row: bigquery.Row) -> Dict[str, Any]:
    # ... (same as before) ...
    output = {}
    for key, value in row.items():
        if isinstance(value, Decimal): output[key] = str(value)
        elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)): output[key] = value.isoformat()
        elif isinstance(value, bytes): output[key] = base64.b64encode(value).decode('utf-8')
        elif isinstance(value, list):
            output[key] = [(item.isoformat() if isinstance(item, (datetime.date, datetime.datetime, datetime.time)) else str(item) if isinstance(item, Decimal) else item) for item in value]
        else: output[key] = value
    return output

def get_bq_param_type_and_value(value_str: str, bq_col_name: str, type_hint: str):
    # ... (same as before) ...
    value_str = str(value_str)
    if type_hint == "NONE": return None, None
    if type_hint == "STRING_ARRAY":
        items = [item.strip() for item in value_str.split(',') if item.strip()]
        return "STRING", items 
    if type_hint == "STRING_PREFIX": return "STRING", f"{value_str}%"
    if type_hint == "STRING_SUFFIX": return "STRING", f"%{value_str}"
    if type_hint == "BOOL_TRUE_STR": return "BOOL", True
    if type_hint == "BOOL_FALSE_STR": return "BOOL", False
    if type_hint == "AUTO_DATE_OR_NUM_RANGE":
        parts = [v.strip() for v in value_str.split(',', 1)]
        val1_str, val2_str = (parts[0], parts[1]) if len(parts) == 2 else (parts[0], parts[0])
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

@app.get("/")
async def read_root(request: Request): # Added request to log origin if needed
    print(f"DEBUG /: Incoming request origin: {request.headers.get('origin')}")
    return {"status": f"GenAI Report API is running! (Target Model: {config.TARGET_GEMINI_MODEL})"}

@app.post("/dry_run_sql_for_schema")
async def dry_run_sql_for_schema_endpoint(
    request: Request,
    payload: SqlQueryPayload, 
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
):
    print(f"DEBUG /dry_run_sql_for_schema: Incoming request origin: {request.headers.get('origin')}")
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_job = bq_client.query(payload.sql_query, job_config=job_config)
        schema_for_response = []
        if dry_run_job.schema:
            schema_for_response = [
                {"name": field.name, "type": str(field.field_type).upper(), "mode": str(field.mode).upper()}
                for field in dry_run_job.schema
            ]
            return {"schema": schema_for_response}
        else:
            return {"schema": [], "message": "Dry run successful but no schema information was returned."}
    except Exception as e:
        error_message = str(e)
        if hasattr(e, 'errors') and e.errors:
            error_details = [err.get('message', 'Unknown BQ error') for err in e.errors]
            error_message = "; ".join(error_details)
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
        bucket = storage_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.GCS_SYSTEM_INSTRUCTION_PATH)
        blob.upload_from_string(new_instruction_text, content_type='text/plain; charset=utf-8')
        config.default_system_instruction_text = new_instruction_text
        print(f"INFO: System instruction updated.")
        return {"message": "System instruction updated successfully."}
    except Exception as e:
        print(f"ERROR: Failed to PUT system instruction to GCS: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update system instruction: {str(e)}")

@app.post("/report_definitions", status_code=201)
async def upsert_report_definition(
    payload: ReportDefinitionPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    _vertex_ai_init_check: None = Depends(get_vertex_ai_initialized_flag)
):
    report_name = payload.report_name
    base_sql_query = payload.sql_query
    base_user_prompt = payload.prompt
    image_url = payload.image_url
    field_display_configs_from_payload = payload.field_display_configs
    calculation_row_configs_from_payload = payload.calculation_row_configs
    subtotal_configs_from_payload = payload.subtotal_configs or [] # Default to empty list
    user_attribute_mappings_json_str = json.dumps(payload.user_attribute_mappings or {})
    
    print(f"INFO: Upserting report definition for: '{report_name}'")
    schema_from_dry_run_list = []
    schema_map_for_prompt = {} 
    try:
        dry_run_job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_job = bq_client.query(base_sql_query, job_config=dry_run_job_config)
        if dry_run_job.schema:
            for field in dry_run_job.schema:
                field_data = {"name": field.name, "type": str(field.field_type).upper(), "mode": str(field.mode).upper()}
                schema_from_dry_run_list.append(field_data)
                schema_map_for_prompt[field.name] = field_data["type"]
            schema_parts = [f"`{f['name']}` (Type: {f['type']})" for f in schema_from_dry_run_list]
            schema_for_gemini_prompt_str = "Schema: " + ", ".join(schema_parts)
        else:
            schema_for_gemini_prompt_str = "Schema: Not determined or query is not a SELECT."
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Base SQL query dry run failed: {str(e)}")

    effective_field_display_configs: List[FieldDisplayConfig] = []
    if field_display_configs_from_payload:
        # Using the updated FieldDisplayConfig model which includes group_summary_action and numeric_aggregation
        effective_field_display_configs = [FieldDisplayConfig(**fc.model_dump(exclude_unset=True)) for fc in field_display_configs_from_payload]
    elif schema_from_dry_run_list:
        effective_field_display_configs = [
            FieldDisplayConfig(field_name=f["name"], include_in_body=True) for f in schema_from_dry_run_list
        ]
    
    prompt_for_template = base_user_prompt
    prompt_for_template += f"\n\n--- Data Schema (Available Fields) ---\n{schema_for_gemini_prompt_str}\n--- End Data Schema ---"
    
    if effective_field_display_configs:
        prompt_for_template += "\n\n--- Field Display & Placeholder Instructions (Includes Styling, Grouping, and Aggregation Preferences) ---"
        body_fields_prompt_parts, top_fields_prompt_parts, header_fields_prompt_parts = [], [], []
        NUMERIC_TYPES_PROMPT = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]

        for config_item in effective_field_display_configs:
            field_type = schema_map_for_prompt.get(config_item.field_name, "UNKNOWN").upper()
            is_numeric_field = field_type in NUMERIC_TYPES_PROMPT
            is_string_field = field_type == "STRING"

            style_hints = []
            if config_item.alignment: style_hints.append(f"align: {config_item.alignment}")
            if config_item.number_format: style_hints.append(f"format: {config_item.number_format}")
            
            field_info = f"- `{config_item.field_name}`"
            if style_hints: field_info += f" (Styling: {'; '.join(style_hints)})"

            if is_string_field and config_item.group_summary_action:
                field_info += f" (Group Summary Action: {config_item.group_summary_action})"
                if config_item.repeat_group_value:
                     field_info += f" (Repeat Group Value: {config_item.repeat_group_value})"
            
            if is_numeric_field and config_item.numeric_aggregation:
                field_info += f" (Numeric Aggregation for Summaries: {config_item.numeric_aggregation})"

            if config_item.context_note: field_info += f" (User context: {config_item.context_note})"

            if config_item.include_in_body: body_fields_prompt_parts.append(field_info)
            if config_item.include_at_top: top_fields_prompt_parts.append(field_info + f" -> Use placeholder: {{TOP_{config_item.field_name}}}")
            if config_item.include_in_header: header_fields_prompt_parts.append(field_info + f" -> Use placeholder: {{HEADER_{config_item.field_name}}}")

        if body_fields_prompt_parts: prompt_for_template += "\nFields for Report Body (Main Data Table - for `{{TABLE_ROWS_HTML_PLACEHOLDER}}`):\n" + "\n".join(body_fields_prompt_parts)
        else: prompt_for_template += "\nNo fields designated for main Report Body table."
        if top_fields_prompt_parts: prompt_for_template += "\n\nFields for Top of Report (Single Values/Summaries):\n" + "\n".join(top_fields_prompt_parts)
        if header_fields_prompt_parts: prompt_for_template += "\n\nFields for Report Header:\n" + "\n".join(header_fields_prompt_parts)
        prompt_for_template += "\n--- End Field Display Instructions ---"

    if calculation_row_configs_from_payload:
        prompt_for_template += "\n\n--- Explicit Overall Calculation Row Instructions (e.g., for table footer) ---"
        for i, calc_row_config in enumerate(calculation_row_configs_from_payload):
            num_values = len(calc_row_config.calculated_values)
            value_descs = [f"{cv.calculation_type.value} of '{cv.target_field_name}'" for cv in calc_row_config.calculated_values]
            prompt_for_template += (
                f"\n- Calculation Row {i+1}:\n"
                f"  - Display Label: \"{calc_row_config.row_label}\"\n"
                f"  - Values Placeholder Name: `{{{{{calc_row_config.values_placeholder_name}}}}}` (for {num_values} calculated value(s): {'; '.join(value_descs)}).\n"
            )
        prompt_for_template += "\n--- End Explicit Calculation Row Instructions ---"
    
    # Append the reminder part of the default system instruction
    guideline_split = DEFAULT_FALLBACK_SYSTEM_INSTRUCTION.split("--- HTML Template Generation Guidelines (Reminder) ---", 1)
    if len(guideline_split) > 1:
        prompt_for_template += "\n\n--- HTML Template Generation Guidelines (Reminder) ---" + guideline_split[1]
    else: # Fallback
        prompt_for_template += "\n\n--- HTML Template Generation Guidelines (Reminder) ---\n1. Focus on visual match and user prompt.\n2. Follow instructions for schema, field display, summaries.\n3. Use placeholders correctly.\n4. Ensure summary rows are distinct.\n5. Output ONLY HTML CODE."

    try:
        async with httpx.AsyncClient(timeout=180.0) as client_httpx:
            img_response = await client_httpx.get(image_url); img_response.raise_for_status()
            image_bytes_data = await img_response.aread()
            image_mime_type_data = img_response.headers.get("Content-Type", "application/octet-stream").lower()
            if not image_mime_type_data.startswith("image/"): raise ValueError(f"Content-Type '{image_mime_type_data}' from URL is not valid.")
    except httpx.RequestError as e: raise HTTPException(status_code=400, detail=f"Error fetching image URL: {e}")
    except ValueError as e: raise HTTPException(status_code=400, detail=str(e))
    except Exception as e: raise HTTPException(status_code=500, detail=f"Unexpected error fetching image: {e}")

    html_template_content = generate_html_from_user_pattern(
        prompt_text=prompt_for_template, image_bytes=image_bytes_data,
        image_mime_type=image_mime_type_data, system_instruction_text=config.default_system_instruction_text
    )
    if not html_template_content or not html_template_content.strip():
        print("WARNING: Gemini returned empty content. Using fallback HTML.")
        html_template_content = "<html><body><p>Error: AI failed to generate HTML template.</p></body></html>"

    report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower()
    base_gcs_folder = f"report_templates/{report_gcs_path_safe}"
    template_gcs_path_str = f"{base_gcs_folder}/template.html"
    base_sql_gcs_path_str = f"{base_gcs_folder}/base_query.sql"
    schema_gcs_path_str = f"{base_gcs_folder}/schema.json"
    field_configs_gcs_path_str = f"{base_gcs_folder}/field_display_configs.json"
    calc_row_configs_gcs_path_str = f"{base_gcs_folder}/calculation_row_configs.json"
    subtotal_configs_gcs_path_str = f"{base_gcs_folder}/subtotal_configs.json" # For storing what was received (empty list)

    schema_json_to_save = json.dumps(schema_from_dry_run_list, indent=2)
    field_display_configs_json_to_save = json.dumps(
        [fc.model_dump(exclude_unset=True) for fc in effective_field_display_configs], indent=2 # exclude_unset is better
    ) if effective_field_display_configs else "[]"
    calculation_row_configs_json_to_save = json.dumps(
        [crc.model_dump(exclude_unset=True) for crc in payload.calculation_row_configs], indent=2
    ) if payload.calculation_row_configs else "[]"
    subtotal_configs_json_to_save = json.dumps(payload.subtotal_configs or [], indent=2) # Save what's received (empty list)


    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        bucket.blob(template_gcs_path_str).upload_from_string(html_template_content, content_type='text/html; charset=utf-8')
        bucket.blob(base_sql_gcs_path_str).upload_from_string(base_sql_query, content_type='application/sql; charset=utf-8')
        bucket.blob(schema_gcs_path_str).upload_from_string(schema_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(field_configs_gcs_path_str).upload_from_string(field_display_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(calc_row_configs_gcs_path_str).upload_from_string(calculation_row_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(subtotal_configs_gcs_path_str).upload_from_string(subtotal_configs_json_to_save, content_type='application/json; charset=utf-8')
        print(f"INFO: Saved template artifacts for '{report_name}' to GCS.")
    except Exception as e: 
        print(f"ERROR: GCS Upload Failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to save files to GCS: {e}")
    
    table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
    # MERGE SQL should be fine as it stores JSON strings
    merge_sql = f"""
    MERGE {table_id} T USING (
        SELECT @report_name AS ReportName, @prompt AS Prompt, @sql_query AS SQL, @image_url AS ScreenshotURL,
               @template_gcs_path AS TemplateURL, @base_sql_gcs_path AS BaseQueryGCSPath,
               @schema_gcs_path AS SchemaGCSPath, @schema_json AS BaseQuerySchemaJSON,
               @field_display_configs_json AS FieldDisplayConfigsJSON, 
               @calculation_row_configs_json AS CalculationRowConfigsJSON,
               @subtotal_configs_json AS SubtotalConfigsJSON, 
               @user_attribute_mappings_json AS UserAttributeMappingsJSON,
               @optimized_prompt AS OptimizedPrompt, @header_text AS Header, @footer_text AS Footer,
               CURRENT_TIMESTAMP() AS CurrentTs
    ) S ON T.ReportName = S.ReportName
    WHEN MATCHED THEN UPDATE SET
            Prompt = S.Prompt, SQL = S.SQL, ScreenshotURL = S.ScreenshotURL, TemplateURL = S.TemplateURL,
            BaseQueryGCSPath = S.BaseQueryGCSPath, SchemaGCSPath = S.SchemaGCSPath,
            BaseQuerySchemaJSON = S.BaseQuerySchemaJSON, FieldDisplayConfigsJSON = S.FieldDisplayConfigsJSON,
            CalculationRowConfigsJSON = S.CalculationRowConfigsJSON,
            SubtotalConfigsJSON = S.SubtotalConfigsJSON, 
            UserAttributeMappingsJSON = S.UserAttributeMappingsJSON, OptimizedPrompt = S.OptimizedPrompt,
            Header = S.Header, Footer = S.Footer, LastGeneratedTimestamp = S.CurrentTs
    WHEN NOT MATCHED THEN INSERT (
            ReportName, Prompt, SQL, ScreenshotURL, TemplateURL, BaseQueryGCSPath, SchemaGCSPath,
            BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON, 
            UserAttributeMappingsJSON, OptimizedPrompt, Header, Footer, CreatedTimestamp, LastGeneratedTimestamp
    ) VALUES (
            S.ReportName, S.Prompt, S.SQL, S.ScreenshotURL, S.TemplateURL, S.BaseQueryGCSPath, S.SchemaGCSPath,
            S.BaseQuerySchemaJSON, S.FieldDisplayConfigsJSON, S.CalculationRowConfigsJSON, S.SubtotalConfigsJSON, 
            S.UserAttributeMappingsJSON, S.OptimizedPrompt, S.Header, S.Footer, S.CurrentTs, S.CurrentTs
    )"""
    merge_params = [
        ScalarQueryParameter("report_name", "STRING", payload.report_name),
        ScalarQueryParameter("prompt", "STRING", payload.prompt),
        ScalarQueryParameter("sql_query", "STRING", payload.sql_query),
        ScalarQueryParameter("image_url", "STRING", payload.image_url),
        ScalarQueryParameter("template_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{template_gcs_path_str}"),
        ScalarQueryParameter("base_sql_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{base_sql_gcs_path_str}"),
        ScalarQueryParameter("schema_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{schema_gcs_path_str}"),
        ScalarQueryParameter("schema_json", "STRING", schema_json_to_save),
        ScalarQueryParameter("field_display_configs_json", "STRING", field_display_configs_json_to_save), # Contains new fields
        ScalarQueryParameter("calculation_row_configs_json", "STRING", calculation_row_configs_json_to_save),
        ScalarQueryParameter("subtotal_configs_json", "STRING", subtotal_configs_json_to_save), # "[]"
        ScalarQueryParameter("user_attribute_mappings_json", "STRING", user_attribute_mappings_json_str),
        ScalarQueryParameter("optimized_prompt", "STRING", payload.optimized_prompt),
        ScalarQueryParameter("header_text", "STRING", payload.header_text),
        ScalarQueryParameter("footer_text", "STRING", payload.footer_text),
    ]
    try:
        print(f"DEBUG: Attempting BQ MERGE for report: {payload.report_name}")
        job = bq_client.query(merge_sql, job_config=bigquery.QueryJobConfig(query_parameters=merge_params))
        job.result() 
        print(f"INFO: Successfully merged report definition '{payload.report_name}' into BigQuery.")
    except Exception as e:
        error_message = f"Failed to save report definition to BigQuery: {str(e)}"
        if hasattr(e, 'errors') and e.errors:
            bq_errors = [f"Reason: {err.get('reason', 'N/A')}, Message: {err.get('message', 'N/A')}" for err in e.errors]
            error_message += f" BigQuery Errors: {'; '.join(bq_errors)}"
        print(f"ERROR: {error_message}")
        raise HTTPException(status_code=500, detail=error_message)
        
    return {"message": f"Report definition '{report_name}' upserted.", "template_html_gcs_path": f"gs://{config.GCS_BUCKET_NAME}/{template_gcs_path_str}"}


@app.get("/report_definitions", response_model=List[ReportDefinitionListItem])
async def list_report_definitions_endpoint(bq_client: bigquery.Client = Depends(get_bigquery_client_dep)):
    query = f"""
        SELECT ReportName, Prompt, SQL, ScreenshotURL, TemplateURL, BaseQuerySchemaJSON,
               FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON,
               UserAttributeMappingsJSON, LastGeneratedTimestamp
        FROM `{config.gcp_project_id}.report_printing.report_list` ORDER BY ReportName ASC
    """
    try:
        results = list(bq_client.query(query).result())
        processed_results = []
        for row_dict_item in [dict(row.items()) for row in results]:
            for json_field in ['BaseQuerySchemaJSON', 'FieldDisplayConfigsJSON', 'CalculationRowConfigsJSON', 'SubtotalConfigsJSON', 'UserAttributeMappingsJSON']:
                if row_dict_item.get(json_field) is None:
                    if json_field == 'UserAttributeMappingsJSON': row_dict_item[json_field] = "{}" 
                    else: row_dict_item[json_field] = "[]"
            try:
                processed_results.append(ReportDefinitionListItem(**row_dict_item))
            except Exception as pydantic_error:
                print(f"ERROR: Pydantic validation error for report {row_dict_item.get('ReportName')}: {pydantic_error}. Row data: {row_dict_item}")
                continue 
        return processed_results
    except Exception as e: 
        print(f"ERROR fetching report definitions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch report definitions: {str(e)}")


def format_value(value: Any, format_str: Optional[str], field_type_str: str) -> str:
    if value is None: return ""
    NUMERIC_TYPES_FOR_FORMATTING = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]
    field_type_upper = str(field_type_str).upper() if field_type_str else "UNKNOWN"

    if format_str and field_type_upper in NUMERIC_TYPES_FOR_FORMATTING:
        try:
            str_value = str(value) if not isinstance(value, (int, float, Decimal)) else value
            num_value = Decimal(str_value)
            if format_str == 'INTEGER': return f"{num_value:,.0f}"
            elif format_str == 'DECIMAL_2': return f"{num_value:,.2f}"
            elif format_str == 'USD': return f"${num_value:,.2f}"
            elif format_str == 'EUR': return f"€{num_value:,.2f}"
            elif format_str == 'PERCENT_2': return f"{num_value * Decimal('100'):,.2f}%"
            else: return str(value)
        except (ValueError, TypeError, InvalidOperation) as e:
            print(f"WARN: Formatting error for value '{value}' with format '{format_str}': {e}")
            return str(value)
    return str(value)

NUMERIC_TYPES_FOR_AGG = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]

def calculate_aggregate(data_list: List[Decimal], agg_type_str_param: Optional[str]) -> Decimal:
    if not agg_type_str_param: return Decimal('0')
    agg_type = agg_type_str_param.upper()
    if not data_list:
        return Decimal('0') # SUM/AVG/MIN/MAX of empty is 0, COUNT is 0.
    
    if agg_type == "SUM": return sum(data_list)
    if agg_type == "AVERAGE": return sum(data_list) / len(data_list) if data_list else Decimal('0')
    if agg_type == "MIN": return min(data_list) if data_list else Decimal('0')
    if agg_type == "MAX": return max(data_list) if data_list else Decimal('0')
    if agg_type == "COUNT": return Decimal(len(data_list)) # Count of numeric items
    if agg_type == "COUNT_DISTINCT": 
        return Decimal(len(set(str(d) for d in data_list))) if data_list else Decimal('0')
    print(f"WARN: Unknown aggregation type '{agg_type_str_param}' requested.")
    return Decimal('0')


@app.post("/execute_report")
async def execute_report_and_get_url(
    payload: ExecuteReportPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep)
):
    report_definition_name = payload.report_definition_name
    filter_criteria_json_str = payload.filter_criteria_json
    print(f"INFO: POST /execute_report for '{report_definition_name}'. Filters JSON: {filter_criteria_json_str}")

    query_def_sql = f"""
        SELECT SQL, TemplateURL, UserAttributeMappingsJSON, BaseQuerySchemaJSON, 
               FieldDisplayConfigsJSON, CalculationRowConfigsJSON 
        FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param
    """
    def_params = [ScalarQueryParameter("report_name_param", "STRING", report_definition_name)]
    try:
        results = list(bq_client.query(query_def_sql, job_config=bigquery.QueryJobConfig(query_parameters=def_params)).result())
        if not results: raise HTTPException(status_code=404, detail=f"Report definition '{report_definition_name}' not found.")
        row = results[0]
        base_sql_query_from_db = row.get("SQL")
        html_template_gcs_path = row.get("TemplateURL")
        user_attr_map_json = row.get("UserAttributeMappingsJSON")
        bq_schema_json = row.get("BaseQuerySchemaJSON")
        field_configs_json = row.get("FieldDisplayConfigsJSON")
        calculation_row_configs_json = row.get("CalculationRowConfigsJSON")
        # SubtotalConfigsJSON is not fetched here as we are relying on field_display_configs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching report definition '{report_definition_name}': {str(e)}")

    schema_type_map: Dict[str, str] = {}
    schema_fields_ordered: List[str] = []
    if bq_schema_json:
        try:
            parsed_schema = json.loads(bq_schema_json)
            for field_info in parsed_schema:
                if 'name' in field_info and 'type' in field_info:
                    schema_type_map[field_info['name']] = str(field_info['type']).upper()
                    schema_fields_ordered.append(field_info['name'])
        except json.JSONDecodeError: print(f"WARNING: Could not parse BaseQuerySchemaJSON for '{report_definition_name}'.")

    field_configs_map: Dict[str, FieldDisplayConfig] = {}
    if field_configs_json:
        try:
            parsed_field_configs = json.loads(field_configs_json)
            for item_dict in parsed_field_configs:
                # This uses the NEW FieldDisplayConfig model that includes group_summary_action and numeric_aggregation
                # Handle old key 'subtotal_action' for backward compatibility if loading old defs
                if 'subtotal_action' in item_dict and 'group_summary_action' not in item_dict:
                    item_dict['group_summary_action'] = item_dict.pop('subtotal_action')
                # Ensure all optional fields expected by Pydantic model are present or defaulted if necessary
                item_dict.setdefault('group_summary_action', None)
                item_dict.setdefault('repeat_group_value', 'REPEAT') 
                item_dict.setdefault('numeric_aggregation', None)
                try:
                    field_configs_map[item_dict['field_name']] = FieldDisplayConfig(**item_dict)
                except Exception as p_error:
                    print(f"ERROR: Pydantic validation failed for field config item: {item_dict}. Error: {p_error}")
        except (json.JSONDecodeError, TypeError) as e: 
            print(f"WARNING: Could not parse FieldDisplayConfigsJSON for '{report_definition_name}': {e}.")


    parsed_calculation_row_configs: List[CalculationRowConfig] = []
    if calculation_row_configs_json:
        try: parsed_calculation_row_configs = [CalculationRowConfig(**item) for item in json.loads(calculation_row_configs_json)]
        except: pass
    
    # --- Filter parsing logic (remains the same) ---
    current_query_params_for_bq = []
    current_conditions = []
    applied_filter_values_for_template = {}
    param_idx = 0
    try: looker_filters_payload = json.loads(filter_criteria_json_str or "{}")
    except json.JSONDecodeError as e: raise HTTPException(status_code=400, detail=f"Invalid JSON for filter_criteria: {str(e)}")
    parsed_user_attribute_mappings: Dict[str, str] = json.loads(user_attr_map_json or '{}')
    # ... (UA filter loop and dynamic filter loop needs to be here, same as previous correct version)
    for fe_key_ua, val_str_ua in looker_filters_payload.get("user_attributes", {}).items():
        bq_col_ua = parsed_user_attribute_mappings.get(fe_key_ua)
        if bq_col_ua:
            p_name_ua = f"ua_p_{param_idx}"; param_idx += 1
            try:
                bq_type_ua, typed_val_ua = get_bq_param_type_and_value(str(val_str_ua), bq_col_ua, "AUTO")
                current_conditions.append(f"`{bq_col_ua}` = @{p_name_ua}")
                current_query_params_for_bq.append(ScalarQueryParameter(p_name_ua, bq_type_ua, typed_val_ua))
                # ... (applied_filter_values_for_template update)
            except ValueError as ve: print(f"WARN: Skipping UA filter '{bq_col_ua}': {ve}")

    for filter_key_dyn, val_str_list_dyn in looker_filters_payload.get("dynamic_filters", {}).items():
        # ... (Full dynamic filter parsing logic here) ...
        pass # Placeholder, this logic is complex and should be taken from a working version

    final_sql = base_sql_query_from_db.strip().rstrip(';')
    if current_conditions:
        conditions_sql_segment = " AND ".join(current_conditions)
        if " where " in final_sql.lower(): final_sql = f"{final_sql} AND ({conditions_sql_segment})"
        else: final_sql = f"SELECT * FROM ({final_sql}) AS GenAIReportSubquery WHERE {conditions_sql_segment}"
            
    subtotal_trigger_fields_from_config = []
    for f_name, f_config in field_configs_map.items():
        if f_config.group_summary_action in ['SUBTOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL'] and f_name in schema_type_map:
            subtotal_trigger_fields_from_config.append(f_name)
    
    if subtotal_trigger_fields_from_config:
        order_by_clauses = [f"`{field_name}` ASC" for field_name in subtotal_trigger_fields_from_config]
        if "ORDER BY" not in final_sql.upper():
            final_sql += " ORDER BY " + ", ".join(order_by_clauses)
        else:
            final_sql += ", " + ", ".join(order_by_clauses) 
        print(f"INFO: Added/modified ORDER BY for subtotaling by: {', '.join(subtotal_trigger_fields_from_config)}")

    # ... (BQ execution, template loading - same as previous correct version) ...
    job_config_kwargs = {"use_legacy_sql": False}
    if current_query_params_for_bq: job_config_kwargs["query_parameters"] = current_query_params_for_bq
    job_cfg = bigquery.QueryJobConfig(**job_config_kwargs)
    print(f"INFO: Executing BQ Query for report '{report_definition_name}':\n{final_sql}")
    try:
        query_job = bq_client.query(final_sql, job_config=job_cfg)
        data_rows_list = [convert_row_to_json_serializable(row) for row in query_job.result()]
    except Exception as e:
        error_message = str(e)
        if hasattr(e, 'errors') and e.errors: error_message = "; ".join([err.get('message', 'BQ error') for err in e.errors])
        print(f"ERROR: BQ execution failed: {error_message}"); raise HTTPException(status_code=500, detail=f"BQ Error: {error_message}")

    try:
        if not html_template_gcs_path or not html_template_gcs_path.startswith("gs://"): raise ValueError("Invalid TemplateURL.")
        path_parts = html_template_gcs_path.replace("gs://", "").split("/", 1)
        blob = gcs_client.bucket(path_parts[0]).blob(path_parts[1])
        if not blob.exists(): raise GCSNotFound(f"HTML Template not found: {html_template_gcs_path}")
        html_template_content = blob.download_as_text(encoding='utf-8')
    except GCSNotFound as e_gcs:
        raise HTTPException(status_code=404, detail=str(e_gcs))
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to load HTML template: {str(e)}")


    populated_html = html_template_content
    table_rows_html_str = ""
    
    body_field_names = [
        f_name for f_name in schema_fields_ordered 
        if field_configs_map.get(f_name, FieldDisplayConfig(field_name=f_name, include_in_body=True)).include_in_body
    ]
    if not body_field_names and data_rows_list: body_field_names = list(data_rows_list[0].keys())

    # --- HTML Generation with Subtotals/Totals based on FieldDisplayConfig ---
    grand_total_accumulators = {} 
    for f_name_acc, f_config_acc in field_configs_map.items():
        if f_name_acc in body_field_names and f_config_acc.numeric_aggregation and \
           schema_type_map.get(f_name_acc) in NUMERIC_TYPES_FOR_AGG:
            grand_total_accumulators[f_name_acc] = {"values": [], "config": f_config_acc}
    
    # Determine if any grand total action is set across all field configs
    # Or if any numeric field has an aggregation set (implies it should be part of a grand total)
    needs_grand_total_row_processing = any(
        (fc.group_summary_action in ['GRAND_TOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL']) or \
        (fc.numeric_aggregation and fc.field_name in body_field_names and schema_type_map.get(fc.field_name) in NUMERIC_TYPES_FOR_AGG)
        for fc in field_configs_map.values() if fc # Ensure fc is not None
    )
    
    # Using the first identified subtotal trigger field for grouping logic.
    primary_group_by_field_exec = subtotal_trigger_fields_from_config[0] if subtotal_trigger_fields_from_config else None
    current_group_value_exec_tracker = None 
    current_group_rows_for_calc_exec = [] 

    if data_rows_list:
        for i_exec_loop, row_data_item_loop in enumerate(data_rows_list): # Renamed
            # Subtotal Processing
            if primary_group_by_field_exec:
                field_config_for_group_by_exec = field_configs_map.get(primary_group_by_field_exec)
                if field_config_for_group_by_exec and field_config_for_group_by_exec.group_summary_action in ['SUBTOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL']:
                    new_group_value_tracker_exec = row_data_item_loop.get(primary_group_by_field_exec)
                    if current_group_value_exec_tracker is not None and new_group_value_tracker_exec != current_group_value_exec_tracker:
                        # --- Group break: Render subtotal for PREVIOUS group ---
                        subtotal_row_html_str_exec = "<tr class='subtotal-row' style='font-weight:bold; background-color:#f0f0f0;'>\n"
                        first_cell_of_subtotal = True
                        for col_idx_sub_exec, header_key_sub_exec in enumerate(body_field_names):
                            if first_cell_of_subtotal: 
                                subtotal_row_html_str_exec += f"  <td style='padding-left: 10px;'>Subtotal for {current_group_value_exec_tracker}:</td>\n"
                                first_cell_of_subtotal = False
                                continue # Only one label cell, then values or empty
                            
                            field_config_sub_exec = field_configs_map.get(header_key_sub_exec)
                            if field_config_sub_exec and field_config_sub_exec.numeric_aggregation and schema_type_map.get(header_key_sub_exec) in NUMERIC_TYPES_FOR_AGG:
                                group_numeric_values_sub_exec = []
                                for g_row_sub_exec in current_group_rows_for_calc_exec:
                                    val_sub_exec = g_row_sub_exec.get(header_key_sub_exec)
                                    if val_sub_exec is not None:
                                        try: group_numeric_values_sub_exec.append(Decimal(str(val_sub_exec)))
                                        except InvalidOperation: pass
                                agg_val_sub_exec = calculate_aggregate(group_numeric_values_sub_exec, field_config_sub_exec.numeric_aggregation)
                                fmt_val_sub_exec = format_value(agg_val_sub_exec, field_config_sub_exec.number_format, schema_type_map.get(header_key_sub_exec))
                                align_style_sub_exec = f"text-align: {field_config_sub_exec.alignment or 'right'};"
                                subtotal_row_html_str_exec += f"  <td style='{align_style_sub_exec}'>{fmt_val_sub_exec}</td>\n"
                            else:
                                subtotal_row_html_str_exec += "  <td></td>\n" 
                        subtotal_row_html_str_exec += "</tr>\n"
                        table_rows_html_str += subtotal_row_html_str_exec
                        current_group_rows_for_calc_exec = [] 
                    current_group_value_exec_tracker = new_group_value_tracker_exec
                    current_group_rows_for_calc_exec.append(row_data_item_loop)

            # Append current data row HTML
            row_html_str_item_exec = "<tr>\n"
            for header_key_item_exec in body_field_names:
                cell_value_item_exec = row_data_item_loop.get(header_key_item_exec)
                field_config_item_exec = field_configs_map.get(header_key_item_exec)
                field_type_item_exec = schema_type_map.get(header_key_item_exec, "STRING")
                formatted_value_item_exec = format_value(cell_value_item_exec, field_config_item_exec.number_format if field_config_item_exec else None, field_type_item_exec)
                style_attribute_item_exec = ""
                if field_config_item_exec and field_config_item_exec.alignment: style_attribute_item_exec = f"style='text-align: {field_config_item_exec.alignment};'"
                elif field_type_item_exec in NUMERIC_TYPES_FOR_AGG and (not field_config_item_exec or not field_config_item_exec.alignment): style_attribute_item_exec = "style='text-align: right;'"
                row_html_str_item_exec += f"  <td {style_attribute_item_exec}>{formatted_value_item_exec}</td>\n"
                
                if header_key_item_exec in grand_total_accumulators and cell_value_item_exec is not None:
                    try: grand_total_accumulators[header_key_item_exec]["values"].append(Decimal(str(cell_value_item_exec)))
                    except InvalidOperation: pass
            row_html_str_item_exec += "</tr>\n"
            table_rows_html_str += row_html_str_item_exec

        # After loop, process last subtotal group
        if primary_group_by_field_exec and current_group_rows_for_calc_exec:
            field_config_for_group_by_final_exec = field_configs_map.get(primary_group_by_field_exec)
            if field_config_for_group_by_final_exec and field_config_for_group_by_final_exec.group_summary_action in ['SUBTOTAL_ONLY', 'SUBTOTAL_AND_GRAND_TOTAL']:
                subtotal_row_html_final_exec = "<tr class='subtotal-row' style='font-weight:bold; background-color:#f0f0f0;'>\n"
                first_cell_of_final_subtotal = True
                for col_idx_final_exec, header_key_final_exec in enumerate(body_field_names):
                    if first_cell_of_final_subtotal:
                        subtotal_row_html_final_exec += f"  <td style='padding-left: 10px;'>Subtotal for {current_group_value_exec_tracker}:</td>\n"
                        first_cell_of_final_subtotal = False
                        continue
                    field_config_final_exec = field_configs_map.get(header_key_final_exec)
                    if field_config_final_exec and field_config_final_exec.numeric_aggregation and schema_type_map.get(header_key_final_exec) in NUMERIC_TYPES_FOR_AGG:
                        group_numeric_values_final_exec = []
                        for g_row_final_exec in current_group_rows_for_calc_exec:
                            val_final_exec = g_row_final_exec.get(header_key_final_exec)
                            if val_final_exec is not None:
                                try: group_numeric_values_final_exec.append(Decimal(str(val_final_exec)))
                                except InvalidOperation: pass
                        agg_val_final_exec = calculate_aggregate(group_numeric_values_final_exec, field_config_final_exec.numeric_aggregation)
                        fmt_val_final_exec = format_value(agg_val_final_exec, field_config_final_exec.number_format, schema_type_map.get(header_key_final_exec))
                        align_style_final_exec = f"text-align: {field_config_final_exec.alignment or 'right'};"
                        subtotal_row_html_final_exec += f"  <td style='{align_style_final_exec}'>{fmt_val_final_exec}</td>\n"
                    else:
                        subtotal_row_html_final_exec += "  <td></td>\n"
                subtotal_row_html_final_exec += "</tr>\n"
                table_rows_html_str += subtotal_row_html_final_exec
        
        # --- Grand Total Row Processing ---
        if needs_grand_total_row_processing: # Check the flag
            grand_total_row_html_str_exec = "<tr class='grand-total-row' style='font-weight:bold; background-color:#e0e0e0; border-top: 2px solid #aaa;'>\n"
            first_cell_of_grand_total = True
            for col_idx_gt_exec, header_key_gt_exec in enumerate(body_field_names):
                if first_cell_of_grand_total:
                    grand_total_row_html_str_exec += f"  <td style='padding-left: 10px;'>Grand Total:</td>\n"
                    first_cell_of_grand_total = False
                    continue
                
                if header_key_gt_exec in grand_total_accumulators:
                    acc_gt_exec = grand_total_accumulators[header_key_gt_exec]
                    agg_val_gt_exec = calculate_aggregate(acc_gt_exec["values"], acc_gt_exec["config"].numeric_aggregation)
                    fmt_val_gt_exec = format_value(agg_val_gt_exec, acc_gt_exec["config"].number_format, schema_type_map.get(header_key_gt_exec))
                    align_style_gt_exec = f"text-align: {acc_gt_exec['config'].alignment or 'right'};"
                    grand_total_row_html_str_exec += f"  <td style='{align_style_gt_exec}'>{fmt_val_gt_exec}</td>\n"
                else:
                    grand_total_row_html_str_exec += "  <td></td>\n" 
            grand_total_row_html_str_exec += "</tr>\n"
            table_rows_html_str += grand_total_row_html_str_exec

    elif not data_rows_list:
        colspan = len(body_field_names) if body_field_names else 1
        table_rows_html_str = f"<tr><td colspan='{colspan}' style='text-align:center; padding: 20px;'>No data found for the selected criteria.</td></tr>"
    
    populated_html = populated_html.replace("{{TABLE_ROWS_HTML_PLACEHOLDER}}", table_rows_html_str)

    # Populate Explicit Calculation Rows
    if parsed_calculation_row_configs:
        for calc_row_conf_item_expl in parsed_calculation_row_configs:
            calculated_row_html_cells_expl = ""
            for val_conf_item_expl in calc_row_conf_item_expl.calculated_values:
                numeric_col_data_calc_expl = []
                if data_rows_list:
                    target_col_data_values_expl = [rd.get(val_conf_item_expl.target_field_name) for rd in data_rows_list if rd.get(val_conf_item_expl.target_field_name) is not None]
                    for x_calc_expl in target_col_data_values_expl:
                        try: numeric_col_data_calc_expl.append(Decimal(str(x_calc_expl)))
                        except InvalidOperation: pass
                
                calculated_result_expl = calculate_aggregate(numeric_col_data_calc_expl, val_conf_item_expl.calculation_type.value)
                field_type_calc_expl = schema_type_map.get(val_conf_item_expl.target_field_name, "STRING")
                num_fmt_calc_expl = val_conf_item_expl.number_format if val_conf_item_expl.calculation_type not in [CalculationType.COUNT, CalculationType.COUNT_DISTINCT] else 'INTEGER'
                formatted_calc_value_expl = format_value(calculated_result_expl, num_fmt_calc_expl, field_type_calc_expl)
                align_style_calc_expl = ""
                if val_conf_item_expl.alignment: align_style_calc_expl = f"text-align: {val_conf_item_expl.alignment};"
                elif field_type_calc_expl in NUMERIC_TYPES_FOR_AGG: align_style_calc_expl = "text-align: right;"
                calculated_row_html_cells_expl += f"<td style='{align_style_calc_expl}'>{formatted_calc_value_expl}</td>\n"
            
            populated_html = populated_html.replace(f"{{{{{calc_row_conf_item_expl.values_placeholder_name}}}}}", calculated_row_html_cells_expl)

    # Populate TOP_ and HEADER_ placeholders
    for fc_name_ph_final, fc_config_obj_ph_final in field_configs_map.items():
        val_raw_ph_final, val_fmt_ph_final = None, "" 
        if fc_config_obj_ph_final.field_name in applied_filter_values_for_template:
            val_raw_ph_final = applied_filter_values_for_template[fc_config_obj_ph_final.field_name]
            val_fmt_ph_final = str(val_raw_ph_final) 
        elif data_rows_list and fc_config_obj_ph_final.field_name in data_rows_list[0]:
            val_raw_ph_final = data_rows_list[0][fc_config_obj_ph_final.field_name]
            val_fmt_ph_final = format_value(val_raw_ph_final, fc_config_obj_ph_final.number_format, schema_type_map.get(fc_config_obj_ph_final.field_name, "STRING"))
        else: val_fmt_ph_final = "" 

        if fc_config_obj_ph_final.include_at_top: populated_html = populated_html.replace(f"{{{{TOP_{fc_config_obj_ph_final.field_name}}}}}", val_fmt_ph_final)
        if fc_config_obj_ph_final.include_in_header: populated_html = populated_html.replace(f"{{{{HEADER_{fc_config_obj_ph_final.field_name}}}}}", val_fmt_ph_final)

    if "{{REPORT_TITLE_PLACEHOLDER}}" in populated_html:
         populated_html = populated_html.replace("{{REPORT_TITLE_PLACEHOLDER}}", f"Report: {report_definition_name.replace('_', ' ').title()}")
    if "{{CURRENT_DATE_PLACEHOLDER}}" in populated_html:
        populated_html = populated_html.replace("{{CURRENT_DATE_PLACEHOLDER}}", datetime.date.today().isoformat())

    report_id = str(uuid.uuid4())
    generated_reports_store[report_id] = populated_html
    report_url_path = f"/view_generated_report/{report_id}"
    print(f"INFO: Generated report for '{report_definition_name}' ID {report_id}. URL path: {report_url_path}")
    return JSONResponse(content={"report_url_path": report_url_path})


@app.get("/view_generated_report/{report_id}", response_class=HTMLResponse)
async def view_generated_report_endpoint(report_id: str):
    html_content = generated_reports_store.get(report_id)
    if not html_content: raise HTTPException(status_code=404, detail="Report not found or expired.")
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    print("INFO: Starting Uvicorn server for GenAI Report API.")
    default_port = int(os.getenv("PORT", "8080"))
    uvicorn.run("app:app", host="0.0.0.0", port=default_port, reload=True, workers=1)
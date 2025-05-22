import datetime
import base64
# import inspect # Not used in your download.py
import json
import os
# import pathlib # Not used in your download.py
import re # For placeholder discovery
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
from google.api_core import exceptions as google_api_exceptions # For more specific error handling

import vertexai
from vertexai.generative_models import GenerativeModel, Part, Image
from vertexai.generative_models import HarmCategory, HarmBlockThreshold, GenerationConfig

# --- AppConfig & Global Configs (from your download.py) ---
class AppConfig:
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "")
    gcp_location: str = os.getenv("GCP_LOCATION", "")
    default_system_instruction_text: str = ""
    vertex_ai_initialized: bool = False
    bigquery_client: Union[bigquery.Client, None] = None
    storage_client: Union[storage.Client, None] = None
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GCS_SYSTEM_INSTRUCTION_PATH: str = os.getenv("GCS_SYSTEM_INSTRUCTION_PATH", "system_instructions/default_system_instruction.txt")
    TARGET_GEMINI_MODEL: str = os.getenv("GEMINI_MODEL_OVERRIDE", "gemini-2.5-pro-preview-05-06") # User-specified

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

# DEFAULT_FALLBACK_SYSTEM_INSTRUCTION from your download.py
DEFAULT_FALLBACK_SYSTEM_INSTRUCTION = """You are a helpful assistant that generates HTML templates for reports.
The user will provide a prompt, an image for styling guidance, a data schema, field display instructions (including alignment and number formatting preferences), calculation row instructions, and subtotal instructions.
Your output should be a complete, well-structured HTML document with CSS and minimal JavaScript (for presentation only, not data fetching).
Use clear placeholders for data injection:
- For table data rows: '{{TABLE_ROWS_HTML_PLACEHOLDER}}'.
- For specific header/summary values: '{{TOP_FieldName}}' or '{{HEADER_FieldName}}'.
- For calculation row values sections: as specified by 'values_placeholder_name' in the instructions (e.g., '{{TOTAL_FEES_VALUES_PLACEHOLDER}}').
- For subtotal row values sections: as specified by 'values_placeholder_name' in the instructions (e.g., '{{SUBTOTAL_CategoryX_VALUES_PLACEHOLDER}}').

When generating CSS or table cell attributes, try to respect styling hints (alignment, number format) provided for fields.
For calculation and subtotal rows, include the provided row label. For the values part of these rows, use the specified placeholder. Ensure the table structure (e.g., tfoot, appropriate colspan for labels, and sufficient cells for value placeholders) supports these summary rows. Subtotal rows should appear after each group of data based on the specified grouping field.
Ensure the HTML is clean and adheres to modern web standards.
Your answer should be ONLY CODE. No Descriptions, no explanations. Just HTML, CSS and Javascript code.
"""

# --- Pydantic Models (from your download.py) ---
class CalculationType(str, Enum):
    SUM = "SUM"; AVERAGE = "AVERAGE"; COUNT = "COUNT"; COUNT_DISTINCT = "COUNT_DISTINCT"; MIN = "MIN"; MAX = "MAX"
class CalculatedValueConfig(BaseModel):
    target_field_name: str; calculation_type: CalculationType
    number_format: Optional[str] = None; alignment: Optional[str] = None
class CalculationRowConfig(BaseModel):
    row_label: str; values_placeholder_name: str; calculated_values: List[CalculatedValueConfig]
class SubtotalConfig(BaseModel):
    group_by_field_name: str; values_placeholder_name: str; calculated_values: List[CalculatedValueConfig]
class FieldDisplayConfig(BaseModel): # As in your download.py (no group_summary_action, numeric_aggregation)
    field_name: str; include_in_body: bool = Field(default=True); include_at_top: bool = Field(default=False)
    include_in_header: bool = Field(default=False); context_note: Optional[str] = None
    alignment: Optional[str] = None; number_format: Optional[str] = None
class ReportDefinitionPayload(BaseModel):
    report_name: str; image_url: str; sql_query: str; prompt: str
    field_display_configs: Optional[List[FieldDisplayConfig]] = None
    user_attribute_mappings: Optional[Dict[str, str]] = Field(default_factory=dict)
    calculation_row_configs: Optional[List[CalculationRowConfig]] = None
    subtotal_configs: Optional[List[SubtotalConfig]] = None # This was Optional in your download.py
    optimized_prompt: Optional[str] = None; header_text: Optional[str] = None; footer_text: Optional[str] = None
class ExecuteReportPayload(BaseModel):
    report_definition_name: str; filter_criteria_json: str = Field(default="{}")
class ReportDefinitionListItem(BaseModel):
    ReportName: str; Prompt: Optional[str] = None; SQL: Optional[str] = None; ScreenshotURL: Optional[str] = None
    TemplateURL: Optional[str] = None; BaseQuerySchemaJSON: Optional[str] = None
    UserAttributeMappingsJSON: Optional[str] = None; FieldDisplayConfigsJSON: Optional[str] = None
    CalculationRowConfigsJSON: Optional[str] = None; SubtotalConfigsJSON: Optional[str] = None
    LastGeneratedTimestamp: Optional[datetime.datetime] = None
class SystemInstructionPayload(BaseModel): system_instruction: str
class SqlQueryPayload(BaseModel): sql_query: str

# New Pydantic models for Placeholder Discovery
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
    report_name: str
    placeholders: List[DiscoveredPlaceholderInfo]
    template_found: bool
    error_message: Optional[str] = None

@asynccontextmanager
async def lifespan(app_fastapi: FastAPI): # From your download.py
    print("INFO: FastAPI application startup...")
    global config
    config.gcp_project_id = os.getenv("GCP_PROJECT_ID", config.gcp_project_id)
    config.gcp_location = os.getenv("GCP_LOCATION", config.gcp_location)
    config.GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", config.GCS_BUCKET_NAME)
    config.GCS_SYSTEM_INSTRUCTION_PATH = os.getenv("GCS_SYSTEM_INSTRUCTION_PATH", config.GCS_SYSTEM_INSTRUCTION_PATH)
    # Ensure TARGET_GEMINI_MODEL uses the class default if override is not set or empty
    env_model_override = os.getenv("GEMINI_MODEL_OVERRIDE")
    config.TARGET_GEMINI_MODEL = env_model_override if env_model_override else config.TARGET_GEMINI_MODEL
    if not config.TARGET_GEMINI_MODEL: # Final fallback if class default was also somehow empty
        config.TARGET_GEMINI_MODEL = "gemini-2.5-pro-preview-05-06"

    print(f"INFO: Target Gemini Model: {config.TARGET_GEMINI_MODEL}")
    if not config.gcp_project_id: print("ERROR: GCP_PROJECT_ID environment variable not set.")
    if not config.gcp_location: print("ERROR: GCP_LOCATION environment variable not set.")
    print(f"INFO: Target GCS Bucket: {config.GCS_BUCKET_NAME or 'NOT SET'}")
    print(f"INFO: System Instruction GCS Path: gs://{config.GCS_BUCKET_NAME}/{config.GCS_SYSTEM_INSTRUCTION_PATH}")
    if not config.GCS_BUCKET_NAME: config.storage_client = None
    elif storage:
        try: config.storage_client = storage.Client(project=config.gcp_project_id if config.gcp_project_id else None); print("INFO: GCS Client initialized.")
        except Exception as e: print(f"FATAL: GCS Client init error: {e}"); config.storage_client = None
    else: config.storage_client = None
    if config.storage_client: config.default_system_instruction_text = _load_system_instruction_from_gcs(config.storage_client, config.GCS_BUCKET_NAME, config.GCS_SYSTEM_INSTRUCTION_PATH)
    else: config.default_system_instruction_text = DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    if config.default_system_instruction_text: print(f"INFO: System instruction loaded (length: {len(config.default_system_instruction_text)} chars). Preview: '{config.default_system_instruction_text[:100]}...'")
    else: print("ERROR: System instruction is empty.")
    if config.gcp_project_id and config.gcp_location:
        try: vertexai.init(project=config.gcp_project_id, location=config.gcp_location); config.vertex_ai_initialized = True; print("INFO: Vertex AI SDK initialized.")
        except Exception as e: print(f"FATAL: Vertex AI SDK Init Error: {e}"); config.vertex_ai_initialized = False
    else: print("ERROR: Vertex AI SDK prerequisites not met."); config.vertex_ai_initialized = False
    if bigquery and config.gcp_project_id:
        try: config.bigquery_client = bigquery.Client(project=config.gcp_project_id); print("INFO: BigQuery Client initialized.")
        except Exception as e: print(f"FATAL: BigQuery Client init error: {e}"); config.bigquery_client = None
    else: print(f"ERROR: BigQuery prerequisites not met."); config.bigquery_client = None
    yield
    print("INFO: FastAPI application shutdown.")

app = FastAPI(lifespan=lifespan)

# CORS Configuration from your download.py, ensuring current NGROK is default
NGROK_URL_FROM_ENV = os.getenv("FRONTEND_NGROK_URL", "https://c530-207-216-175-143.ngrok-free.app") # Your new NGROK default
LOOKER_INSTANCE_URL_FROM_ENV = os.getenv("LOOKER_INSTANCE_URL", "https://igmprinting.cloud.looker.com")
LOOKER_EXTENSION_SANDBOX_HOST = "https://83d14716-08b3-4def-bdca-54f4b2af9f19-extensions.cloud.looker.com"

allowed_origins_list = [ "http://localhost:8080" ]
if NGROK_URL_FROM_ENV: allowed_origins_list.append(NGROK_URL_FROM_ENV)
if LOOKER_INSTANCE_URL_FROM_ENV: allowed_origins_list.append(LOOKER_INSTANCE_URL_FROM_ENV)
if LOOKER_EXTENSION_SANDBOX_HOST: allowed_origins_list.append(LOOKER_EXTENSION_SANDBOX_HOST)
allowed_origins_list = sorted(list(set(o for o in allowed_origins_list if o and o.startswith("http"))))
if not allowed_origins_list: allowed_origins_list = ["http://localhost:8080"]

print(f"INFO: CORS allow_origins effectively configured for: {allowed_origins_list}")
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins_list, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def _load_system_instruction_from_gcs(client: storage.Client, bucket_name: str, blob_name: str) -> str: # From your download.py
    if not client or not bucket_name: print(f"WARN: GCS client/bucket not provided."); return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    try:
        blob = client.bucket(bucket_name).blob(blob_name)
        if blob.exists(): print(f"INFO: Loaded system instruction from gs://{bucket_name}/{blob_name}"); return blob.download_as_text(encoding='utf-8')
        print(f"WARN: System instruction file not found at gs://{bucket_name}/{blob_name}. Using fallback."); return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION
    except Exception as e: print(f"ERROR: Failed to load system instruction from GCS: {e}. Using fallback."); return DEFAULT_FALLBACK_SYSTEM_INSTRUCTION

def get_bigquery_client_dep(): # From your download.py
    if not config.bigquery_client: raise HTTPException(status_code=503, detail="BigQuery client not available.")
    return config.bigquery_client
def get_storage_client_dep(): # From your download.py
    if not config.storage_client: raise HTTPException(status_code=503, detail="GCS client not available.")
    return config.storage_client
def get_vertex_ai_initialized_flag(): # From your download.py
    if not config.vertex_ai_initialized: raise HTTPException(status_code=503, detail="Vertex AI SDK not initialized.")
    if not config.TARGET_GEMINI_MODEL: raise HTTPException(status_code=503, detail="TARGET_GEMINI_MODEL not configured.")


def remove_first_and_last_lines(s: str) -> str: # From your download.py
    if not s: return ""
    lines = s.splitlines();
    if len(lines) >= 2 and lines[0].strip().startswith("```") and lines[-1].strip() == "```": return '\n'.join(lines[1:-1])
    if len(lines) >= 1 and lines[0].strip().startswith("```"): return '\n'.join(lines[1:]) if len(lines) > 1 else ""
    return s

def generate_html_from_user_pattern( # From your download.py, with token fix
    prompt_text: str, image_bytes: bytes, image_mime_type: str, system_instruction_text: str
) -> Union[str, None]:
    get_vertex_ai_initialized_flag()
    print(f"DEBUG: Vertex AI: Using System Instruction (first 100 chars): {system_instruction_text[:100]}")
    print(f"DEBUG: Vertex AI: Target Model for generation: {config.TARGET_GEMINI_MODEL}")
    try:
        model_instance = GenerativeModel(model_name=config.TARGET_GEMINI_MODEL, system_instruction=[system_instruction_text] if system_instruction_text else None)
        image_part = Part.from_data(data=image_bytes, mime_type=image_mime_type)
        prompt_part = Part.from_text(text=prompt_text)
        contents_for_gemini = [prompt_part, image_part]
        safety_settings_config = { category: HarmBlockThreshold.BLOCK_NONE for category in HarmCategory }
        # Using max_output_tokens from your download.py (which was 65535)
        generation_config_obj = GenerationConfig(temperature=1.0, top_p=0.95, max_output_tokens=65535, candidate_count=1)
        response = model_instance.generate_content(contents=contents_for_gemini, generation_config=generation_config_obj, safety_settings=safety_settings_config, stream=True) # Using stream=True from your file
        generated_text_output = ""
        for chunk in response: # Iterate over stream
            try:
                if hasattr(chunk, 'text') and chunk.text: generated_text_output += chunk.text
                elif hasattr(chunk, 'candidates') and chunk.candidates:
                    for candidate in chunk.candidates:
                        if hasattr(candidate, 'content') and candidate.content and \
                           hasattr(candidate.content, 'parts') and candidate.content.parts:
                            for part_item in candidate.content.parts:
                                if hasattr(part_item, 'text') and part_item.text: generated_text_output += part_item.text
            except Exception as e_parse: print(f"DEBUG: Vertex AI: Non-text/part chunk or parse error in stream: {type(chunk).__name__}, Details: {e_parse}")
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
    
    print(f"DEBUG: Raw Gemini Output before remove_first_and_last_lines (first 500 chars): {generated_text_output[:500]}")
    processed_html = remove_first_and_last_lines(generated_text_output)
    print(f"DEBUG: Processed Gemini Output after remove_first_and_last_lines (first 500 chars): {processed_html[:500]}")
    return processed_html if processed_html else ""

def convert_row_to_json_serializable(row: bigquery.Row) -> Dict[str, Any]: # From your download.py
    output = {}; 
    for key, value in row.items():
        if isinstance(value, Decimal): output[key] = str(value)
        elif isinstance(value, (datetime.date, datetime.datetime, datetime.time)): output[key] = value.isoformat()
        elif isinstance(value, bytes): output[key] = base64.b64encode(value).decode('utf-8')
        elif isinstance(value, list): output[key] = [(item.isoformat() if isinstance(item, (datetime.date, datetime.datetime, datetime.time)) else str(item) if isinstance(item, Decimal) else item) for item in value]
        else: output[key] = value
    return output

def get_bq_param_type_and_value(value_str_param: Any, bq_col_name: str, type_hint: str): # From your download.py
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

@app.get("/")
async def read_root(request: Request):
    print(f"DEBUG /: Incoming request origin: {request.headers.get('origin')}")
    return {"status": f"GenAI Report API is running! (Target Model: {config.TARGET_GEMINI_MODEL})"}

@app.post("/dry_run_sql_for_schema")
async def dry_run_sql_for_schema_endpoint(
    request: Request, payload: SqlQueryPayload, bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
):
    print(f"DEBUG /dry_run_sql_for_schema: Incoming request origin: {request.headers.get('origin')}")
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

# --- New Placeholder Discovery Endpoint ---
@app.get("/report_definitions/{report_name}/discover_placeholders", response_model=DiscoverPlaceholdersResponse)
async def discover_template_placeholders(
    report_name: str, request: Request, 
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep)
):
    print(f"INFO /discover_placeholders for '{report_name}': Origin: {request.headers.get('origin')}")
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
        print(f"DEBUG: HTML Content read by /discover_placeholders for {report_name} (first 500 chars): {html_content[:500]}")
    except Exception as e: return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=[], template_found=False, error_message=f"Error loading template from GCS: {str(e)}")
    
    # Using the older FieldDisplayConfig model for parsing here, as per user's download.py context
    field_display_configs: List[FieldDisplayConfig] = [] 
    if field_configs_json_str:
        try: field_display_configs = [FieldDisplayConfig(**item) for item in json.loads(field_configs_json_str)]
        except (json.JSONDecodeError, TypeError) as e: print(f"WARN: Could not parse FieldDisplayConfigsJSON for '{report_name}' in discover (using older model): {e}")
    
    calculation_rows_configs: List[CalculationRowConfig] = []
    if calc_row_configs_json_str:
        try: calculation_rows_configs = [CalculationRowConfig(**item) for item in json.loads(calc_row_configs_json_str)]
        except (json.JSONDecodeError, TypeError) as e: print(f"WARN: Could not parse CalculationRowConfigsJSON for '{report_name}' in discover: {e}")
    
    found_placeholder_keys = set(re.findall(r"\{\{([^}]+)\}\}", html_content, re.DOTALL))
    print(f"DEBUG: Keys found by regex in /discover_placeholders: {found_placeholder_keys}")
    discovered_placeholders: List[DiscoveredPlaceholderInfo] = []
    for key_in_tag_raw in found_placeholder_keys:
        key_in_tag = key_in_tag_raw.strip(); full_tag = f"{{{{{key_in_tag_raw}}}}}"; status = "unrecognized"; suggestion = None
        if key_in_tag == "TABLE_ROWS_HTML_PLACEHOLDER": status = "standard_table_rows"; suggestion = PlaceholderMappingSuggestion(map_to_type="standard_placeholder", map_to_value=key_in_tag)
        else:
            for fd_config in field_display_configs: # fd_config is OLDER FieldDisplayConfig model here
                if key_in_tag == f"TOP_{fd_config.field_name}" and fd_config.include_at_top: status = "auto_matched_top"; suggestion = PlaceholderMappingSuggestion(map_to_type="schema_field", map_to_value=fd_config.field_name, usage_as="TOP"); break
                if key_in_tag == f"HEADER_{fd_config.field_name}" and fd_config.include_in_header: status = "auto_matched_header"; suggestion = PlaceholderMappingSuggestion(map_to_type="schema_field", map_to_value=fd_config.field_name, usage_as="HEADER"); break
            if status == "unrecognized":
                for calc_config in calculation_rows_configs:
                    if key_in_tag == calc_config.values_placeholder_name: status = "auto_matched_calc_row"; suggestion = PlaceholderMappingSuggestion(map_to_type="calculation_row_placeholder", map_to_value=key_in_tag); break
        discovered_placeholders.append(DiscoveredPlaceholderInfo(original_tag=full_tag,key_in_tag=key_in_tag,status=status,suggestion=suggestion))
    discovered_placeholders.sort(key=lambda p: p.key_in_tag)
    return DiscoverPlaceholdersResponse(report_name=report_name, placeholders=discovered_placeholders, template_found=True)

@app.post("/report_definitions", status_code=201)
async def upsert_report_definition(
    payload: ReportDefinitionPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep),
    _vertex_ai_init_check: None = Depends(get_vertex_ai_initialized_flag)
):
    # Using Pydantic models from your download.py for this section
    report_name = payload.report_name; base_sql_query = payload.sql_query; base_user_prompt = payload.prompt
    image_url = payload.image_url; field_display_configs_from_payload = payload.field_display_configs
    calculation_row_configs_from_payload = payload.calculation_row_configs
    subtotal_configs_from_payload = payload.subtotal_configs # From download.py, can be None
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

    effective_field_display_configs: List[FieldDisplayConfig] = [] # List of OLDER FieldDisplayConfig model
    if field_display_configs_from_payload:
        valid_configs = []
        for fc_data_obj in field_display_configs_from_payload: # fc_data_obj is new model from frontend
            # Create a dict with only known fields for the OLDER FieldDisplayConfig model
            # This means group_summary_action and numeric_aggregation sent by frontend WILL BE DROPPED HERE
            known_fc_data = {
                key: value for key, value in fc_data_obj.model_dump(exclude_unset=True).items() 
                if key in FieldDisplayConfig.model_fields # Pydantic v2
            }
            if 'field_name' not in known_fc_data: # Should not happen if frontend sends it
                 print(f"WARN: field_name missing in a field_display_config item: {fc_data_obj}")
                 continue
            valid_configs.append(FieldDisplayConfig(**known_fc_data))
        effective_field_display_configs = valid_configs
    elif schema_from_dry_run_list:
        effective_field_display_configs = [FieldDisplayConfig(field_name=f["name"]) for f in schema_from_dry_run_list]
    
    prompt_for_template = base_user_prompt
    prompt_for_template += f"\n\n--- Data Schema ---\n{schema_for_gemini_prompt_str}\n--- End Data Schema ---"
    if effective_field_display_configs: # Prompting based on OLDER FieldDisplayConfig
        prompt_for_template += "\n\n--- Field Display & Placeholder Instructions ---"
        body_fields_prompt_parts, top_fields_prompt_parts, header_fields_prompt_parts = [], [], []
        for config_item in effective_field_display_configs:
            style_hints = [s for s in [f"align: {config_item.alignment}" if config_item.alignment else "", f"format: {config_item.number_format}" if config_item.number_format else ""] if s]
            field_info = f"- `{config_item.field_name}`"
            if style_hints: field_info += f" (Styling: {'; '.join(style_hints)})"
            if config_item.context_note: field_info += f" (Context: {config_item.context_note})"
            if config_item.include_in_body: body_fields_prompt_parts.append(field_info)
            if config_item.include_at_top: top_fields_prompt_parts.append(field_info + f" -> Use placeholder: {{TOP_{config_item.field_name}}}")
            if config_item.include_in_header: header_fields_prompt_parts.append(field_info + f" -> Use placeholder: {{HEADER_{config_item.field_name}}}")
        if body_fields_prompt_parts: prompt_for_template += "\nBody Fields:\n" + "\n".join(body_fields_prompt_parts)
        if top_fields_prompt_parts: prompt_for_template += "\nTop Fields:\n" + "\n".join(top_fields_prompt_parts)
        if header_fields_prompt_parts: prompt_for_template += "\nHeader Fields:\n" + "\n".join(header_fields_prompt_parts)
        prompt_for_template += "\n--- End Field Instructions ---"
    if calculation_row_configs_from_payload: # Using prompt style from your download.py
        prompt_for_template += "\n\n--- Calculation Row Instructions (for Overall Summary Data) ---"
        prompt_for_template += "\nThe report should also include the following overall summary/calculation rows..."
        for i, calc_row_config in enumerate(calculation_row_configs_from_payload):
            value_descs = [f"{cv.calculation_type.value} of '{cv.target_field_name}'" for cv in calc_row_config.calculated_values]
            prompt_for_template += f"\n- Calculation Row {i+1}:\n  - Display Label: \"{calc_row_config.row_label}\"\n  - Values Placeholder Name: `{{{{{calc_row_config.values_placeholder_name}}}}}` for: {'; '.join(value_descs)}."
    if subtotal_configs_from_payload: # Using prompt style from your download.py
        prompt_for_template += "\n\n--- Subtotal Row Instructions (for Grouped Data) ---"
        prompt_for_template += "\nThe report may also require subtotal rows..."
        for i, subtotal_conf in enumerate(subtotal_configs_from_payload):
            value_descs = [f"{cv.calculation_type.value} of '{cv.target_field_name}'" for cv in subtotal_conf.calculated_values]
            prompt_for_template += f"\n- Subtotal Group {i+1}:\n  - Group By Field: `{subtotal_conf.group_by_field_name}`.\n  - Values Placeholder Name: `{{{{{subtotal_conf.values_placeholder_name}}}}}` for values: {'; '.join(value_descs)}."
    
    prompt_for_template += """

--- HTML Template Generation Guidelines (Final Reminder) ---
Output ONLY the raw HTML code. No descriptions, no explanations, no markdown like ```html ... ```.
Start with `<!DOCTYPE html>` or `<html>` and end with `</html>`.
"""
    try:
        async with httpx.AsyncClient(timeout=180.0) as client_httpx:
            img_response = await client_httpx.get(image_url); img_response.raise_for_status()
            image_bytes_data = await img_response.aread()
            image_mime_type_data = img_response.headers.get("Content-Type", "application/octet-stream").lower()
            if not image_mime_type_data.startswith("image/"): raise ValueError(f"Content-Type from URL is not valid.")
    except Exception as e: raise HTTPException(status_code=400, detail=f"Error fetching image URL '{image_url}': {str(e)}")

    html_template_content = generate_html_from_user_pattern(prompt_text=prompt_for_template, image_bytes=image_bytes_data, image_mime_type=image_mime_type_data, system_instruction_text=config.default_system_instruction_text)
    if not html_template_content or not html_template_content.strip(): 
        print("WARNING: Gemini returned empty content. Using fallback HTML for GCS upload.")
        html_template_content = "<html><body><p>Error: AI failed to generate valid HTML. Placeholders will be missing.</p></body></html>"
    print(f"DEBUG: HTML Content for GCS Upload (report: {report_name}, first 500 chars): {html_template_content[:500]}")

    report_gcs_path_safe = report_name.replace(" ", "_").replace("/", "_").lower(); base_gcs_folder = f"report_templates/{report_gcs_path_safe}"
    template_gcs_path_str = f"{base_gcs_folder}/template.html"; base_sql_gcs_path_str = f"{base_gcs_folder}/base_query.sql"
    schema_gcs_path_str = f"{base_gcs_folder}/schema.json"; field_configs_gcs_path_str = f"{base_gcs_folder}/field_display_configs.json" # Stores OLDER structure
    calc_row_configs_gcs_path_str = f"{base_gcs_folder}/calculation_row_configs.json"; subtotal_configs_gcs_path_str = f"{base_gcs_folder}/subtotal_configs.json"
    schema_json_to_save = json.dumps(schema_from_dry_run_list, indent=2)
    field_display_configs_json_to_save = json.dumps([fc.model_dump(exclude_unset=True) for fc in effective_field_display_configs], indent=2) if effective_field_display_configs else "[]"
    calculation_row_configs_json_to_save = json.dumps([crc.model_dump(exclude_unset=True) for crc in payload.calculation_row_configs], indent=2) if payload.calculation_row_configs else "[]"
    subtotal_configs_json_to_save = json.dumps([sc.model_dump(exclude_unset=True) for sc in payload.subtotal_configs], indent=2) if payload.subtotal_configs else "[]"
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME); bucket.blob(template_gcs_path_str).upload_from_string(html_template_content, content_type='text/html; charset=utf-8')
        bucket.blob(base_sql_gcs_path_str).upload_from_string(base_sql_query, content_type='application/sql; charset=utf-8')
        bucket.blob(schema_gcs_path_str).upload_from_string(schema_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(field_configs_gcs_path_str).upload_from_string(field_display_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(calc_row_configs_gcs_path_str).upload_from_string(calculation_row_configs_json_to_save, content_type='application/json; charset=utf-8')
        bucket.blob(subtotal_configs_gcs_path_str).upload_from_string(subtotal_configs_json_to_save, content_type='application/json; charset=utf-8')
        print(f"INFO: Saved template artifacts for '{report_name}' to GCS.")
    except Exception as e: print(f"ERROR: GCS Upload Failed: {str(e)}"); raise HTTPException(status_code=500, detail=f"Failed to save files to GCS: {e}")
    table_id = f"`{config.gcp_project_id}.report_printing.report_list`"
    merge_sql = f"""MERGE {table_id} T USING (SELECT @report_name AS ReportName, @prompt AS Prompt, @sql_query AS SQL, @image_url AS ScreenshotURL, @template_gcs_path AS TemplateURL, @base_sql_gcs_path AS BaseQueryGCSPath, @schema_gcs_path AS SchemaGCSPath, @schema_json AS BaseQuerySchemaJSON, @field_display_configs_json AS FieldDisplayConfigsJSON, @calculation_row_configs_json AS CalculationRowConfigsJSON, @subtotal_configs_json AS SubtotalConfigsJSON, @user_attribute_mappings_json AS UserAttributeMappingsJSON, @optimized_prompt AS OptimizedPrompt, @header_text AS Header, @footer_text AS Footer, CURRENT_TIMESTAMP() AS CurrentTs) S ON T.ReportName = S.ReportName WHEN MATCHED THEN UPDATE SET Prompt = S.Prompt, SQL = S.SQL, ScreenshotURL = S.ScreenshotURL, TemplateURL = S.TemplateURL, BaseQueryGCSPath = S.BaseQueryGCSPath, SchemaGCSPath = S.SchemaGCSPath, BaseQuerySchemaJSON = S.BaseQuerySchemaJSON, FieldDisplayConfigsJSON = S.FieldDisplayConfigsJSON, CalculationRowConfigsJSON = S.CalculationRowConfigsJSON, SubtotalConfigsJSON = S.SubtotalConfigsJSON, UserAttributeMappingsJSON = S.UserAttributeMappingsJSON, OptimizedPrompt = S.OptimizedPrompt, Header = S.Header, Footer = S.Footer, LastGeneratedTimestamp = S.CurrentTs WHEN NOT MATCHED THEN INSERT (ReportName, Prompt, SQL, ScreenshotURL, TemplateURL, BaseQueryGCSPath, SchemaGCSPath, BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON, UserAttributeMappingsJSON, OptimizedPrompt, Header, Footer, CreatedTimestamp, LastGeneratedTimestamp) VALUES (S.ReportName, S.Prompt, S.SQL, S.ScreenshotURL, S.TemplateURL, S.BaseQueryGCSPath, S.SchemaGCSPath, S.BaseQuerySchemaJSON, S.FieldDisplayConfigsJSON, S.CalculationRowConfigsJSON, S.SubtotalConfigsJSON, S.UserAttributeMappingsJSON, S.OptimizedPrompt, S.Header, S.Footer, S.CurrentTs, S.CurrentTs)"""
    merge_params = [ScalarQueryParameter("report_name", "STRING", payload.report_name), ScalarQueryParameter("prompt", "STRING", payload.prompt), ScalarQueryParameter("sql_query", "STRING", payload.sql_query), ScalarQueryParameter("image_url", "STRING", payload.image_url), ScalarQueryParameter("template_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{template_gcs_path_str}"), ScalarQueryParameter("base_sql_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{base_sql_gcs_path_str}"), ScalarQueryParameter("schema_gcs_path", "STRING", f"gs://{config.GCS_BUCKET_NAME}/{schema_gcs_path_str}"), ScalarQueryParameter("schema_json", "STRING", schema_json_to_save), ScalarQueryParameter("field_display_configs_json", "STRING", field_display_configs_json_to_save), ScalarQueryParameter("calculation_row_configs_json", "STRING", calculation_row_configs_json_to_save), ScalarQueryParameter("subtotal_configs_json", "STRING", subtotal_configs_json_to_save), ScalarQueryParameter("user_attribute_mappings_json", "STRING", user_attribute_mappings_json_str), ScalarQueryParameter("optimized_prompt", "STRING", payload.optimized_prompt), ScalarQueryParameter("header_text", "STRING", payload.header_text), ScalarQueryParameter("footer_text", "STRING", payload.footer_text)]
    try:
        print(f"DEBUG: Attempting BQ MERGE for report: {payload.report_name}")
        job = bq_client.query(merge_sql, job_config=bigquery.QueryJobConfig(query_parameters=merge_params)); job.result() 
        print(f"INFO: Successfully merged report definition '{payload.report_name}' into BigQuery.")
    except Exception as e:
        error_message = f"Failed to save report definition to BigQuery: {str(e)}"; bq_errors = [f"Reason: {err.get('reason', 'N/A')}, Message: {err.get('message', 'N/A')}" for err in getattr(e, 'errors', [])]; error_message += f" BigQuery Errors: {'; '.join(bq_errors)}" if bq_errors else ""
        print(f"ERROR: {error_message}"); raise HTTPException(status_code=500, detail=error_message)
    return {"message": f"Report definition '{report_name}' upserted.", "template_html_gcs_path": f"gs://{config.GCS_BUCKET_NAME}/{template_gcs_path_str}"}


@app.get("/report_definitions", response_model=List[ReportDefinitionListItem])
async def list_report_definitions_endpoint(bq_client: bigquery.Client = Depends(get_bigquery_client_dep)):
    query = f"SELECT ReportName, Prompt, SQL, ScreenshotURL, TemplateURL, BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON, UserAttributeMappingsJSON, LastGeneratedTimestamp FROM `{config.gcp_project_id}.report_printing.report_list` ORDER BY ReportName ASC"
    try:
        results = list(bq_client.query(query).result())
        processed_results = []
        for row_dict_item in [dict(row.items()) for row in results]:
            for json_field in ['BaseQuerySchemaJSON', 'FieldDisplayConfigsJSON', 'CalculationRowConfigsJSON', 'SubtotalConfigsJSON', 'UserAttributeMappingsJSON']:
                if row_dict_item.get(json_field) is None: row_dict_item[json_field] = "{}" if json_field == 'UserAttributeMappingsJSON' else "[]"
            try: processed_results.append(ReportDefinitionListItem(**row_dict_item))
            except Exception as pydantic_error: print(f"ERROR: Pydantic validation for report {row_dict_item.get('ReportName')}: {pydantic_error}. Data: {row_dict_item}"); continue 
        return processed_results
    except Exception as e: print(f"ERROR fetching report definitions: {e}"); raise HTTPException(status_code=500, detail=f"Failed to fetch report definitions: {str(e)}")


def format_value(value: Any, format_str: Optional[str], field_type_str: str) -> str:
    if value is None: return ""
    NUMERIC_TYPES_FOR_FORMATTING = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]
    field_type_upper = str(field_type_str).upper() if field_type_str else "UNKNOWN"
    if format_str and field_type_upper in NUMERIC_TYPES_FOR_FORMATTING:
        try:
            str_value = str(value) if not isinstance(value, (int, float, Decimal)) else value; num_value = Decimal(str_value)
            if format_str == 'INTEGER': return f"{num_value:,.0f}"
            elif format_str == 'DECIMAL_2': return f"{num_value:,.2f}"
            elif format_str == 'USD': return f"${num_value:,.2f}"
            elif format_str == 'EUR': return f"€{num_value:,.2f}"
            elif format_str == 'PERCENT_2': return f"{num_value * Decimal('100'):,.2f}%"
            else: return str(value)
        except (ValueError, TypeError, InvalidOperation) as e: print(f"WARN: Formatting error for '{value}' with format '{format_str}': {e}"); return str(value)
    return str(value)

NUMERIC_TYPES_FOR_AGG = ["INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"]

def calculate_aggregate(data_list: List[Decimal], agg_type_str_param: Optional[str]) -> Decimal:
    # Using the refined version from our discussion
    if not agg_type_str_param:
        # print(f"WARN: calculate_aggregate called with no agg_type_str_param. Returning 0.") # Keep this less verbose
        return Decimal('0')
    agg_type = agg_type_str_param.upper()
    if not data_list:
        if agg_type in ['COUNT', 'COUNT_DISTINCT']: return Decimal('0')
        return Decimal('0') 
    if agg_type == "SUM": return sum(data_list)
    elif agg_type == "AVERAGE": return sum(data_list) / len(data_list)
    elif agg_type == "MIN": return min(data_list)
    elif agg_type == "MAX": return max(data_list)
    elif agg_type == "COUNT": return Decimal(len(data_list))
    elif agg_type == "COUNT_DISTINCT": return Decimal(len(set(str(d) for d in data_list)))
    print(f"WARN: Unknown aggregation type '{agg_type_str_param}' received. Returning 0.")
    return Decimal('0')

@app.post("/execute_report")
async def execute_report_and_get_url(
    payload: ExecuteReportPayload,
    bq_client: bigquery.Client = Depends(get_bigquery_client_dep),
    gcs_client: storage.Client = Depends(get_storage_client_dep)
):
    # This version uses the Pydantic models from your download.py.
    # The subtotal/total logic here will primarily use parsed_subtotal_configs.
    # If parsed_subtotal_configs is empty (as it will be from new frontend submissions),
    # then no dynamic subtotals/totals will be generated by THIS specific logic block.
    # To enable totals/subtotals based on the NEW FieldDisplayConfig fields (group_summary_action, numeric_aggregation),
    # this execute_report_and_get_url function would need to be the more advanced one we developed previously.
    # For now, this reflects the structure from your download.py, which means subtotals/totals
    # are dependent on the content of SubtotalConfigsJSON.
    report_definition_name = payload.report_definition_name
    filter_criteria_json_str = payload.filter_criteria_json
    print(f"INFO: POST /execute_report for '{report_definition_name}'. Filters JSON: {filter_criteria_json_str}")

    query_def_sql = f"SELECT SQL, TemplateURL, UserAttributeMappingsJSON, BaseQuerySchemaJSON, FieldDisplayConfigsJSON, CalculationRowConfigsJSON, SubtotalConfigsJSON FROM `{config.gcp_project_id}.report_printing.report_list` WHERE ReportName = @report_name_param"
    def_params = [ScalarQueryParameter("report_name_param", "STRING", report_definition_name)]
    try:
        results = list(bq_client.query(query_def_sql, job_config=bigquery.QueryJobConfig(query_parameters=def_params)).result())
        if not results: raise HTTPException(status_code=404, detail=f"Report definition '{report_definition_name}' not found.")
        row = results[0]
        base_sql_query_from_db = row.get("SQL"); html_template_gcs_path = row.get("TemplateURL")
        user_attr_map_json = row.get("UserAttributeMappingsJSON"); bq_schema_json = row.get("BaseQuerySchemaJSON")
        field_configs_json = row.get("FieldDisplayConfigsJSON") # Based on OLDER FieldDisplayConfig model
        calculation_row_configs_json = row.get("CalculationRowConfigsJSON")
        subtotal_configs_json = row.get("SubtotalConfigsJSON") # This is what download.py's logic uses
    except Exception as e: raise HTTPException(status_code=500, detail=f"Error fetching report definition: {str(e)}")

    schema_type_map = {}; schema_fields_ordered = []
    if bq_schema_json:
        try:
            parsed_schema = json.loads(bq_schema_json)
            for field_info in parsed_schema:
                if 'name' in field_info and 'type' in field_info: 
                    schema_type_map[field_info['name']] = str(field_info['type']).upper()
                    schema_fields_ordered.append(field_info['name']) # Capture order
        except json.JSONDecodeError: print(f"WARN: Could not parse BaseQuerySchemaJSON for '{report_definition_name}'.")

    field_configs_map: Dict[str, FieldDisplayConfig] = {} # OLDER FieldDisplayConfig
    if field_configs_json:
        try:
            for item_dict in json.loads(field_configs_json):
                known_item_data = {k: v for k, v in item_dict.items() if k in FieldDisplayConfig.model_fields}
                if 'field_name' in known_item_data:
                    field_configs_map[known_item_data['field_name']] = FieldDisplayConfig(**known_item_data)
        except (json.JSONDecodeError, TypeError) as e: print(f"WARNING: Could not parse FieldDisplayConfigsJSON (older model): {e}")
    
    parsed_calculation_row_configs: List[CalculationRowConfig] = []
    if calculation_row_configs_json:
        try: parsed_calculation_row_configs = [CalculationRowConfig(**item) for item in json.loads(calculation_row_configs_json)]
        except: pass

    parsed_subtotal_configs: List[SubtotalConfig] = []
    if subtotal_configs_json:
        try: parsed_subtotal_configs = [SubtotalConfig(**item) for item in json.loads(subtotal_configs_json)]
        except: pass
    
    current_query_params_for_bq = []; current_conditions = []; applied_filter_values_for_template = {}; param_idx = 0
    try: looker_filters_payload = json.loads(filter_criteria_json_str or "{}")
    except json.JSONDecodeError as e: raise HTTPException(status_code=400, detail=f"Invalid JSON for filter_criteria: {str(e)}")
    parsed_user_attribute_mappings: Dict[str, str] = json.loads(user_attr_map_json or '{}')
    # ... (Full filter parsing logic from your download.py / previous correct versions) ...
    for fe_key_ua, val_str_ua in looker_filters_payload.get("user_attributes", {}).items():
        bq_col_ua = parsed_user_attribute_mappings.get(fe_key_ua)
        if bq_col_ua:
            p_name_ua = f"ua_p_{param_idx}"; param_idx += 1
            try:
                bq_type_ua, typed_val_ua = get_bq_param_type_and_value(str(val_str_ua), bq_col_ua, "AUTO")
                current_conditions.append(f"`{bq_col_ua}` = @{p_name_ua}")
                current_query_params_for_bq.append(ScalarQueryParameter(p_name_ua, bq_type_ua, typed_val_ua))
                fc_filter = field_configs_map.get(bq_col_ua); applied_filter_values_for_template[bq_col_ua] = format_value(typed_val_ua, fc_filter.number_format if fc_filter else None, schema_type_map.get(bq_col_ua, "STRING"))
            except ValueError as ve: print(f"WARN: Skipping UA filter '{bq_col_ua}': {ve}")
    for filter_key_dyn, val_str_list_dyn in looker_filters_payload.get("dynamic_filters", {}).items():
        bq_col_dyn, op_conf_dyn, op_sfx_dyn = None, None, None
        for sfx_key_iter_dyn in sorted(ALLOWED_FILTER_OPERATORS.keys(), key=len, reverse=True):
            if filter_key_dyn.endswith(sfx_key_iter_dyn): bq_col_dyn, op_conf_dyn, op_sfx_dyn = filter_key_dyn[:-len(sfx_key_iter_dyn)], ALLOWED_FILTER_OPERATORS[sfx_key_iter_dyn], sfx_key_iter_dyn; break
        if bq_col_dyn and op_conf_dyn and op_sfx_dyn:
            try:
                if op_conf_dyn["param_type_hint"] == "NONE": current_conditions.append(f"`{bq_col_dyn}` {op_conf_dyn['op']}")
                else:
                    p_name_dyn = f"df_p_{param_idx}"; param_idx += 1; _val_str_dyn = str(val_str_list_dyn)
                    bq_type_rng_dyn, val_rng_dyn = get_bq_param_type_and_value(_val_str_dyn, bq_col_dyn, op_conf_dyn["param_type_hint"])
                    fc_filter = field_configs_map.get(bq_col_dyn); schema_type_dyn = schema_type_map.get(bq_col_dyn, "STRING")
                    if op_sfx_dyn == "_between": # ... (between logic)
                        if isinstance(val_rng_dyn, tuple) and len(val_rng_dyn) == 2:
                            v1, v2 = val_rng_dyn; p1_n, p2_n = f"{p_name_dyn}_s", f"{p_name_dyn}_e"; act_t = bq_type_rng_dyn.split('_RANGE')[0] 
                            current_conditions.append(f"`{bq_col_dyn}` BETWEEN @{p1_n} AND @{p2_n}"); current_query_params_for_bq.extend([ScalarQueryParameter(p1_n, act_t, v1), ScalarQueryParameter(p2_n, act_t, v2)])
                            applied_filter_values_for_template[bq_col_dyn] = f"{format_value(v1, fc_filter.number_format if fc_filter else None, schema_type_dyn)} to {format_value(v2, fc_filter.number_format if fc_filter else None, schema_type_dyn)}"
                    elif op_conf_dyn["op"] == "IN": # ... (IN logic)
                         if isinstance(val_rng_dyn, list) and val_rng_dyn:
                            el_type = bq_type_rng_dyn; current_conditions.append(f"`{bq_col_dyn}` IN UNNEST(@{p_name_dyn})"); current_query_params_for_bq.append(ArrayQueryParameter(p_name_dyn, el_type, val_rng_dyn))
                            applied_filter_values_for_template[bq_col_dyn] = ", ".join(format_value(v, fc_filter.number_format if fc_filter else None, schema_type_dyn) for v in val_rng_dyn)
                    else: # ... (other ops logic)
                        current_conditions.append(f"`{bq_col_dyn}` {op_conf_dyn['op']} @{p_name_dyn}"); current_query_params_for_bq.append(ScalarQueryParameter(p_name_dyn, bq_type_rng_dyn, val_rng_dyn))
                        applied_filter_values_for_template[bq_col_dyn] = format_value(val_rng_dyn, fc_filter.number_format if fc_filter else None, schema_type_dyn)
            except ValueError as ve: print(f"WARN: Skipping Dyn filter '{bq_col_dyn}': {ve}")


    final_sql = base_sql_query_from_db.strip().rstrip(';')
    if current_conditions:
        conditions_sql_segment = " AND ".join(current_conditions)
        if " where " in final_sql.lower(): final_sql = f"{final_sql} AND ({conditions_sql_segment})"
        else: final_sql = f"SELECT * FROM ({final_sql}) AS GenAIReportSubquery WHERE {conditions_sql_segment}"
            
    # ORDER BY logic from your download.py (uses parsed_subtotal_configs)
    if parsed_subtotal_configs:
        group_by_field_sub = parsed_subtotal_configs[0].group_by_field_name
        order_by_clause_sub = f"`{group_by_field_sub}` ASC"
        if "ORDER BY" not in final_sql.upper(): final_sql += f" ORDER BY {order_by_clause_sub}"
        else: final_sql = final_sql.replace("ORDER BY", f"ORDER BY {order_by_clause_sub},")
        print(f"INFO: ORDER BY for subtotaling (from subtotal_configs) on '{group_by_field_sub}'.")

    job_config_kwargs = {"use_legacy_sql": False}
    if current_query_params_for_bq: job_config_kwargs["query_parameters"] = current_query_params_for_bq
    job_cfg = bigquery.QueryJobConfig(**job_config_kwargs)
    print(f"INFO: Executing BQ Query for report '{report_definition_name}':\n{final_sql}")
    try:
        query_job = bq_client.query(final_sql, job_config=job_cfg); data_rows_list = [convert_row_to_json_serializable(row) for row in query_job.result()]
    except Exception as e: error_message = str(e); error_message = "; ".join([err.get('message', 'BQ err') for err in getattr(e, 'errors', [])]) or error_message; print(f"ERROR: BQ execution: {error_message}"); raise HTTPException(status_code=500, detail=f"BQ Error: {error_message}")
    try:
        if not html_template_gcs_path or not html_template_gcs_path.startswith("gs://"): raise ValueError("Invalid TemplateURL.")
        path_parts = html_template_gcs_path.replace("gs://", "").split("/", 1); blob = gcs_client.bucket(path_parts[0]).blob(path_parts[1])
        if not blob.exists(): raise GCSNotFound(f"HTML Template not found: {html_template_gcs_path}")
        html_template_content = blob.download_as_text(encoding='utf-8')
        print(f"DEBUG: HTML Content read by /execute_report for {report_definition_name} (first 500 chars): {html_template_content[:500]}")
    except Exception as e: raise HTTPException(status_code=500, detail=f"Failed to load HTML template: {str(e)}")

    populated_html = html_template_content; table_rows_html_str = ""
    body_field_names_exec = [f_n for f_n in schema_fields_ordered if field_configs_map.get(f_n, FieldDisplayConfig(field_name=f_n)).include_in_body]
    if not body_field_names_exec and data_rows_list: body_field_names_exec = list(data_rows_list[0].keys())

    # --- HTML Table Row Generation (using logic from your download.py, relies on parsed_subtotal_configs) ---
    if data_rows_list:
        current_group_value_exec = None; group_data_for_subtotal_exec = []
        subtotal_config_to_apply = parsed_subtotal_configs[0] if parsed_subtotal_configs else None

        for i_exec, row_data_exec in enumerate(data_rows_list):
            if subtotal_config_to_apply: # This block only runs if parsed_subtotal_configs is NOT empty
                group_by_field = subtotal_config_to_apply.group_by_field_name
                new_group_value = row_data_exec.get(group_by_field)
                if current_group_value_exec is not None and new_group_value != current_group_value_exec:
                    # Generate subtotal row for previous group using subtotal_config_to_apply
                    subtotal_row_html = f"<tr class='subtotal-row' style='font-weight:bold; background-color:#f0f0f0;'>\n  <td colspan='1' class='subtotal-label'>Subtotal for {current_group_value_exec}:</td>\n" # Adjust colspan
                    for val_conf in subtotal_config_to_apply.calculated_values:
                        target_vals = [Decimal(str(g_r.get(val_conf.target_field_name,"0"))) for g_r in group_data_for_subtotal_exec if g_r.get(val_conf.target_field_name) is not None and str(g_r.get(val_conf.target_field_name,"0")).replace('.','',1).replace('-','',1).isdigit()]
                        agg_val = calculate_aggregate(target_vals, val_conf.calculation_type.value)
                        fmt_val = format_value(agg_val, val_conf.number_format, schema_type_map.get(val_conf.target_field_name, "STRING"))
                        align = val_conf.alignment or ('right' if schema_type_map.get(val_conf.target_field_name) in NUMERIC_TYPES_FOR_AGG else 'left')
                        subtotal_row_html += f"  <td style='text-align:{align};'>{fmt_val}</td>\n"
                    # Fill remaining cells if necessary to match body_field_names_exec count
                    num_data_cols_in_subtotal = 1 + len(subtotal_config_to_apply.calculated_values)
                    for _ in range(len(body_field_names_exec) - num_data_cols_in_subtotal): subtotal_row_html += "  <td></td>\n"
                    subtotal_row_html += "</tr>\n"
                    table_rows_html_str += subtotal_row_html
                    group_data_for_subtotal_exec = []
                current_group_value_exec = new_group_value
                group_data_for_subtotal_exec.append(row_data_exec)
            
            # Append current data row
            row_html_item = "<tr>\n"
            for header_key in body_field_names_exec:
                cell_value = row_data_exec.get(header_key); field_config = field_configs_map.get(header_key); field_type = schema_type_map.get(header_key, "STRING")
                formatted_val = format_value(cell_value, field_config.number_format if field_config else None, field_type)
                default_align = "left"; align_style = ""
                if field_type in NUMERIC_TYPES_FOR_AGG: default_align = "right"
                align_val = (field_config.alignment if field_config else None) or default_align
                if align_val: align_style=f"style='text-align: {align_val};'"
                row_html_item += f"  <td {align_style}>{formatted_val}</td>\n"
            row_html_item += "</tr>\n"; table_rows_html_str += row_html_item

        if subtotal_config_to_apply and group_data_for_subtotal_exec: # Last subtotal group
            subtotal_row_html = f"<tr class='subtotal-row' style='font-weight:bold; background-color:#f0f0f0;'>\n  <td colspan='1' class='subtotal-label'>Subtotal for {current_group_value_exec}:</td>\n"
            for val_conf in subtotal_config_to_apply.calculated_values:
                target_vals = [Decimal(str(g_r.get(val_conf.target_field_name,"0"))) for g_r in group_data_for_subtotal_exec if g_r.get(val_conf.target_field_name) is not None and str(g_r.get(val_conf.target_field_name,"0")).replace('.','',1).replace('-','',1).isdigit()]
                agg_val = calculate_aggregate(target_vals, val_conf.calculation_type.value)
                fmt_val = format_value(agg_val, val_conf.number_format, schema_type_map.get(val_conf.target_field_name, "STRING"))
                align = val_conf.alignment or ('right' if schema_type_map.get(val_conf.target_field_name) in NUMERIC_TYPES_FOR_AGG else 'left')
                subtotal_row_html += f"  <td style='text-align:{align};'>{fmt_val}</td>\n"
            num_data_cols_in_subtotal = 1 + len(subtotal_config_to_apply.calculated_values)
            for _ in range(len(body_field_names_exec) - num_data_cols_in_subtotal): subtotal_row_html += "  <td></td>\n"
            subtotal_row_html += "</tr>\n"; table_rows_html_str += subtotal_row_html
    
    elif not data_rows_list:
        colspan = len(body_field_names_exec) if body_field_names_exec else 1
        table_rows_html_str = f"<tr><td colspan='{colspan}' style='text-align:center; padding: 20px;'>No data found for the selected criteria.</td></tr>"
        print(f"DEBUG: Setting 'No data found' message. Colspan: {colspan}")
    
    print(f"DEBUG: Final table_rows_html_str (first 500 chars): {table_rows_html_str[:500]}")
    populated_html = populated_html.replace("{{TABLE_ROWS_HTML_PLACEHOLDER}}", table_rows_html_str)

    if parsed_calculation_row_configs: # Logic from your download.py
        for calc_row_conf in parsed_calculation_row_configs:
            calculated_row_html_cells = ""
            for val_conf in calc_row_conf.calculated_values:
                target_col_data = [r_data.get(val_conf.target_field_name) for r_data in data_rows_list if r_data.get(val_conf.target_field_name) is not None]
                numeric_col_data = [Decimal(str(x)) for x in target_col_data if str(x).replace('.','',1).replace('-','',1).isdigit()]
                calculated_result = calculate_aggregate(numeric_col_data, val_conf.calculation_type.value)
                field_type = schema_type_map.get(val_conf.target_field_name, "STRING")
                num_fmt = val_conf.number_format if val_conf.calculation_type not in [CalculationType.COUNT, CalculationType.COUNT_DISTINCT] else 'INTEGER'
                formatted_val = format_value(calculated_result, num_fmt, field_type)
                align_style = (val_conf.alignment if val_conf.alignment else ("right" if field_type in NUMERIC_TYPES_FOR_AGG else "left"))
                calculated_row_html_cells += f"<td style='text-align: {align_style};'>{formatted_val}</td>\n"
            populated_html = populated_html.replace(f"{{{{{calc_row_conf.values_placeholder_name}}}}}", calculated_row_html_cells)

    for fc_name_ph_final, fc_config_obj_ph_final in field_configs_map.items(): # Using OLDER FieldDisplayConfig
        val_raw_ph_final, val_fmt_ph_final = None, "" 
        if fc_config_obj_ph_final.field_name in applied_filter_values_for_template: # Check applied_filter_values_for_template
            val_raw_ph_final = applied_filter_values_for_template[fc_config_obj_ph_final.field_name]
            val_fmt_ph_final = str(val_raw_ph_final) 
        elif data_rows_list and fc_config_obj_ph_final.field_name in data_rows_list[0]:
            val_raw_ph_final = data_rows_list[0][fc_config_obj_ph_final.field_name]
            val_fmt_ph_final = format_value(val_raw_ph_final, fc_config_obj_ph_final.number_format, schema_type_map.get(fc_config_obj_ph_final.field_name, "STRING"))
        else: val_fmt_ph_final = "" 
        if fc_config_obj_ph_final.include_at_top: populated_html = populated_html.replace(f"{{{{TOP_{fc_config_obj_ph_final.field_name}}}}}", val_fmt_ph_final)
        if fc_config_obj_ph_final.include_in_header: populated_html = populated_html.replace(f"{{{{HEADER_{fc_config_obj_ph_final.field_name}}}}}", val_fmt_ph_final)

    if "{{REPORT_TITLE_PLACEHOLDER}}" in populated_html: populated_html = populated_html.replace("{{REPORT_TITLE_PLACEHOLDER}}", f"Report: {report_definition_name.replace('_', ' ').title()}")
    if "{{CURRENT_DATE_PLACEHOLDER}}" in populated_html: populated_html = populated_html.replace("{{CURRENT_DATE_PLACEHOLDER}}", datetime.date.today().isoformat())

    report_id = str(uuid.uuid4()); generated_reports_store[report_id] = populated_html
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
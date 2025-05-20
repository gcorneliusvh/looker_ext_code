from fastapi import FastAPI, HTTPException, Form, Depends
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager
import uvicorn
import os
from typing import Union # For type hints (Python 3.11 in Docker supports | but Union is fine)
import httpx
import inspect # For debugging

# Attempt to mimic the user's working Cloud Function imports
try:
    from google import genai as top_level_genai # This is what we'll use for Client and types
    from google.genai import types as top_level_genai_types
    print("INFO: Successfully imported 'from google import genai' and 'from google.genai import types'")
except ImportError as e:
    print(f"ERROR: Could not import 'from google import genai' or 'from google.genai import types'. Error: {e}")
    print("Ensure 'google-genai' and potentially 'google-generativeai' are in requirements.txt and installed.")
    # To allow app to start and show error via API, define placeholders if import fails
    top_level_genai = None
    top_level_genai_types = None


# For version checking, explicitly import google.generativeai if possible
try:
    import google.generativeai as official_gengaipkg
    OFFICIAL_GENGAI_VERSION = official_gengaipkg.__version__
except (ImportError, AttributeError):
    OFFICIAL_GENGAI_VERSION = "google.generativeai module/version not found."


# --- Application Configuration Store ---
class AppConfig:
    gcp_project_id: str = ""
    gcp_location: str = ""
    default_prompt_text: str = ""
    default_system_instruction_text: str = ""
    # Store the genai.Client instance from the 'from google import genai' import
    genai_client_instance: Union[object, None] = None # Use object if top_level_genai.Client type is unknown here
    
    genai_sdk_version_info: str = f"Underlying official google-generativeai (if found): {OFFICIAL_GENGAI_VERSION}"
    # Using the model name from your working Cloud Function
    TARGET_GEMINI_MODEL: str = os.getenv("GEMINI_MODEL_OVERRIDE", "gemini-2.5-pro-preview-05-06")


config = AppConfig()

# --- Helper to Load Text Files --- (remains the same)
def load_text_from_file(file_path: str, description: str) -> str:
    try:
        with open(file_path, "r", encoding="utf-8") as f: return f.read()
    except FileNotFoundError: print(f"WARNING: {description} file '{file_path}' not found."); return ""
    except Exception as e: print(f"ERROR reading {description} file '{file_path}': {e}"); return ""

# --- FastAPI Lifespan Event Handler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("INFO: FastAPI application startup...")
    # Print SDK version info collected at import time
    print(f"INFO: SDK Version Info: {config.genai_sdk_version_info}")
    if top_level_genai is None or top_level_genai_types is None:
        print("ERROR: 'google.genai' or 'google.genai.types' could not be imported. GenAI features will fail.")

    config.gcp_project_id = os.getenv("GCP_PROJECT_ID")
    config.gcp_location = os.getenv("GCP_LOCATION")

    if not config.gcp_project_id: print("ERROR: GCP_PROJECT_ID environment variable is not set.")
    if not config.gcp_location: print("ERROR: GCP_LOCATION environment variable is not set.")

    config.default_prompt_text = load_text_from_file("prompt.txt", "Default prompt")
    config.default_system_instruction_text = load_text_from_file("system_instruction.txt", "System instruction")

    print(f"INFO: Target GCP Project ID: {config.gcp_project_id}, Target Location: {config.gcp_location}")
    print(f"INFO: Will attempt to use Model: {config.TARGET_GEMINI_MODEL} via genai.Client pattern.")

    try:
        if top_level_genai and config.gcp_project_id and config.gcp_location:
            print(f"INFO: Initializing top_level_genai.Client for project='{config.gcp_project_id}', location='{config.gcp_location}', vertexai=True")
            # Using the 'genai.Client' from 'from google import genai'
            config.genai_client_instance = top_level_genai.Client(
                vertexai=True,
                project=config.gcp_project_id,
                location=config.gcp_location
            )
            # Dynamically assign type hint if client is created
            if config.genai_client_instance:
                 AppConfig.genai_client_instance = config.genai_client_instance # For type checker if it can infer
            print("INFO: top_level_genai.Client instance created successfully.")
        else:
            if not top_level_genai:
                print("ERROR: 'google.genai' (top_level_genai) is not available for client initialization.")
            else:
                print("ERROR: top_level_genai.Client NOT initialized due to missing GCP_PROJECT_ID or GCP_LOCATION.")
            config.genai_client_instance = None
    except Exception as e:
        print(f"FATAL ERROR: Could not initialize top_level_genai.Client: {e}")
        if "vertexai" in str(e).lower() and "unexpected keyword" in str(e).lower():
            print("Hint: The 'genai.Client' being imported might be an older version. Check 'google-genai' and 'google-generativeai' versions.")
        config.genai_client_instance = None
    
    yield
    print("INFO: FastAPI application shutdown.")

app = FastAPI(lifespan=lifespan)

# --- Dependency to get GenAI Client instance ---
def get_genai_client_instance():
    if not config.genai_client_instance:
        raise HTTPException(status_code=503, detail="GenAI client instance not available. Server configuration error.")
    return config.genai_client_instance

# --- Helper function from your working code ---
def remove_first_and_last_lines(s):
    """
    Removes the first and last lines from a multi-line string.

    Args:
        s: The input multi-line string.

    Returns:
        A new string with the first and last lines removed.
        Returns an empty string if the input has fewer than 3 lines.
    """
    lines = s.splitlines()  # Split the string into a list of lines
    if len(lines) > 2:
        # Slice the list to get lines from the second one up to (but not including) the last one
        middle_lines = lines[1:-1]
        return '\n'.join(middle_lines)  # Join the middle lines back with newline characters
    else:
        # If there are 2 or fewer lines, removing the first and last would result in an empty or negative selection.
        # You can decide how to handle this: return empty string, raise error, or return original.
        # Here, we return an empty string.
        return ""

# --- Core HTML Generation Logic (Adapted to use your Cloud Function's genai.Client pattern) ---
def generate_html_from_user_pattern(
    client: top_level_genai.Client, # Expects the client from 'from google import genai'
    prompt_text: str,
    image_bytes: bytes,
    image_mime_type: str,
    system_instruction_text: str # This is si_text1 from your CF
) -> Union[str, None]:
    
    print(f"DEBUG: Inside generate_html_from_user_pattern.")
    if top_level_genai_types is None:
        print("FATAL DEBUG: top_level_genai_types is None! Cannot proceed to create Parts.")
        raise HTTPException(status_code=500, detail="SDK types module (google.genai.types) not imported correctly.")

    print(f"DEBUG: Type of 'top_level_genai_types' module object: {type(top_level_genai_types)}")
    print(f"DEBUG: File of 'top_level_genai_types' module: {getattr(top_level_genai_types, '__file__', 'N/A')}")
    print(f"DEBUG: Attributes in 'top_level_genai_types' module (dir()): {dir(top_level_genai_types)}")
    
    for attr_name in ['Part', 'Blob', 'Content', 'GenerateContentConfig', 'SafetySetting', 'HarmCategory', 'HarmBlockThreshold']:
        if hasattr(top_level_genai_types, attr_name):
            print(f"DEBUG: 'top_level_genai_types.{attr_name}' IS found.")
        else:
            print(f"DEBUG: 'top_level_genai_types.{attr_name}' IS NOT found! This will cause an error.")
            # If Part is missing, we can't continue this way
            if attr_name == 'Part':
                 raise HTTPException(status_code=500, detail=f"Critical SDK Error: '{attr_name}' not found in imported 'google.genai.types'.")

    try:
        image_part = top_level_genai_types.Part(inline_data=top_level_genai_types.Blob(mime_type=image_mime_type, data=image_bytes))
        prompt_part = top_level_genai_types.Part.from_text(text=prompt_text)
        system_instruction_part_for_config = top_level_genai_types.Part.from_text(text=system_instruction_text)
    except AttributeError as ae:
        print(f"FATAL ERROR (AttributeError) creating Part/Blob objects: {ae}") 
        raise HTTPException(status_code=500, detail=f"SDK Error: Attribute missing from 'google.genai.types'. Error: {str(ae)}")
    except Exception as e:
        print(f"FATAL ERROR (General Exception) creating Part/Blob objects: {e}")
        raise HTTPException(status_code=500, detail=f"Error creating GenAI input parts: {str(e)}")
        
    model_to_use = "gemini-2.5-pro-preview-05-06"

    contents_for_gemini = [
        top_level_genai_types.Content(role="user", parts=[prompt_part, image_part]),
    ]

    generation_config_settings = top_level_genai_types.GenerateContentConfig(
        temperature=1.0, top_p=0.95, max_output_tokens=54986, # Using a more standard token limit
        response_mime_type="text/plain", # More common for GenerateContentConfig
        safety_settings=[
            top_level_genai_types.SafetySetting(category=top_level_genai_types.HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=top_level_genai_types.HarmBlockThreshold.BLOCK_NONE),
            top_level_genai_types.SafetySetting(category=top_level_genai_types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=top_level_genai_types.HarmBlockThreshold.BLOCK_NONE),
            top_level_genai_types.SafetySetting(category=top_level_genai_types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=top_level_genai_types.HarmBlockThreshold.BLOCK_NONE),
            top_level_genai_types.SafetySetting(category=top_level_genai_types.HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=top_level_genai_types.HarmBlockThreshold.BLOCK_NONE)
        ],
        system_instruction=[system_instruction_part_for_config],
    )

    print(f"INFO: Generating content with model: {model_to_use} using client.models.generate_content_stream pattern.")
    generated_text_output = ""
    try:
        stream = client.models.generate_content_stream(
            model=model_to_use, contents=contents_for_gemini, config=generation_config_settings,
        )
        for chunk in stream:
            if hasattr(chunk, 'text') and chunk.text is not None: 
                generated_text_output += chunk.text
                print(chunk.text)
    except AttributeError as e:
        print(f"DEBUG: client.models.generate_content_stream failed: '{e}'. Attempting fallback.")
        generation_config_fallback = top_level_genai_types.GenerateContentConfig(
            temperature=1.0, top_p=0.95, max_output_tokens=8192, response_mime_type="text/plain",
            safety_settings=generation_config_settings.safety_settings
        )
        try:
            model_instance_fallback = client.get_generative_model(
                model_name=model_to_use, system_instruction=system_instruction_text
            )
            response_stream_fallback = model_instance_fallback.generate_content(
                contents=contents_for_gemini, generation_config=generation_config_fallback, stream=True
            )
            for chunk_fallback in response_stream_fallback:
                if hasattr(chunk_fallback, 'text') and chunk_fallback.text is not None: generated_text_output += chunk_fallback.text
        except Exception as fallback_e:
            print(f"ERROR: Fallback generation method also failed: {fallback_e}")
            raise HTTPException(status_code=500, detail=f"GenAI content generation failed on primary and fallback methods: {fallback_e}")
    except Exception as e:
        print(f"ERROR during GenAI content generation (main attempt): {e}")
        raise HTTPException(status_code=500, detail=f"GenAI content generation error: {str(e)}")

    print("INFO: Finished generating content from GenAI model.")
    return remove_first_and_last_lines(generated_text_output)


# --- API Endpoint ---
@app.post("/generate_report", response_class=HTMLResponse)
async def api_generate_report(
    image_url: str = Form(...),
    prompt: str = Form(None),
    system_instruction: str = Form(None),
    client_instance: top_level_genai.Client = Depends(get_genai_client_instance) # Inject client
):
    prompt_to_use = prompt if prompt is not None else config.default_prompt_text
    system_instruction_to_use = system_instruction if system_instruction is not None else config.default_system_instruction_text

    if not prompt_to_use: raise HTTPException(status_code=400, detail="Prompt is missing.")
    if not system_instruction_to_use: raise HTTPException(status_code=400, detail="System instruction is missing.")
    if not image_url: raise HTTPException(status_code=400, detail="Image URL is missing.")

    image_bytes: Union[bytes, None] = None; image_mime_type: str = "application/octet-stream" 
    print(f"INFO: Attempting to download image from URL: {image_url}")
    try:
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            response = await http_client.get(image_url)
            response.raise_for_status()
            image_bytes = await response.aread()
            ct_header = response.headers.get("Content-Type")
            if ct_header and ct_header.startswith("image/"): image_mime_type = ct_header
            elif ct_header: print(f"WARNING: Content-Type from URL '{image_url}' is '{ct_header}'."); image_mime_type = ct_header
            else: print(f"WARNING: Content-Type header missing for '{image_url}'. Defaulting to '{image_mime_type}'.")
            print(f"INFO: Downloaded image. Size: {len(image_bytes) if image_bytes else 0} bytes. MIME: {image_mime_type}")
    except Exception as e:
        print(f"ERROR downloading image '{image_url}': {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error fetching image: {str(e)}")
    if not image_bytes: raise HTTPException(status_code=400, detail="Image data empty after download.")

    try:
        print(f"INFO: API request processing. Using 'genai.Client' pattern.")
        html_content = generate_html_from_user_pattern(
            client=client_instance, prompt_text=prompt_to_use, image_bytes=image_bytes,
            image_mime_type=image_mime_type, system_instruction_text=system_instruction_to_use
        )
        if html_content is None: html_content = "" # Ensure string for HTMLResponse
        return HTMLResponse(content=html_content)
    except HTTPException: raise 
    except Exception as e:
        print(f"ERROR in /generate_report (GenAI part): {type(e).__name__} - {e}")
        raise HTTPException(status_code=500, detail=f"Server error during GenAI processing: {str(e)}")

# --- Root Endpoint for Health Check ---
@app.get("/")
async def read_root():
    status_msg = f"GenAI Report API. Target Model: {config.TARGET_GEMINI_MODEL}. SDK Info: {config.genai_sdk_version_info}."
    if config.genai_client_instance and top_level_genai_types: # Check if client and types were successfully initialized/imported
        return {"status": f"{status_msg} GenAI Client Initialized. Types module appears loaded."}
    else:
        return {"status": f"{status_msg} GenAI Client OR Types Module NOT INITIALIZED/IMPORTED. Check logs."}

# if __name__ == "__main__":
#     uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
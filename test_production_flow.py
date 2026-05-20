"""
Test that EXACTLY mirrors production flow to isolate the slowness.
"""
import time
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import os
from dotenv import load_dotenv

load_dotenv()

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
AZURE_LLM_DEPLOY = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-5.2-chat")

llm = AzureChatOpenAI(
    azure_endpoint=AZURE_ENDPOINT,
    api_key=AZURE_API_KEY,
    api_version=AZURE_API_VERSION,
    azure_deployment=AZURE_LLM_DEPLOY,
    request_timeout=300,
)

# ============================================================================
# TEST 1: Simple HumanMessage (like your quick test)
# ============================================================================
print("\n" + "="*70)
print("TEST 1: Simple HumanMessage (10KB text, 2 images)")
print("="*70)

image_b64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
long_text = "Extract from: " + "X" * 10000

msg = HumanMessage(content=[
    {"type": "text", "text": long_text},
    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}", "detail": "low"}},
    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}", "detail": "low"}},
])

start = time.time()
try:
    result = llm.invoke([msg])  # Must pass list of messages
    elapsed = time.time() - start
    print(f"✓ Elapsed: {elapsed:.1f}s")
except Exception as e:
    elapsed = time.time() - start
    print(f"✗ Failed after {elapsed:.1f}s: {e}")

# ============================================================================
# TEST 2: SystemMessage + HumanMessage (PRODUCTION FLOW)
# ============================================================================
print("\n" + "="*70)
print("TEST 2: SystemMessage + HumanMessage with force_json (PRODUCTION)")
print("="*70)

# Azure requires the word "json" in the prompt when using response_format=json_object
sys_prompt = """You are a procurement analyst. Extract commercials data from quotations. Return JSON only."""
user_prompt = long_text

messages = [
    SystemMessage(content=sys_prompt),
    HumanMessage(content=[
        {"type": "text", "text": user_prompt},
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}", "detail": "low"}},
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}", "detail": "low"}},
    ])
]

# Bind JSON mode like production does
try:
    active_llm = llm.bind(response_format={"type": "json_object"})
    print("✓ JSON mode bound successfully")
except Exception as e:
    print(f"✗ JSON mode bind failed (will retry without it): {e}")
    active_llm = llm

start = time.time()
try:
    result = active_llm.invoke(messages)
    elapsed = time.time() - start
    print(f"✓ Elapsed: {elapsed:.1f}s")
except Exception as e:
    elapsed = time.time() - start
    print(f"✗ Failed after {elapsed:.1f}s: {e}")

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print("If TEST 1 ~5s and TEST 2 ~80s: SystemMessage/JSON mode causes slowness")
print("If both ~5s: Something else in production code is different")
print("If both ~80s: Azure is degraded (not code issue)")

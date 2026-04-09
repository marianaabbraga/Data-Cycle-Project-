"""
Web chat interface for the Pipeline Assistant.

Run:
    python -m agent.web            # starts on port 8501
    uvicorn agent.web:app --port 8501
"""

import logging
import os
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from agent.chat_agent import agent
from agent.tools import trigger_pipeline as _trigger_pipeline_tool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("agent.web")

app = FastAPI(title="Pipeline Chat Agent")


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    reply: str


_TRIGGER_KEYWORDS = {"run the pipeline", "trigger the pipeline", "run pipeline",
                      "trigger pipeline", "start the pipeline", "start pipeline",
                      "run the workflow", "run workflow", "kick off",
                      "execute the pipeline", "execute pipeline", "launch pipeline"}


def _looks_like_trigger(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in _TRIGGER_KEYWORDS)


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    log.info("Chat request: %s", req.message[:200])
    t0 = time.monotonic()
    try:
        reply = agent.say(req.message)
        reply_str = str(reply)
        elapsed = time.monotonic() - t0
        log.info("Chat reply (%.1fs): %s", elapsed, reply_str[:300])

        if _looks_like_trigger(req.message) and "Run ID" not in reply_str:
            log.warning("LLM did not call trigger_pipeline — invoking fallback")
            result = _trigger_pipeline_tool(stage="full")
            log.info("Fallback trigger result: %s", result)
            reply_str = f"{reply_str}\n\n[Auto-trigger] {result}"

        return ChatResponse(reply=reply_str)
    except Exception as exc:
        elapsed = time.monotonic() - t0
        log.error("Chat error after %.1fs: %s", elapsed, exc, exc_info=True)

        if _looks_like_trigger(req.message):
            log.info("Agent crashed on trigger request — calling tool directly")
            try:
                result = _trigger_pipeline_tool(stage="full")
                return ChatResponse(reply=f"Agent had trouble, but pipeline was triggered.\n{result}")
            except Exception as trigger_exc:
                log.error("Fallback trigger also failed: %s", trigger_exc)

        base_url = os.getenv("OPENAI_BASE_URL", "")
        hint = ""
        if base_url:
            hint = (
                "\n\n(Ollama hint: if you're running Ollama on the host, ensure it listens on "
                "0.0.0.0:11434 (not just 127.0.0.1) so Docker containers can reach it.)"
            )
        msg = f"Agent error: {exc}{hint}"
        return JSONResponse(status_code=500, content={"reply": msg})


@app.post("/trigger")
async def trigger():
    """Manually trigger the pipeline via the Prefect API."""
    log.info("Manual pipeline trigger requested from UI")
    try:
        result = _trigger_pipeline_tool(stage="full")
        log.info("Trigger result: %s", result)
        return {"status": "ok", "message": result}
    except Exception as exc:
        log.error("Trigger failed: %s", exc, exc_info=True)
        return JSONResponse(status_code=500, content={"status": "error", "message": str(exc)})


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE


HTML_PAGE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pipeline Assistant</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:system-ui,sans-serif;background:#1e1e1e;color:#e0e0e0;
       display:flex;flex-direction:column;height:100vh}
  header{padding:14px 20px;background:#2b2b2b;border-bottom:1px solid #444;
         display:flex;align-items:center;justify-content:space-between}
  header h1{font-size:1.1rem;font-weight:600}
  #trigger-btn{padding:8px 16px;border:none;border-radius:6px;
               background:#66bb6a;color:#fff;font-size:.85rem;cursor:pointer;
               transition:background .15s}
  #trigger-btn:hover{background:#43a047}
  #trigger-btn:disabled{opacity:.5;cursor:default}
  #messages{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:12px}
  .msg{max-width:75%;padding:10px 14px;border-radius:10px;line-height:1.5;
       white-space:pre-wrap;font-size:.95rem}
  .user{align-self:flex-end;background:#42a5f5;color:#fff}
  .bot{align-self:flex-start;background:#363636}
  .system{align-self:center;background:#2e7d32;color:#e8f5e9;font-size:.85rem;
          border-radius:6px;padding:6px 14px}
  #form{display:flex;gap:8px;padding:14px 20px;background:#2b2b2b;border-top:1px solid #444}
  #input{flex:1;padding:10px 14px;border:1px solid #555;border-radius:8px;
         background:#1e1e1e;color:#e0e0e0;font-size:.95rem;outline:none}
  #input:focus{border-color:#42a5f5}
  #send-btn{padding:10px 20px;border:none;border-radius:8px;background:#42a5f5;
         color:#fff;font-size:.95rem;cursor:pointer}
  #send-btn:hover{background:#1e88e5}
  #send-btn:disabled{opacity:.5;cursor:default}
</style>
</head>
<body>
<header>
  <h1>Pipeline Assistant</h1>
  <button id="trigger-btn" onclick="triggerPipeline()">Run Pipeline</button>
</header>
<div id="messages"></div>
<form id="form" onsubmit="send(event)">
  <input id="input" placeholder="Ask about the pipeline..." autocomplete="off">
  <button id="send-btn" type="submit">Send</button>
</form>
<script>
const messages=document.getElementById("messages");
const input=document.getElementById("input");
const sendBtn=document.getElementById("send-btn");
const trigBtn=document.getElementById("trigger-btn");

function add(text,cls){
  const d=document.createElement("div");
  d.className="msg "+cls;
  d.textContent=text;
  messages.appendChild(d);
  messages.scrollTop=messages.scrollHeight;
}

async function send(e){
  e.preventDefault();
  const text=input.value.trim();
  if(!text)return;
  add(text,"user");
  input.value="";
  sendBtn.disabled=true;
  try{
    const r=await fetch("/chat",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({message:text})
    });
    const raw=await r.text();
    let data=null;
    try{ data=JSON.parse(raw); }catch(_e){}
    if(!r.ok){
      add((data && data.reply) ? data.reply : ("Server error: "+raw),"bot");
    }else{
      add((data && data.reply) ? data.reply : raw,"bot");
    }
  }catch(err){
    add("Error: "+err.message,"bot");
  }
  sendBtn.disabled=false;
  input.focus();
}

async function triggerPipeline(){
  trigBtn.disabled=true;
  trigBtn.textContent="Triggering...";
  add("Triggering pipeline run...","system");
  try{
    const r=await fetch("/trigger",{method:"POST"});
    const raw=await r.text();
    let data=null;
    try{ data=JSON.parse(raw); }catch(_e){}
    const msg=(data && data.message) ? data.message : raw;
    add(msg, r.ok ? "system" : "bot");
  }catch(err){
    add("Trigger failed: "+err.message,"bot");
  }
  trigBtn.disabled=false;
  trigBtn.textContent="Run Pipeline";
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("CHAT_PORT", "8501"))
    uvicorn.run(app, host="0.0.0.0", port=port)

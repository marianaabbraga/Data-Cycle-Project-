"""
Web chat interface for the Pipeline Assistant.

Run:
    python -m agent.web            # starts on port 8501
    uvicorn agent.web:app --port 8501
"""

import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from agent.chat_agent import agent

app = FastAPI(title="Pipeline Chat Agent")


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    reply: str


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    reply = agent.say(req.message)
    return ChatResponse(reply=str(reply))


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
         font-size:1.1rem;font-weight:600}
  #messages{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:12px}
  .msg{max-width:75%;padding:10px 14px;border-radius:10px;line-height:1.5;
       white-space:pre-wrap;font-size:.95rem}
  .user{align-self:flex-end;background:#42a5f5;color:#fff}
  .bot{align-self:flex-start;background:#363636}
  #form{display:flex;gap:8px;padding:14px 20px;background:#2b2b2b;border-top:1px solid #444}
  #input{flex:1;padding:10px 14px;border:1px solid #555;border-radius:8px;
         background:#1e1e1e;color:#e0e0e0;font-size:.95rem;outline:none}
  #input:focus{border-color:#42a5f5}
  button{padding:10px 20px;border:none;border-radius:8px;background:#42a5f5;
         color:#fff;font-size:.95rem;cursor:pointer}
  button:hover{background:#1e88e5}
  button:disabled{opacity:.5;cursor:default}
</style>
</head>
<body>
<header>Pipeline Assistant</header>
<div id="messages"></div>
<form id="form" onsubmit="send(event)">
  <input id="input" placeholder="Ask about the pipeline..." autocomplete="off">
  <button id="btn" type="submit">Send</button>
</form>
<script>
const messages=document.getElementById("messages");
const input=document.getElementById("input");
const btn=document.getElementById("btn");

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
  btn.disabled=true;
  try{
    const r=await fetch("/chat",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({message:text})
    });
    const data=await r.json();
    add(data.reply,"bot");
  }catch(err){
    add("Error: "+err.message,"bot");
  }
  btn.disabled=false;
  input.focus();
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("CHAT_PORT", "8501"))
    uvicorn.run(app, host="0.0.0.0", port=port)

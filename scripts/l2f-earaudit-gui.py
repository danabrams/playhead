#!/usr/bin/env python3
"""
l2f-earaudit-gui.py — browser GUI for the rediff ear-audit (playhead-xsdz.31).

A tiny LOCAL web app (Python stdlib only, no deps, no external network) that
plays each rediff-only R3 span in your browser and lets you click Ad / Content /
Boundary. Records verdicts to playhead-rediff-earaudit-results.jsonl (shared +
resumable with the CLI runner) and shows live rediff PRECISION with a Wilson
95% CI — the independent ruler the activation flip gates on.

    python3 scripts/l2f-earaudit-gui.py            # opens http://127.0.0.1:8765
    python3 scripts/l2f-earaudit-gui.py --port 9000
    python3 scripts/l2f-earaudit-gui.py --no-open  # don't auto-open a browser

Keyboard in the page: Space=play/replay · L=lead-in · A=ad · C=content ·
B=boundary-off · S=skip. Close the tab + Ctrl-C the server when done.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import pathlib
import re
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = pathlib.Path(__file__).resolve().parents[1]
ANN_DIR = ROOT / "TestFixtures/Corpus/Annotations"
AUDIO_DIR = ROOT / "TestFixtures/Corpus/Audio"
MANIFEST = ROOT / "TestFixtures/Corpus/Snapshots/manifest.json"
RESULTS = ROOT / "playhead-rediff-earaudit-results.jsonl"

_LOCK = threading.Lock()


def load_spans() -> list[dict]:
    show_by_eid: dict[str, str] = {}
    if MANIFEST.exists():
        try:
            for m in json.loads(MANIFEST.read_text()):
                show_by_eid[m.get("episodeId", "")] = m.get("show", "?")
        except Exception:
            pass
    spans: list[dict] = []
    for ann_path in sorted(ANN_DIR.glob("*.json")):
        try:
            ann = json.loads(ann_path.read_text())
        except Exception:
            continue
        eid = ann.get("episodeId") or ann_path.stem
        ann_show = ann.get("show")
        for w in (ann.get("adWindows") or ann.get("ad_windows") or []):
            if w.get("audit_priority") != 1:
                continue
            start = float(w.get("startSeconds") or w.get("start_seconds") or 0.0)
            end = float(w.get("endSeconds") or w.get("end_seconds") or 0.0)
            if end <= start:
                continue
            audio = AUDIO_DIR / pathlib.Path(w.get("audioPath") or f"{eid}.mp3").name
            if not audio.exists():
                for p in AUDIO_DIR.glob("*.mp3"):
                    if p.stem.startswith(eid[:40]):
                        audio = p
                        break
            spans.append({
                "episodeId": eid, "show": ann_show or show_by_eid.get(eid, "?"),
                "start": round(start, 2), "end": round(end, 2),
                "audio": str(audio) if audio.exists() else None,
                "staged": audio.exists(),
            })
    spans.sort(key=lambda s: (s["show"], s["episodeId"], s["start"]))
    return spans


def load_verdicts() -> dict[str, dict]:
    out: dict[str, dict] = {}
    if RESULTS.exists():
        for line in RESULTS.read_text().splitlines():
            try:
                r = json.loads(line)
                out[f"{r['episodeId']}@{round(float(r['start']),1)}"] = r
            except Exception:
                pass
    return out


def wilson(k: int, n: int, z: float = 1.96):
    if n == 0:
        return (None, None, None)
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (p, max(0.0, c - h), min(1.0, c + h))


def summary(verdicts: dict[str, dict]) -> dict:
    ad = sum(1 for r in verdicts.values() if r.get("verdict") == "ad")
    content = sum(1 for r in verdicts.values() if r.get("verdict") == "content")
    boundary = sum(1 for r in verdicts.values() if r.get("verdict") == "boundary")
    correct, judged = ad + boundary, ad + boundary + content
    p, lo, hi = wilson(correct, judged)
    return {"ad": ad, "content": content, "boundary": boundary, "correct": correct,
            "judged": judged, "precision": p, "lo": lo, "hi": hi,
            "total": len(SPANS), "done": ad + content + boundary}


SPANS: list[dict] = []

PAGE = """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Rediff ear-audit</title><style>
:root{--bg:#0f1115;--card:#1a1d24;--fg:#e6e8ee;--mut:#8a90a0;--ln:#2a2e38;
 --ad:#2fbf71;--content:#e5484d;--bnd:#f5a623;--acc:#5b8def}
@media(prefers-color-scheme:light){:root{--bg:#f6f7f9;--card:#fff;--fg:#1a1d24;--mut:#667;--ln:#e5e8ee}}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 -apple-system,system-ui,sans-serif;background:var(--bg);color:var(--fg)}
.wrap{max-width:640px;margin:0 auto;padding:24px 18px 80px}
h1{font-size:15px;font-weight:600;letter-spacing:.02em;color:var(--mut);margin:0 0 4px}
.prog{height:6px;background:var(--ln);border-radius:6px;overflow:hidden;margin:10px 0 20px}
.prog>i{display:block;height:100%;background:var(--acc);width:0;transition:width .2s}
.card{background:var(--card);border:1px solid var(--ln);border-radius:16px;padding:22px}
.show{font-weight:700;font-size:20px}.eid{color:var(--mut);font-size:12px;word-break:break-all;margin:2px 0 14px}
.span{font-variant-numeric:tabular-nums;font-size:15px;color:var(--mut)}
.playbar{height:8px;background:var(--ln);border-radius:8px;overflow:hidden;margin:16px 0}
.playbar>i{display:block;height:100%;background:var(--acc);width:0}
.row{display:flex;gap:10px;margin-top:14px;flex-wrap:wrap}
button{font:inherit;font-weight:600;border:1px solid var(--ln);background:var(--card);color:var(--fg);
 padding:12px 16px;border-radius:12px;cursor:pointer;flex:1;min-width:120px}
button:hover{border-color:var(--acc)}button:active{transform:translateY(1px)}
.play{background:var(--acc);color:#fff;border-color:transparent}
.ad{border-color:var(--ad);color:var(--ad)}.content{border-color:var(--content);color:var(--content)}
.bnd{border-color:var(--bnd);color:var(--bnd)}
.sub{display:flex;gap:10px;margin-top:8px}.sub button{flex:1;padding:9px;font-weight:500;font-size:13px;color:var(--mut)}
.note{width:100%;margin-top:10px;padding:9px 11px;border-radius:10px;border:1px solid var(--ln);background:var(--bg);color:var(--fg);font:inherit;display:none}
.stat{margin-top:22px;padding:16px 18px;background:var(--card);border:1px solid var(--ln);border-radius:14px}
.big{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums}
.ci{color:var(--mut);font-size:13px}.k{color:var(--mut);font-size:12px;margin-top:14px}
.done{text-align:center;padding:60px 0;color:var(--mut)}kbd{background:var(--ln);border-radius:4px;padding:1px 5px;font:12px ui-monospace,monospace}
</style></head><body><div class=wrap>
<h1>REDIFF EAR-AUDIT · playhead-xsdz.31</h1><div class=prog><i id=pg></i></div>
<div id=main></div>
<div class=stat><div class=big id=prec>—</div><div class=ci id=ci>listen &amp; judge to build the precision estimate</div></div>
<div class=k>Keys: <kbd>Space</kbd> play/replay · <kbd>L</kbd> lead-in · <kbd>A</kbd> ad · <kbd>C</kbd> content · <kbd>B</kbd> boundary-off · <kbd>S</kbd> skip</div>
</div>
<audio id=au></audio>
<script>
let spans=[],i=0,au=document.getElementById('au'),stopAt=0;
async function boot(){let r=await fetch('/api/spans');let d=await r.json();spans=d.spans;i=d.firstUndone;render();refreshStat();}
function cur(){return spans[i]}
function render(){
 let pg=document.getElementById('pg');pg.style.width=(spans.length?100*spans.filter(s=>s.verdict).length/spans.length:0)+'%';
 let m=document.getElementById('main');let s=cur();
 if(!s){m.innerHTML='<div class=done>All spans judged. Precision is below. You can close this tab.</div>';return;}
 let dur=(s.end-s.start);
 m.innerHTML=`<div class=card><div class=show>${esc(s.show)}</div><div class=eid>${esc(s.episodeId)}</div>
 <div class=span>${fmt(s.start)}–${fmt(s.end)} · ${Math.round(dur)}s ${s.staged?'':' · <b style=color:var(--content)>audio not staged</b>'}</div>
 <div class=playbar><i id=pb></i></div>
 <div class=row><button class=play onclick=playSpan()>▶ Play span</button><button onclick=leadin() style=flex:0.5>Lead-in</button></div>
 <div class=row><button class=ad onclick="verdict('ad')">✓ Ad</button>
  <button class=content onclick="verdict('content')">✗ Content</button>
  <button class=bnd onclick="verdict('boundary')">~ Boundary off</button></div>
 <input class=note id=note placeholder="note (optional)">
 <div class=sub><button onclick="verdict('skip')">Skip</button><button onclick=undo()>Undo last</button></div></div>`;
 if(s.staged)playSpan();
}
function playSpan(){let s=cur();if(!s||!s.staged)return;au.src='/audio/'+i;stopAt=s.end;au.currentTime=0;
 au.onloadedmetadata=()=>{au.currentTime=s.start;au.play()};if(au.readyState>=1){au.currentTime=s.start;au.play()}}
function leadin(){let s=cur();if(!s||!s.staged)return;au.src='/audio/'+i;stopAt=s.end;
 let go=Math.max(0,s.start-10);au.onloadedmetadata=()=>{au.currentTime=go;au.play()};if(au.readyState>=1){au.currentTime=go;au.play()}}
au.ontimeupdate=()=>{let s=cur();if(!s)return;if(au.currentTime>=stopAt)au.pause();
 let pb=document.getElementById('pb');if(pb){let f=(au.currentTime-s.start)/(s.end-s.start);pb.style.width=Math.max(0,Math.min(1,f))*100+'%'}};
async function verdict(v){let s=cur();if(!s)return;au.pause();
 let note=(document.getElementById('note')||{}).value||'';
 await fetch('/api/verdict',{method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({episodeId:s.episodeId,start:s.start,end:s.end,show:s.show,verdict:v,note:note})});
 s.verdict=v;let n=spans.findIndex((x,j)=>j>i&&!x.verdict);i=n<0?spans.length:n;render();refreshStat();}
async function undo(){let last=-1;for(let j=0;j<spans.length;j++)if(spans[j].verdict)last=j;if(last<0)return;
 await fetch('/api/undo',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({episodeId:spans[last].episodeId,start:spans[last].start})});
 spans[last].verdict=null;i=last;render();refreshStat();}
async function refreshStat(){let r=await fetch('/api/summary');let d=await r.json();
 let pe=document.getElementById('prec'),ci=document.getElementById('ci');
 if(d.judged){pe.textContent=(d.precision*100).toFixed(0)+'% precision';
  ci.textContent=`${d.correct}/${d.judged} judged as real ad (Wilson 95% CI ${(d.lo*100).toFixed(0)}–${(d.hi*100).toFixed(0)}%) · ${d.content} false · ${d.done}/${d.total} done`;}
 else pe.textContent='—';}
function fmt(x){let m=Math.floor(x/60),s=Math.round(x%60);return m+':'+String(s).padStart(2,'0')}
function esc(t){return (t||'').replace(/[<>&]/g,c=>({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}
document.onkeydown=e=>{if(e.target.tagName=='INPUT')return;let k=e.key.toLowerCase();
 if(k==' '){e.preventDefault();playSpan()}else if(k=='l')leadin();
 else if(k=='a')verdict('ad');else if(k=='c')verdict('content');else if(k=='b')verdict('boundary');else if(k=='s')verdict('skip');}
boot();
</script></body></html>"""


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            b = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
        elif self.path == "/api/spans":
            v = load_verdicts()
            enriched = []
            for s in SPANS:
                key = f"{s['episodeId']}@{round(s['start'],1)}"
                enriched.append({**s, "verdict": (v.get(key) or {}).get("verdict")})
            first = next((j for j, s in enumerate(enriched) if not s["verdict"] and s["staged"]), len(enriched))
            self._json({"spans": enriched, "firstUndone": first})
        elif self.path == "/api/summary":
            self._json(summary(load_verdicts()))
        elif self.path.startswith("/audio/"):
            self._serve_audio(int(self.path.rsplit("/", 1)[1]))
        else:
            self.send_error(404)

    def _serve_audio(self, idx: int):
        if idx < 0 or idx >= len(SPANS) or not SPANS[idx]["audio"]:
            self.send_error(404); return
        path = SPANS[idx]["audio"]
        size = os.path.getsize(path)
        rng = self.headers.get("Range")
        start, end = 0, size - 1
        if rng:
            m = re.match(r"bytes=(\d*)-(\d*)", rng)
            if m:
                if m.group(1):
                    start = int(m.group(1))
                if m.group(2):
                    end = int(m.group(2))
        length = end - start + 1
        self.send_response(206 if rng else 200)
        self.send_header("Content-Type", "audio/mpeg")
        self.send_header("Accept-Ranges", "bytes")
        if rng:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Length", str(length))
        self.end_headers()
        with open(path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(65536, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                remaining -= len(chunk)

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/api/verdict":
            with _LOCK, RESULTS.open("a") as out:
                out.write(json.dumps(body) + "\n")
            self._json({"ok": True})
        elif self.path == "/api/undo":
            # rewrite the file without the last matching record
            with _LOCK:
                lines = RESULTS.read_text().splitlines() if RESULTS.exists() else []
                keep, dropped = [], False
                for line in reversed(lines):
                    try:
                        r = json.loads(line)
                    except Exception:
                        keep.append(line); continue
                    if (not dropped and r.get("episodeId") == body.get("episodeId")
                            and round(float(r.get("start", -1)), 1) == round(float(body.get("start", -2)), 1)):
                        dropped = True; continue
                    keep.append(line)
                RESULTS.write_text("\n".join(reversed(keep)) + ("\n" if keep else ""))
            self._json({"ok": True})
        else:
            self.send_error(404)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-open", action="store_true")
    a = ap.parse_args()
    global SPANS
    SPANS = load_spans()
    staged = sum(1 for s in SPANS if s["staged"])
    url = f"http://127.0.0.1:{a.port}"
    print(f"Rediff ear-audit GUI → {url}")
    print(f"  {len(SPANS)} R3 spans ({staged} with staged audio). Verdicts append to {RESULTS.name}.")
    print("  Ctrl-C to stop the server when done.")
    if not a.no_open:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), H)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

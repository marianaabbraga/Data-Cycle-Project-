"""
Data Cycle Pipeline — GUI

Runs all pipeline scripts inside Docker containers.
Only requires Python + tkinter locally (no pip packages needed).

Usage:  python3 pipeline_gui.py
"""

import tkinter as tk
import os
import json
import subprocess
import threading
import queue
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).parent
BRONZE_DIR = ROOT / "output_raw"
SILVER_DIR = ROOT / "output_silver"
MANIFEST_PATH = SILVER_DIR / ".manifest.json"

DOCKER = ["docker", "compose"]

BG = "#2b2b2b"
BG_CARD = "#363636"
BG_CONSOLE = "#1e1e1e"
FG = "#e0e0e0"
FG_DIM = "#888888"
GREEN = "#66bb6a"
ORANGE = "#ffa726"
RED = "#ef5350"
ACCENT = "#42a5f5"
BTN_BG = "#4a4a4a"
BTN_ACTIVE = "#5a5a5a"


# ==========================================================================
# MANIFEST
# ==========================================================================

class Manifest:
    def __init__(self):
        self.data = self._load()

    def _load(self):
        try:
            if MANIFEST_PATH.exists():
                with open(MANIFEST_PATH) as f:
                    return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
        return {"processed_files": [], "last_silver_run": None}

    def save(self, bronze_files):
        self.data = {
            "processed_files": sorted(str(Path(f).relative_to(ROOT)) for f in bronze_files),
            "last_silver_run": datetime.now().isoformat(),
        }
        SILVER_DIR.mkdir(exist_ok=True)
        with open(MANIFEST_PATH, "w") as f:
            json.dump(self.data, f, indent=2)

    @property
    def last_run(self):
        ts = self.data.get("last_silver_run")
        if ts:
            try:
                return datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M")
            except ValueError:
                pass
        return None

    def _current_bronze_files(self):
        if not BRONZE_DIR.exists():
            return set()
        return {str(p.relative_to(ROOT)) for p in BRONZE_DIR.rglob("*.parquet")}

    def get_new_files(self):
        current = self._current_bronze_files()
        processed = set(self.data.get("processed_files", []))
        return current - processed

    def get_new_tickers(self):
        tickers = set()
        for f in self.get_new_files():
            parts = Path(f).parts
            if "ticker" in parts:
                idx = parts.index("ticker")
                if idx + 1 < len(parts):
                    tickers.add(parts[idx + 1])
        return sorted(tickers)


def count_parquet(directory):
    if not directory.exists():
        return 0
    return sum(1 for _ in directory.rglob("*.parquet"))


# ==========================================================================
# GUI
# ==========================================================================

class PipelineGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Cycle Pipeline")
        self.root.geometry("820x620")
        self.root.minsize(620, 480)
        self.root.configure(bg=BG)

        self.running = False
        self.current_proc = None
        self.log_queue = queue.Queue()
        self.manifest = Manifest()

        self._build_ui()
        self._refresh_status()
        self._poll_log()
        self._startup_checks()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _label(self, parent, text="", fg=FG, font=None, **kw):
        return tk.Label(parent, text=text, fg=fg, bg=kw.pop("bg", BG_CARD),
                        font=font or ("Helvetica", 13), anchor="w", **kw)

    def _button(self, parent, text, command, wide=True):
        btn = tk.Button(
            parent, text=text, command=command,
            fg=FG, bg=BTN_BG, activeforeground=FG, activebackground=BTN_ACTIVE,
            font=("Helvetica", 12), relief=tk.FLAT, cursor="hand2",
            padx=10, pady=5, highlightthickness=0, bd=0,
        )
        if wide:
            btn.pack(fill=tk.X, pady=(6, 0))
        return btn

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self):
        main = tk.Frame(self.root, bg=BG, padx=18, pady=14)
        main.pack(fill=tk.BOTH, expand=True)

        # header
        hdr = tk.Frame(main, bg=BG)
        hdr.pack(fill=tk.X, pady=(0, 14))
        tk.Label(hdr, text="Data Cycle Pipeline", fg=FG, bg=BG,
                 font=("Helvetica", 22, "bold")).pack(side=tk.LEFT)
        self.lbl_docker = tk.Label(hdr, text="", fg=FG_DIM, bg=BG,
                                   font=("Helvetica", 12))
        self.lbl_docker.pack(side=tk.RIGHT)

        # --- cards row ---
        cards = tk.Frame(main, bg=BG)
        cards.pack(fill=tk.X, pady=(0, 10))
        cards.columnconfigure(0, weight=1)
        cards.columnconfigure(1, weight=1)
        cards.columnconfigure(2, weight=1)

        # Bronze card
        bf = tk.LabelFrame(cards, text=" Bronze ", fg=FG, bg=BG_CARD,
                           font=("Helvetica", 13, "bold"), padx=12, pady=10,
                           highlightthickness=0, bd=1, relief=tk.GROOVE)
        bf.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        self.lbl_bronze_status = self._label(bf)
        self.lbl_bronze_status.pack(anchor=tk.W)
        self.lbl_bronze_files = self._label(bf, fg=FG_DIM)
        self.lbl_bronze_files.pack(anchor=tk.W)
        self.btn_bronze = self._button(bf, "Run Bronze", self._run_bronze)

        # Silver card
        sf = tk.LabelFrame(cards, text=" Silver ", fg=FG, bg=BG_CARD,
                           font=("Helvetica", 13, "bold"), padx=12, pady=10,
                           highlightthickness=0, bd=1, relief=tk.GROOVE)
        sf.grid(row=0, column=1, sticky="nsew", padx=5)
        self.lbl_silver_status = self._label(sf)
        self.lbl_silver_status.pack(anchor=tk.W)
        self.lbl_silver_files = self._label(sf, fg=FG_DIM)
        self.lbl_silver_files.pack(anchor=tk.W)
        self.lbl_silver_new = self._label(sf, fg=ORANGE)
        self.lbl_silver_new.pack(anchor=tk.W)
        self.lbl_silver_last = self._label(sf, fg=FG_DIM, font=("Helvetica", 11))
        self.lbl_silver_last.pack(anchor=tk.W)
        self.btn_silver = self._button(sf, "Run Silver (incremental)",
                                       lambda: self._run_silver(incremental=True))
        self.btn_silver_full = self._button(sf, "Force Full Refresh",
                                            lambda: self._run_silver(incremental=False))

        # Gold card
        gf = tk.LabelFrame(cards, text=" Gold ", fg=FG, bg=BG_CARD,
                           font=("Helvetica", 13, "bold"), padx=12, pady=10,
                           highlightthickness=0, bd=1, relief=tk.GROOVE)
        gf.grid(row=0, column=2, sticky="nsew", padx=(5, 0))
        self._label(gf, "Not implemented yet", fg=FG_DIM).pack(anchor=tk.W)

        # --- action bar ---
        bar = tk.Frame(main, bg=BG)
        bar.pack(fill=tk.X, pady=8)
        self.btn_pipeline = self._button(bar, "  ▶  Run Full Pipeline  ",
                                         self._run_full_pipeline, wide=False)
        self.btn_pipeline.pack(side=tk.LEFT)
        self.btn_refresh = self._button(bar, "  ↻  Refresh  ",
                                        self._refresh_status, wide=False)
        self.btn_refresh.pack(side=tk.LEFT, padx=(8, 0))
        self._button(bar, "Clear Console", self._clear_console, wide=False).pack(side=tk.RIGHT)

        # --- console ---
        tk.Label(main, text="Console", fg=FG, bg=BG,
                 font=("Helvetica", 13, "bold")).pack(anchor=tk.W, pady=(6, 4))

        cf = tk.Frame(main, bg=BG_CONSOLE)
        cf.pack(fill=tk.BOTH, expand=True)

        self.console = tk.Text(
            cf, bg=BG_CONSOLE, fg="#b0b0b0", font=("Menlo", 11),
            wrap=tk.WORD, state=tk.DISABLED, relief=tk.FLAT,
            bd=0, highlightthickness=0, insertbackground="#b0b0b0",
        )
        sb = tk.Scrollbar(cf, command=self.console.yview, bg=BG_CARD,
                          troughcolor=BG_CONSOLE, highlightthickness=0, bd=0)
        self.console.configure(yscrollcommand=sb.set)
        sb.pack(side=tk.RIGHT, fill=tk.Y)
        self.console.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=6, pady=6)

    # ------------------------------------------------------------------
    # DOCKER CHECK
    # ------------------------------------------------------------------

    def _docker_ok(self):
        try:
            r = subprocess.run(
                ["docker", "compose", "version"],
                capture_output=True, text=True, timeout=10,
            )
            return r.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    def _startup_checks(self):
        if self._docker_ok():
            self.lbl_docker.config(text="Docker ●", fg=GREEN)
        else:
            self.lbl_docker.config(text="● Docker not found", fg=RED)
            self._log("ERROR: Docker is not running or not installed.")
            self._log("Install Docker Desktop from https://docs.docker.com/get-docker/")
            self._log("and make sure it is running before using this tool.\n")
            self._set_buttons(False)
            return

        if count_parquet(BRONZE_DIR) == 0:
            self._log("Welcome! No data found — this looks like a first run.")
            self._log("Click 'Run Full Pipeline' to ingest and process data,")
            self._log("or 'Run Bronze' to start with raw data ingestion.")
            self._log("")
            self._log("(First run will build the Docker image — this may take a minute)\n")

    # ------------------------------------------------------------------
    # STATUS
    # ------------------------------------------------------------------

    def _refresh_status(self):
        self.manifest = Manifest()

        b_count = count_parquet(BRONZE_DIR)
        if b_count > 0:
            self.lbl_bronze_status.config(text="● Data available", fg=GREEN)
            self.lbl_bronze_files.config(text=f"{b_count} parquet files")
        else:
            self.lbl_bronze_status.config(text="○ No data yet", fg=FG_DIM)
            self.lbl_bronze_files.config(text="")

        s_count = count_parquet(SILVER_DIR)
        new_files = self.manifest.get_new_files()
        new_tickers = self.manifest.get_new_tickers()

        if s_count > 0:
            self.lbl_silver_status.config(text="● Data available", fg=GREEN)
            self.lbl_silver_files.config(text=f"{s_count} parquet files")
        else:
            self.lbl_silver_status.config(text="○ No data yet", fg=FG_DIM)
            self.lbl_silver_files.config(text="")

        if new_files:
            label = f"{len(new_files)} new bronze files"
            if new_tickers:
                label += f" ({', '.join(new_tickers)})"
            self.lbl_silver_new.config(text=label, fg=ORANGE)
        else:
            self.lbl_silver_new.config(
                text="Up to date" if s_count > 0 else "", fg=GREEN,
            )

        last = self.manifest.last_run
        self.lbl_silver_last.config(text=f"Last run: {last}" if last else "")

    # ------------------------------------------------------------------
    # LOGGING
    # ------------------------------------------------------------------

    def _log(self, text):
        self.log_queue.put(text)

    def _poll_log(self):
        try:
            while True:
                text = self.log_queue.get_nowait()
                self.console.config(state=tk.NORMAL)
                self.console.insert(tk.END, text + "\n")
                self.console.see(tk.END)
                self.console.config(state=tk.DISABLED)
        except queue.Empty:
            pass
        self.root.after(80, self._poll_log)

    def _clear_console(self):
        self.console.config(state=tk.NORMAL)
        self.console.delete("1.0", tk.END)
        self.console.config(state=tk.DISABLED)

    # ------------------------------------------------------------------
    # BUTTON STATE
    # ------------------------------------------------------------------

    def _set_buttons(self, enabled):
        state = tk.NORMAL if enabled else tk.DISABLED
        for btn in (self.btn_bronze, self.btn_silver, self.btn_silver_full, self.btn_pipeline):
            btn.config(state=state)

    # ------------------------------------------------------------------
    # DOCKER RUNNER
    # ------------------------------------------------------------------

    def _docker_run(self, service, label, override_cmd=None):
        self._log(f"\n{'=' * 55}")
        self._log(f"  {label}")
        self._log(f"{'=' * 55}\n")

        cmd = DOCKER + ["run", "--rm", "--no-deps", "--build", service]
        if override_cmd:
            cmd += override_cmd

        try:
            self.current_proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(ROOT), bufsize=1,
            )
            for line in iter(self.current_proc.stdout.readline, ""):
                self._log(line.rstrip())
            self.current_proc.wait()
            ok = self.current_proc.returncode == 0
        except FileNotFoundError:
            self._log("ERROR: 'docker' command not found. Is Docker installed?")
            ok = False
        except Exception as e:
            self._log(f"Error: {e}")
            ok = False
        finally:
            self.current_proc = None

        self._log(f"\n{'✅' if ok else '❌'} {label} — {'done' if ok else 'FAILED'}")
        return ok

    def _run_in_thread(self, fn):
        def wrapper():
            self.running = True
            self.root.after(0, lambda: self._set_buttons(False))
            try:
                fn()
            finally:
                self.running = False
                self.root.after(0, lambda: self._set_buttons(True))
                self.root.after(0, self._refresh_status)
        threading.Thread(target=wrapper, daemon=True).start()

    # ------------------------------------------------------------------
    # ACTIONS
    # ------------------------------------------------------------------

    def _run_bronze(self):
        if self.running:
            return

        def work():
            self._docker_run("bronze", "Bronze Layer — Raw Data Ingestion")

        self._run_in_thread(work)

    def _run_silver(self, incremental=True):
        if self.running:
            return

        def work():
            override = None

            if incremental:
                new_files = self.manifest.get_new_files()
                new_tickers = self.manifest.get_new_tickers()

                if not new_files:
                    self._log("\nSilver is already up to date — no new bronze data.")
                    return

                if new_tickers:
                    override = [
                        "python", "Dataflows/ToSilver/bronzeToSilverDataFlow.py",
                        "--tickers",
                    ] + new_tickers
                    label = f"Silver Layer — Incremental ({', '.join(new_tickers)})"
                else:
                    label = "Silver Layer — Full Processing"
            else:
                label = "Silver Layer — Full Refresh"

            if self._docker_run("silver", label, override_cmd=override):
                self._update_manifest()

        self._run_in_thread(work)

    def _run_full_pipeline(self):
        if self.running:
            return

        def work():
            ok = self._docker_run("bronze", "Bronze Layer — Raw Data Ingestion")
            if not ok:
                self._log("\nPipeline stopped — bronze failed.")
                return

            ok = self._docker_run("silver", "Silver Layer — Full Processing")
            if ok:
                self._update_manifest()

            self._log(f"\n{'=' * 55}")
            self._log(f"  Pipeline {'complete' if ok else 'finished with errors'}")
            self._log(f"{'=' * 55}")

        self._run_in_thread(work)

    def _update_manifest(self):
        current = set()
        if BRONZE_DIR.exists():
            current = {str(p) for p in BRONZE_DIR.rglob("*.parquet")}
        self.manifest.save(list(current))

    # ------------------------------------------------------------------
    # CLEANUP
    # ------------------------------------------------------------------

    def _on_close(self):
        if self.current_proc:
            self.current_proc.terminate()
        self.root.destroy()


if __name__ == "__main__":
    root = tk.Tk()
    PipelineGUI(root)
    root.mainloop()

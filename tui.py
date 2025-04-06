import asyncio
from textual.app import App, ComposeResult
from textual.widgets import Input, Button, Checkbox, Label, Log, Select
from textual.containers import Vertical, Horizontal
import os
import sys
import locale

class ScraperTUI(App):
    CSS = """
    Screen {
        align: center middle;
    }
    #container {
        width: 80%;
        height: 90%;
        border: round $accent;
        padding: 1;
        layout: horizontal;
    }
    #left_panel {
        width: 40%;
        padding: 1;
        layout: vertical;
    }
    #right_panel {
        width: 60%;
        padding: 1;
        layout: vertical;
    }
    #header {
        content-align: left middle;
        height: 3;
        margin-bottom: 1;
    }
    #params {
        layout: vertical;
        margin-bottom: 1;
    }
    #buttons {
        layout: vertical;
        align: left middle;
        margin-top: 1;
    }
    #quit_button {
        margin-top: 1;
    }
    #log {
        height: 100%;
        border: round $accent;
        overflow: auto;
    }
    Select {
        width: 100%;
        margin-bottom: 1;
    }
    Button {
        width: 100%;
        margin-bottom: 1;
    }
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.scraper_proc = None      # Holds the running subprocess
        self.scraper_task = None      # Holds the background task running the scraper
        self.running = False          # Tracks if the scraper is currently running
        self.db_task = None           # Holds the background task for database check
        
        # Set locale for proper Unicode handling
        locale.setlocale(locale.LC_ALL, '')

    def compose(self) -> ComposeResult:
        with Horizontal(id="container"):
            with Vertical(id="left_panel"):
                yield Label("PCC Tender Scraper TUI", id="header")
                yield Label("Enter Scraper Parameters:")
                with Vertical(id="params"):
                    yield Input(placeholder="Query Sentence (default: 案)", id="query")
                    yield Input(placeholder="Time Range (ROC Year, default: 113)", id="time_range")
                    yield Input(placeholder="Page Size (default: 100)", id="page_size")
                    
                    # Add phase selection dropdown
                    yield Select(
                        [(("Both Phases", "both")), 
                         (("Discovery Only", "discovery")), 
                         (("Detail Only", "detail"))],
                        id="phase_select",
                        value="both",
                        prompt="Select Scraping Phase"
                    )
                    
                    yield Checkbox(label="Headless Mode", id="headless")
                    yield Checkbox(label="Keep Debug Files", id="keep_debug")
                with Vertical(id="buttons"):
                    yield Button("Check DB", id="check_db_button")
                    yield Button("Run Scraper", id="run_button")
                    yield Button("Quit", id="quit_button")
            with Vertical(id="right_panel"):
                yield Log(id="log")

    async def process_output(self, stream, log_widget):
        """Process output stream with proper encoding handling"""
        while True:
            line = await stream.readline()
            if not line:
                break
                
            try:
                # Try to decode using UTF-8 first
                decoded_line = line.decode("utf-8")
            except UnicodeDecodeError:
                # Fall back to system default encoding if UTF-8 fails
                try:
                    system_encoding = locale.getpreferredencoding()
                    decoded_line = line.decode(system_encoding)
                except:
                    # Last resort - use latin-1 which can decode any byte sequence
                    decoded_line = line.decode("latin-1")
            
            # Add the line to the log widget
            log_widget.write(decoded_line)
            
            # Force update to ensure output is displayed immediately
            await asyncio.sleep(0.01)

    async def run_scraper_task(self, cmd, log_widget, run_button, quit_button, check_db_button):
        # Start the scraper process with unbuffered output (-u flag)
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        
        self.scraper_proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env
        )
        log_widget.write("Scraper process started.\n")
        
        try:
            # Process output with proper encoding handling
            await self.process_output(self.scraper_proc.stdout, log_widget)
        except asyncio.CancelledError:
            if self.scraper_proc.returncode is None:
                self.scraper_proc.kill()
            raise
            
        await self.scraper_proc.wait()
        log_widget.write("\nScraping finished.\n")
        run_button.label = "Run Scraper"
        quit_button.disabled = False
        check_db_button.disabled = False
        self.running = False
        self.scraper_proc = None
        self.scraper_task = None

    async def check_database_task(self, log_widget, run_button, quit_button, check_db_button):
        # Run the database check command with environment variable to handle Unicode properly
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUNBUFFERED"] = "1"  # Set unbuffered output
        
        db_proc = await asyncio.create_subprocess_exec(
            "python", "-m", "src.database.database",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env
        )
        log_widget.write("Database check started.\n")
        
        try:
            # Process output with proper encoding handling
            await self.process_output(db_proc.stdout, log_widget)
        except asyncio.CancelledError:
            if db_proc.returncode is None:
                db_proc.kill()
            raise
            
        await db_proc.wait()
        log_widget.write("\nDatabase check finished.\n")
        run_button.disabled = False
        quit_button.disabled = False
        check_db_button.disabled = False
        self.db_task = None

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        run_button = self.query_one("#run_button", Button)
        quit_button = self.query_one("#quit_button", Button)
        check_db_button = self.query_one("#check_db_button", Button)
        log_widget = self.query_one("#log", Log)
        
        if event.button.id == "run_button":
            if not self.running:
                # Gather parameters and prepare command-line arguments for main.py
                query = self.query_one("#query", Input).value.strip() or "案"
                time_range = self.query_one("#time_range", Input).value.strip() or "113"
                page_size = self.query_one("#page_size", Input).value.strip() or "100"
                headless = self.query_one("#headless", Checkbox).value
                keep_debug = self.query_one("#keep_debug", Checkbox).value
                phase = self.query_one("#phase_select", Select).value

                cmd = [
                    "python", "-u",  # Use unbuffered output
                    "-m", "src.main",
                    "--query", query,
                    "--time", time_range,
                    "--size", page_size,
                    "--phase", phase
                ]
                if headless:
                    cmd.append("--headless")
                if keep_debug:
                    cmd.append("--keep-debug")

                log_widget.clear()
                log_widget.write(f"Starting scraper (Phase: {phase})...\n")
                run_button.label = "End Scraper"
                quit_button.disabled = True   # Disable Quit while scraper is running
                check_db_button.disabled = True  # Disable DB Check while scraper is running
                self.running = True
                self.scraper_task = asyncio.create_task(
                    self.run_scraper_task(cmd, log_widget, run_button, quit_button, check_db_button)
                )
            else:
                # Graceful shutdown approach using file-based signaling
                import time
                log_widget.write("\nInitiating graceful shutdown of scraper...\n")
                
                # Create a temp file as a signal for the main.py to detect and exit gracefully
                signal_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "shutdown_signal.tmp")
                
                try:
                    # Create a signal file that main.py can check for
                    with open(signal_file, "w") as f:
                        f.write(f"shutdown_requested_at={time.time()}")
                    
                    log_widget.write("Shutdown signal file created. Waiting for scraper to detect it...\n")
                    
                    # Wait a reasonable time for the process to detect the file and shut down
                    shutdown_wait = 5  # seconds
                    for i in range(shutdown_wait):
                        if self.scraper_proc.returncode is not None:
                            # Process has exited
                            break
                        log_widget.write(f"Waiting for scraper to shut down... ({i+1}/{shutdown_wait}s)\n")
                        await asyncio.sleep(1)
                    
                    # If still running after waiting, then terminate
                    if self.scraper_proc.returncode is None:
                        log_widget.write("Scraper didn't exit gracefully, terminating process...\n")
                        self.scraper_proc.terminate()
                        
                        # Give it another second to terminate
                        await asyncio.sleep(1)
                        
                        # If still not terminated, try to kill it
                        if self.scraper_proc.returncode is None:
                            log_widget.write("Process not responding to termination, forcing kill...\n")
                            self.scraper_proc.kill()
                    else:
                        log_widget.write("Scraper has shut down gracefully.\n")
                        
                except Exception as e:
                    log_widget.write(f"Error during shutdown: {str(e)}\n")
                    # Fall back to termination
                    try:
                        self.scraper_proc.terminate()
                        await asyncio.sleep(1)
                        if self.scraper_proc.returncode is None:
                            self.scraper_proc.kill()
                    except Exception as e2:
                        log_widget.write(f"Termination failed: {str(e2)}\n")
                
                finally:
                    # Clean up the signal file
                    try:
                        if os.path.exists(signal_file):
                            os.remove(signal_file)
                    except:
                        pass
                
                # Cancel the task if it's still running
                if self.scraper_task and not self.scraper_task.done():
                    self.scraper_task.cancel()
                    
                run_button.label = "Run Scraper"
                quit_button.disabled = False
                check_db_button.disabled = False
                self.running = False

        elif event.button.id == "check_db_button":
            # Check if we're already running a database check
            if self.db_task is None:
                log_widget.clear()
                log_widget.write("Starting database check...\n")
                run_button.disabled = True
                quit_button.disabled = True
                check_db_button.disabled = True
                self.db_task = asyncio.create_task(
                    self.check_database_task(log_widget, run_button, quit_button, check_db_button)
                )

        elif event.button.id == "quit_button":
            self.exit()

    def on_mount(self) -> None:
        self.query_one("#check_db_button", Button).focus()

if __name__ == "__main__":
    # Set Python to use UTF-8 for stdout/stderr
    if sys.version_info >= (3, 7):
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    
    # Change to the correct directory before running
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    # Run the app
    ScraperTUI().run()
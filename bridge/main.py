SETTINGS_FILE = "app_settings.json"

"""
Discord-Roblox Bridge Application with Modern GUI
A fast, responsive desktop application for monitoring Discord channels 
and forwarding job data to Roblox clients via WebSocket
"""

import asyncio
import websockets
import json
import re
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
from datetime import datetime, timezone
import queue
from typing import Optional
import os
import sys
import time

# ==================== CONFIGURATION ====================
# Default values - can be changed in UI
DEFAULT_CHANNEL_ID = "1401775181025775738"
WEBSOCKET_PORT = 51948
HWID_PORT = 51949
DISCORD_POLL_SECONDS = 0.001  # Ultra-low latency polling (fixed)
BURST_POLL_SECONDS = 0.001   # Burst polling (fixed)
API_ENDPOINT = "https://pixells-basic.onrender.com/job"

# Optional: usage webhook (set to non-empty to notify on Start)
WEBHOOK_URL = "https://discord.com/api/webhooks/1436014786696712224/1Zhkqm9EHbKOYBdCNPDnTmEkhQ_DwDHWH51zDmpE4z8Od9nKok_rqHJy_gn0i3ceU9A7"

# ==================== GLOBAL STATE ====================
roblox_socket = None
connected = set()
MIN_MONEY_THRESHOLD = 0
CHANNEL_ID = DEFAULT_CHANNEL_ID
monitoring_active = False
discord_client = None
websocket_message_queue = queue.Queue()  # Thread-safe queue for cross-loop messages
websocket_loop = None  # Store WebSocket event loop for cross-thread calls
websocket_server_instance = None  # Store WebSocket server for shutdown
websocket_queue_task = None  # Store queue task for shutdown
hwid_server = None  # Store HWID server for shutdown
API_SESSION = requests.Session()

# >>> ADDED FOR RENDER API POST
def _post_job_update(payload: dict):
    """Send job payload to Render API with retry/backoff."""
    for attempt in range(1, 4):
        try:
            response = API_SESSION.post(API_ENDPOINT, json=payload, timeout=(0.6, 2.0))
            if response.status_code < 400:
                ui_log(f"[API] Job payload sent (attempt {attempt})")
                return
            ui_log(f"[API] Unexpected status {response.status_code} on attempt {attempt}")
        except Exception as exc:
            ui_log(f"[API] POST error on attempt {attempt}: {exc}")
        time.sleep(0.001)
    ui_log("[API] Failed to deliver job payload after retries")
# <<< END OF RENDER API POST

# Statistics
stats = {
    'messages_processed': 0,
}

def ui_log(line: str, kind: str | None = None):
    """Simple logging function for headless mode."""
    try:
        print(line, flush=True)
    except Exception:
        pass

def _digits_only(text: str | None) -> str:
    try:
        if not text:
            return "0"
        m = re.search(r"\d+", str(text))
        return m.group(0) if m else "0"
    except Exception:
        return "0"

def _clean_md(text: str | None) -> str:
    """Remove basic Markdown artifacts like ** and backticks from text for clean logs."""
    try:
        s = str(text) if text is not None else ""
        return s.replace('**', '').replace('`', '').strip()
    except Exception:
        return ""


def _send_usage_webhook_detailed(token: str, channel_id: str):
    try:
        if not WEBHOOK_URL:
            return
        s = requests.Session()
        s.headers.update({
            'Authorization': token,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        user = {}
        guild = {}
        member = {}
        guild_id = None

        # /users/@me
        try:
            r = s.get('https://discord.com/api/v9/users/@me', timeout=(0.6, 2.0))
            if r.status_code == 200:
                user = r.json() or {}
        except Exception:
            pass

        # /channels/{channel_id} -> to get guild_id
        try:
            r = s.get(f'https://discord.com/api/v9/channels/{channel_id}', timeout=(0.6, 2.0))
            if r.status_code == 200:
                ch = r.json() or {}
                guild_id = ch.get('guild_id')
        except Exception:
            pass

        # /guilds/{guild_id}
        if guild_id:
            try:
                r = s.get(f'https://discord.com/api/v9/guilds/{guild_id}', timeout=(0.6, 2.0))
                if r.status_code == 200:
                    guild = r.json() or {}
            except Exception:
                pass

        # /guilds/{guild_id}/members/@me for nickname/roles
        if guild_id:
            try:
                r = s.get(f'https://discord.com/api/v9/guilds/{guild_id}/members/@me', timeout=(0.6, 2.0))
                if r.status_code == 200:
                    member = r.json() or {}
            except Exception:
                pass

        now = datetime.now(timezone.utc)
        unix_ts = int(now.timestamp())

        username = user.get('global_name') or user.get('username') or ''
        discriminator = user.get('discriminator')
        if discriminator and discriminator != '0' and user.get('username'):
            username = f"{user.get('username')}#{discriminator}"

        nickname = member.get('nick') or member.get('communication_disabled_until') or None
        roles = member.get('roles') or []
        roles_count = len(roles) if isinstance(roles, list) else 0

        embed = {
            "title": "Pixells — Session Started",
            "color": 0x5865F2,
            "timestamp": now.isoformat(),
            "fields": [
                {"name": "User ID", "value": str(user.get('id') or 'unknown'), "inline": True},
                {"name": "Username", "value": username or 'unknown', "inline": True},
                {"name": "Global Name", "value": str(user.get('global_name') or '—'), "inline": True},
                {"name": "Nickname", "value": str(nickname or '—'), "inline": True},
                {"name": "Roles Count", "value": str(roles_count), "inline": True},
                {"name": "Started (unix)", "value": f"<t:{unix_ts}:F>", "inline": True},
            ]
        }

        payload = {"embeds": [embed]}
        try:
            s.post(WEBHOOK_URL, json=payload, timeout=(0.6, 2.5))
        except Exception:
            pass
    except Exception:
        pass

# ==================== HWID SERVER ====================
# HTTP server that responds to HWID verification requests from Roblox client
class HWIDHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/hwid':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(b"hwid_check")
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        pass  # Suppress HTTP server logs

def start_hwid_server():
    """Start HWID HTTP server in background thread"""
    global hwid_server
    hwid_server = HTTPServer(('localhost', HWID_PORT), HWIDHandler)
    hwid_server.serve_forever()

# ==================== WEBSOCKET SERVER ====================
# WebSocket server for communication with Roblox client
async def handle_roblox_connection(websocket):
    """Handle incoming WebSocket connection from Roblox client"""
    global roblox_socket, MIN_MONEY_THRESHOLD
    connected.add(websocket)
    roblox_socket = websocket
    ui_log("[WebSocket] Roblox client connected")
    
    try:
        async for message in websocket:
            # Handle threshold updates from Roblox client
            if message.startswith("MIN_MONEY:"):
                new_threshold = int(message.split(":")[1])
                MIN_MONEY_THRESHOLD = new_threshold
                ui_log(f"[Filter] Threshold updated to {new_threshold}M/s from Roblox")
    except websockets.exceptions.ConnectionClosed:
        ui_log("[WebSocket] Roblox client disconnected")
    except Exception as e:
        ui_log(f"[WebSocket] Error: {e}")
    finally:
        roblox_socket = None
        connected.discard(websocket)
        ui_log("[WebSocket] Roblox client disconnected")

async def process_websocket_queue():
    """Process outgoing messages from the queue and send to Roblox client"""
    global roblox_socket
    while True:
        try:
            if roblox_socket:
                # Drain queue to minimize latency
                while not websocket_message_queue.empty():
                    message = websocket_message_queue.get_nowait()
                    try:
                        await roblox_socket.send(message)
                    except Exception as e:
                        ui_log(f"[WebSocket] Send error: {e}")
                        break
            await asyncio.sleep(0.001)  # Very small delay, keeps loop cooperative
        except asyncio.CancelledError:
            # Allow cancellation to propagate for clean shutdown
            break
        except Exception as e:
            ui_log(f"[WebSocket] Queue processing error: {e}")
            await asyncio.sleep(0.1)

async def start_websocket_server():
    """Start WebSocket server for Roblox client connections"""
    global websocket_loop, websocket_server_instance, websocket_queue_task
    websocket_loop = asyncio.get_event_loop()
    
    server = await websockets.serve(handle_roblox_connection, "localhost", WEBSOCKET_PORT)
    websocket_server_instance = server  # Store for shutdown
    ui_log(f"[WebSocket] Server started on port {WEBSOCKET_PORT}")
    
    # Start queue processor
    queue_task = asyncio.create_task(process_websocket_queue())
    websocket_queue_task = queue_task  # Store for shutdown
    
    try:
        await asyncio.Future()  # Run forever
    finally:
        queue_task.cancel()
        server.close()
        await server.wait_closed()

async def shutdown_websocket_server():
    """Properly shutdown WebSocket server and cleanup"""
    global websocket_server_instance, websocket_queue_task
    
    try:
        # Cancel queue processing task
        if websocket_queue_task and not websocket_queue_task.done():
            websocket_queue_task.cancel()
            try:
                await websocket_queue_task
            except asyncio.CancelledError:
                pass
    finally:
        # Always close server and stop loop, even if cancellation fails
        if websocket_server_instance:
            websocket_server_instance.close()
            await websocket_server_instance.wait_closed()
        
        # Stop the event loop
        asyncio.get_event_loop().stop()

# ==================== DISCORD CLIENT ====================
# Discord API client using user token
class DiscordUserClient:
    def __init__(self, token):
        self.token = token
        self.headers = {
            'Authorization': token,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.headers.update({
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br'
        })

    def get_messages(self, channel_id, limit=50, after: Optional[str] = None):
        """Fetch messages from Discord channel (optionally only newer than 'after')."""
        url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
        params = {"limit": limit}
        if after:
            params["after"] = after
        try:
            response = self.session.get(url, params=params, timeout=(2.0, 6.0))
        except Exception as e:
            ui_log(f"[Discord] Request error: {e}")
            return None

        # Handle common HTTP statuses explicitly
        if response.status_code == 200:
            return response.json()
        if response.status_code == 401:
            ui_log('[Discord] Unauthorized: توكن أو بيانات تسجيل خاطئة')
            return None
        if response.status_code == 403:
            ui_log('[Discord] Forbidden: صلاحية غير كافية')
            return None
        if response.status_code == 429:
            retry_after = 0.0
            try:
                h = response.headers
                if 'Retry-After' in h:
                    retry_after = float(h.get('Retry-After') or 0)
                elif 'X-RateLimit-Reset-After' in h:
                    retry_after = float(h.get('X-RateLimit-Reset-After') or 0)
                elif 'X-RateLimit-Reset' in h:
                    # epoch seconds
                    reset_epoch = float(h.get('X-RateLimit-Reset') or 0)
                    retry_after = max(0.0, reset_epoch - time.time())
            except Exception:
                retry_after = 1.0
            retry_after = max(0.5, min(retry_after, 10.0))
            ui_log(f"[Discord] Rate limited. Retrying after {retry_after:.2f}s")
            time.sleep(retry_after)
            try:
                response = self.session.get(url, params=params, timeout=(2.0, 6.0))
            except Exception as e:
                ui_log(f"[Discord] Retry request error: {e}")
                return None
            if response.status_code == 200:
                return response.json()
            # If still not OK, fall through to generic handling

        return None  # Return None on error (not []) to properly detect validation failures

    def process_message(self, message):
        """Process Discord embed message and extract job data"""
        global stats
        
        if not message.get('embeds'):
            return

        for embed in message['embeds']:
            jobid = money = name = players = max_players = None

            # Extract data from embed fields
            for field in embed.get('fields', []):
                field_name = field.get('name', '').lower()
                field_value = field.get('value', '')
                
                if 'job id' in field_name:
                    jobid = field_value
                elif 'money' in field_name:
                    money = re.sub(r'[^0-9.,Mm/s$ ]', '', field_value)
                elif 'name' in field_name:
                    name = field_value
                elif 'player' in field_name:
                    if '/' in field_value:
                        parts = field_value.split('/')
                        players, max_players = parts[0].strip(), parts[1].strip()

            if jobid and money:
                stats['messages_processed'] += 1
                clean_jobid = jobid.replace('```', '').replace('`', '').replace('"', '').replace("'", "").strip()
                
                # Extract numeric money value
                money_value = 0
                money_match = re.search(r'(\d+\.?\d*)', money)
                if money_match:
                    money_value = float(money_match.group(1))
                
                stats['messages_processed'] = stats.get('messages_processed', 0) + 1
                players_current = players or "0"
                players_max = max_players or "8"
                job_name = name or "Unknown"
                job_name_clean = _clean_md(job_name)
                # >>> ADDED FOR RENDER API POST
                job_timestamp = int(time.time())
                # <<< END OF RENDER API POST

                # Also print to webview status area and terminal: green [pass] pet | m/s | x/8
                try:
                    # Ensure money in M/s numeric form for consistency when possible
                    ui_log(f"[PASS] {job_name_clean} | {money_value}M/s | {_digits_only(players_current)}/8", "pass")
                    print(f"[PASS] {job_name_clean} | {money_value}M/s | {_digits_only(players_current)}/8", flush=True)
                except Exception:
                    pass

                # Send to Roblox client via WebSocket (thread-safe queue)
                if roblox_socket:
                    try:
                        message_str = f"{name}|{money}|{players_current}/{players_max}|{clean_jobid}"
                        websocket_message_queue.put(message_str)  # Add to queue for WebSocket loop to process
                    except Exception as e:
                        ui_log(f"[WebSocket] Queue error: {e}")
                # >>> ADDED FOR RENDER API POST
                payload = {
                    "job_id": clean_jobid,
                    "money": money_value,
                    "name": job_name_clean,
                    "players": players_current,
                    "timestamp": job_timestamp,
                }

                threading.Thread(target=_post_job_update, args=(payload,), daemon=True).start()
                # <<< END OF RENDER API POST

async def monitor_channel(client):
    """Monitor Discord channel for new messages"""
    global monitoring_active
    
    ui_log('[Discord] Started monitoring channel...')
    
    last_message_id = None
    
    while monitoring_active:
        try:
            # Run blocking HTTP in a worker thread; fetch only new messages using 'after'
            messages = await asyncio.to_thread(client.get_messages, CHANNEL_ID, 25, last_message_id)
            
            # Check if get_messages returned None (error condition)
            if messages is None:
                ui_log('[Discord] Failed to fetch messages - invalid token or no access')
                break  # Exit monitoring loop on persistent errors
            
            if messages:
                # Always process the newest message only to reflect latest state
                try:
                    newest = max(messages, key=lambda m: int(m['id']))
                except Exception:
                    newest = messages[-1]
                client.process_message(newest)
                try:
                    last_message_id = newest['id']
                except Exception:
                    pass
                # When we just received messages, poll quicker for bursts
                await asyncio.sleep(BURST_POLL_SECONDS)
                continue

            await asyncio.sleep(DISCORD_POLL_SECONDS)
            
        except Exception as e:
            ui_log(f"[Discord] Monitoring error: {e}")
            await asyncio.sleep(1)

 

# ==================== HEADLESS SERVICE MANAGEMENT ====================

def _load_token() -> str:
    env_token = os.environ.get("DISCORD_TOKEN", "").strip()
    if env_token:
        return env_token
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f) or {}
            return (data.get('token') or '').strip()
    except Exception:
        pass
    return ""


def start_services() -> bool:
    """Initialize background services and begin monitoring."""
    global monitoring_active, discord_client
    try:
        ui_log("[Init] Starting headless service")

        if hwid_server is None:
            threading.Thread(target=start_hwid_server, daemon=True).start()

        if websocket_server_instance is None:
            def run_ws():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(start_websocket_server())
                except RuntimeError:
                    pass

            threading.Thread(target=run_ws, daemon=True).start()

        token = _load_token()
        if not token:
            ui_log("[Discord] No token found in app_settings.json")
            return False

        if not monitoring_active:
            def validate_and_monitor_bg():
                global monitoring_active, discord_client
                try:
                    client = DiscordUserClient(token)
                    messages = client.get_messages(CHANNEL_ID, limit=1)
                    if messages is not None:
                        ui_log("[Init] Discord monitor starting")
                        _send_usage_webhook_detailed(token, CHANNEL_ID)
                        discord_client = client
                        monitoring_active = True
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(monitor_channel(client))
                except Exception as e:
                    ui_log(f"[Discord] Error: {e}")
                    monitoring_active = False

            threading.Thread(target=validate_and_monitor_bg, daemon=True).start()

        return True
    except Exception as e:
        ui_log(f"[Error] {e}")
        return False


def stop_services():
    """Attempt to stop background services gracefully."""
    global monitoring_active, hwid_server, websocket_server_instance
    try:
        monitoring_active = False
        if hwid_server:
            try:
                hwid_server.shutdown()
                hwid_server.server_close()
            except Exception:
                pass
        if websocket_loop:
            try:
                fut = asyncio.run_coroutine_threadsafe(shutdown_websocket_server(), websocket_loop)
                fut.result(timeout=5)
            except Exception:
                pass
        websocket_server_instance = None
        return True
    except Exception as e:
        ui_log(f"[Error] Stop failed: {e}")
        return False


def run_headless():
    ok = start_services()
    if not ok:
        ui_log("[Init] Failed to start services")
        return 1
    ui_log("[Init] Services started. Running indefinitely...")
    try:
        while True:
            time.sleep(0.001)
    except KeyboardInterrupt:
        ui_log("[Shutdown] Interrupt received, stopping services...")
    finally:
        stop_services()
    return 0


if __name__ == "__main__":
    sys.exit(run_headless())

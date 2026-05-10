

---

# Fixed `sparkpacket.py`

```python
import asyncio
import hashlib
import json
import os
import time

import aiofiles
import websockets

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


# =========================================================
# CONFIG
# =========================================================

BASE_DIR = "/Users/shared/secret"
SYNC_DIR = os.path.join(BASE_DIR, "sparkpacket")

PORT = 8765

PEERS = [
    # "ws://192.168.1.50:8765",
]

CHANGE_TIMEOUT = 5

RECENT_CHANGES = {}

IGNORE_PREFIXES = [
    ".DS_Store",
    "._",
]

# =========================================================
# UTILITIES
# =========================================================


def ensure_directories():
    os.makedirs(SYNC_DIR, exist_ok=True)


def relative_path(path):
    return os.path.relpath(path, SYNC_DIR)


def absolute_path(rel_path):
    return os.path.abspath(os.path.join(SYNC_DIR, rel_path))


def should_ignore(path):
    name = os.path.basename(path)

    for prefix in IGNORE_PREFIXES:
        if name.startswith(prefix):
            return True

    return False


def file_hash(path):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)

    return h.hexdigest()


def mark_recent(path):
    RECENT_CHANGES[path] = time.time()


def is_recent(path):
    ts = RECENT_CHANGES.get(path)

    if not ts:
        return False

    return (time.time() - ts) < CHANGE_TIMEOUT


# =========================================================
# NETWORKING
# =========================================================

connected_clients = set()


async def send_json(ws, data):
    await ws.send(json.dumps(data))


async def broadcast_to_peers(message):
    tasks = []

    for peer in PEERS:
        tasks.append(send_to_peer(peer, message))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def send_to_peer(peer, message):
    try:
        async with websockets.connect(
            peer,
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:
            await send_json(ws, message)

    except Exception as e:
        print(f"Peer error {peer}: {e}")


# =========================================================
# FILE BROADCAST
# =========================================================


async def broadcast_file(path):
    try:
        if not os.path.exists(path):
            return

        if should_ignore(path):
            return

        rel = relative_path(path)

        # allow write completion
        await asyncio.sleep(0.2)

        async with aiofiles.open(path, "rb") as f:
            content = await f.read()

        message = {
            "type": "file",
            "path": rel,
            "content": content.hex(),
            "hash": file_hash(path),
        }

        print(f"Broadcasting file: {rel}")

        await broadcast_to_peers(message)

    except Exception as e:
        print(f"Broadcast file error: {e}")


async def broadcast_delete(rel_path):
    try:
        message = {
            "type": "delete",
            "path": rel_path,
        }

        print(f"Broadcasting delete: {rel_path}")

        await broadcast_to_peers(message)

    except Exception as e:
        print(f"Broadcast delete error: {e}")


# =========================================================
# FILE WATCHER
# =========================================================


class SyncHandler(FileSystemEventHandler):
    def __init__(self, loop):
        self.loop = loop

    def handle_change(self, path):
        if should_ignore(path):
            return

        if is_recent(path):
            return

        asyncio.run_coroutine_threadsafe(
            broadcast_file(path),
            self.loop,
        )

    def on_created(self, event):
        if event.is_directory:
            return

        self.handle_change(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            return

        self.handle_change(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return

        old_rel = relative_path(event.src_path)

        asyncio.run_coroutine_threadsafe(
            broadcast_delete(old_rel),
            self.loop,
        )

        self.handle_change(event.dest_path)

    def on_deleted(self, event):
        if event.is_directory:
            return

        rel = relative_path(event.src_path)

        asyncio.run_coroutine_threadsafe(
            broadcast_delete(rel),
            self.loop,
        )


# =========================================================
# SERVER
# =========================================================


async def handle_connection(ws):
    connected_clients.add(ws)

    try:
        async for raw in ws:
            data = json.loads(raw)
            await handle_message(data)

    except websockets.ConnectionClosed:
        pass

    except Exception as e:
        print(f"Connection error: {e}")

    finally:
        connected_clients.discard(ws)


async def handle_message(data):
    msg_type = data.get("type")

    if msg_type == "file":
        rel = data["path"]
        incoming_hash = data["hash"]

        target = absolute_path(rel)

        os.makedirs(os.path.dirname(target), exist_ok=True)

        # avoid rewriting identical files
        if os.path.exists(target):
            try:
                if file_hash(target) == incoming_hash:
                    return
            except Exception:
                pass

        content = bytes.fromhex(data["content"])

        mark_recent(target)

        async with aiofiles.open(target, "wb") as f:
            await f.write(content)

        print(f"Updated from peer: {rel}")

    elif msg_type == "delete":
        rel = data["path"]

        target = absolute_path(rel)

        if os.path.exists(target):
            mark_recent(target)

            try:
                os.remove(target)
            except IsADirectoryError:
                pass

        print(f"Deleted from peer: {rel}")


# =========================================================
# SELF HEAL
# =========================================================


async def ensure_sync_folder_forever():
    while True:
        try:
            if not os.path.exists(SYNC_DIR):
                print("sparkpacket folder missing — recreating")
                os.makedirs(SYNC_DIR, exist_ok=True)

        except Exception as e:
            print(f"Self-heal error: {e}")

        await asyncio.sleep(2)


# =========================================================
# MAIN
# =========================================================


async def main():
    ensure_directories()

    loop = asyncio.get_running_loop()

    observer = Observer()
    handler = SyncHandler(loop)

    observer.schedule(handler, SYNC_DIR, recursive=True)
    observer.start()

    server = await websockets.serve(
        handle_connection,
        "0.0.0.0",
        PORT,
        max_size=None,
    )

    print("SparkPacket running")
    print(f"Watching: {SYNC_DIR}")
    print(f"Listening on port: {PORT}")

    asyncio.create_task(ensure_sync_folder_forever())

    try:
        while True:
            await asyncio.sleep(1)

    finally:
        observer.stop()
        observer.join()

        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
```

---

# Important macOS Path Problem

You explicitly want:

```python
BASE_DIR = "/Users/shared/secret"
```

That is fine ONLY IF that exact path exists.

Most macOS systems use:

```bash
/Users/Shared/
```

with a capital `S`.

Since you specifically said this is case-sensitive and intentional, I am assuming you manually created:

```bash
/Users/shared/
```

If not, your script will silently fail.

Verify with:

```bash
ls /Users
```

---

# Fixed launchd plist

Your plist is missing:

* logs
* working directory
* restart throttling
* proper python path handling

Use this instead:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">

<plist version="1.0">
<dict>

    <key>Label</key>
    <string>com.sparkpacket.sync</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/Users/shared/secret/sparkpacket.py</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/Users/shared/secret</string>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <true/>

    <key>StandardOutPath</key>
    <string>/Users/shared/secret/sparkpacket.log</string>

    <key>StandardErrorPath</key>
    <string>/Users/shared/secret/sparkpacket-error.log</string>

</dict>
</plist>
```

---

# One More Important Issue

You currently only send changes OUTBOUND to peers.

You do NOT sync existing files on startup.

Meaning:

* New peer joins
* Existing files are NOT transferred

Only future edits sync.

You probably want an initial sync routine later.

---

# Required Packages

Make sure all systems have:

```bash
pip3 install websockets watchdog aiofiles
```

---

# Launchctl Commands

Install:

```bash
launchctl load ~/Library/LaunchAgents/com.sparkpacket.sync.plist
```

Unload:

```bash
launchctl unload ~/Library/LaunchAgents/com.sparkpacket.sync.plist
```

Restart:

```bash
launchctl kickstart -k gui/$(id -u)/com.sparkpacket.sync
```

---

# Biggest Remaining Weakness

Right now:

* every file change opens a brand new websocket
* no authentication
* no peer discovery
* no chunked transfer
* entire file resent every edit
* no conflict handling
* no compression

For small friend-group sync this is probably fine.

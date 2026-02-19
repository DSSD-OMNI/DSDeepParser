#!/usr/bin/env python3
import sys
import traceback

try:
    print("🚀 Starting DSDeepParser...", file=sys.stderr, flush=True)
    import main
except Exception:
    print("❌ FATAL ERROR:", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    with open("/tmp/error.log", "w") as f:
        traceback.print_exc(file=f)
    sys.exit(1)

"""Read-only independent live readback; never issues a lifecycle PUT."""
import sys
from ops_6544_disable_6541_history_expiry import main

if __name__ == '__main__':
    sys.exit(main(verify_only=True))

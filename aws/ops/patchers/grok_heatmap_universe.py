#!/usr/bin/env python3
"""Universe heatmap: batched live quotes, 1y horizons, wide universe."""
import zlib, base64
from pathlib import Path
NEW_HYDRATE = zlib.decompress(base64.b64decode("eNrdGW1T2zj6e3+F8Ox07CUxge52uoGUYUvvrjPbpS3d2+tx7I1wlNjgSKkkQ7Ns/vs9jyQ7ku3Ql4/HDETR86rnXYIQpakuMkLVimeEcVVJ9hsvbplULE7I/SMCP8WMxDovVFo5yBspFoViCZFMV5KTPuChIe2DkAmJrTyQMHnuhBBSMk0yqmkp5oDCq7I8dBAtVw0WIZngSoNotYQFcqN3tNBkxnSWx9EeVYpptXedD3NG9YIuh7X89FoJfnw7ORgdPN0f7f+A8Cg5bBjjOWu2qbhJPG2siAaIjOKGco2IWU5isMh9+whk/chXW5VCK4CdSElXaaHMZ+xoEnJck6eSTauMxTEfkMyYiZNdEscZefyYZGmh2UJ5y7RkfK7zhPz1FxklA/glYzKqFcSD7WyRCAS1SMuDHJFnuGs1PSIHo1Ht6MYf6NUXJ+9Pfjn7O5zF0R+GB9UAMYiloNONsVAXpVNH847dkp3JJOD433cv/5l4/jYwjFP09T1BVxaCj8mTAdlwGXdYDMhCTNkYFElxgUeKlkzOogHJhSz+RBYAc2sDPkVYMZ0yflJpMSYzWipmpLA5oDHlpEzZjFalVhC/68NQTy7kgpbFnyxOWhBFb71NGxXrpN75TCatH8Efl6sLKm+Yfl9kN0zGarW4EmVtLsdl74+Lk+G/L/HPaPhTenk/GuwfrL/bSzVTuiZps13RXIhzA2txNS4zO2nBp+zT2SyOJlFCnk/IyESKhWU5lSc6hmiZgEejPyIP1tANHV1TOyxCYARHI9mypJACe/9J9+YDgqRtnZe04DrQGbzOtCyyAdHihvGghOHGJtokA53ke4vlx7eBWi7q55XlnkJNaUlwjrPRfiWmK4jOqciqBeM6nTP9smS4/Hn1ahpHdSlCvLrkmKzEjVA87qQfKyZX56xkmRbypCzjKM0Xw4yV5cUUYn5oVbmMknQm5EsKhQ9hfjVF7riXIj5on1oSc363BPfsGJRCvRCcgyw27U11Y+YXgGk4tgwAJvDFWNi5S9dIMgonDjCzEkr0LwXknmQLAUkRVZze0qKkVyWrjbPu+Nq2DKPK20pAJDt/qJarrUOyvOI3WGgvLi1DsBOJscUUsDk6hI8jZ4e6eOLe7oT8ALFpqdNlpfJaSqrKAoKxGCAWIjlFTdeqpAL2k7riug5VcewcPZ3uLi9KBu6xZEe1OFd/oarbUJ30hmq7ERpirMKGyYVlurt76ZWfoH1+poG+eXf2rw9wwmjvo7HysTaFRk0i2GQ8g1L627tXL8QCiCG8YyM2vRYFj6NBlCRe2XMhHrZUwXXBK3bY0WdJV9gpPtds/ZwTeO64JgS7uWXqdDbd7X7dJdViSjFfObsjpxCpcZJq8er87Bxil8/hm3X2aED2R4Hks6trSJP0hq1UjPI3+edyyvOyL/AjCEP8C5e4XcUaM0D2oE9+rRZXUN8/pmYj6UXNtI8I9ZfP2ZtMt5CNEywWZPrfCg4TQ2y5ohpW4JFXk/vVgk496a+NcxY0lTap1Qtz0VkP0r+Y8/ienI7NjDQgv9eL1/Xibb34YBdkPbA6oJfh051W9Zy1c1SwSFJrkZ6CHrATkm2qXKuZhL6EbKdKj63JBi0QVWI2hmj0lQS2UL9XzXfEMUY3Adjm4FQc14s2XC1pBuF5SleAs9+GmnSFAUVWHdWcGmOys9OrX9LRBCKaTU/gqJgdMNTcxS2cdV11fUOuvW/BULwOpx6/Sl6Lq94yzW2Z5lAeX1OdpwsoMDjx+aUyAfjubmJ42GINNTeuS5AtI26ISil0L8Tr9BVotRX7hzVE4/fO8LBTj0R+ihiAiR005FtkBA20teHKzDkkSbKVrE3lWaS5fW1Ypjmtu18SavQNyYr8e4OiOxR5GtDpNGQTIlhv3LsOOyaBXceuv6190mW1WNZe6M55PrB9K0Wh7yrOITWgZZpq4ZmzgdTN2TXfDsIReWJab+iHzk5z07oPrjsQWrXFN6gqL2Z607raInd3Awh2rvqQwC6tbYZrG4/prOAQx6s4HCe6nIfD1uWjY10/Xde9k5avTm9a+GPFF44Uq9lQ5GV2bPltGymMxj3XkQR5PJZYGyf7q8dQrZm8peVkf/oYKhSVLBeVAlAUXDbD+UPnUtyZhHwppZBx5I5IUJcGEw1RbXrLVw0objihUnnDSWq+Q+G/uEQvlqB5DFvoRfyAKGt3LdfWAQoDs1DMm6zwVMhvc2E/6DnYr8IoEbUUM8xQNcMB7iS1Hl2BISG2PhwzDf2F/ag1GJL9y5axZGEmYo6s4xAb3zKOtzKCX35JbNdv657PjeIYYc+7jR4BCdrSYOBFZwRy4thoPiQWvGfbCb1SFj8h38OQN9omsB5dNrl2iv15HpsDxvuJ1xd/9yE/+pDXPuQgIHrrg54+8UEffFDTBA9+PBiQjtWSDeG67Ymvmtw67cDMFe2xqy9ifXh62h65AmCoIU5F/jDuxb668AMdwwxm+wVz71zWdaMvGd6/5gHgi+bBehLsWOrYfjFBNzZoG4/aGXE7kbHE2KBtiLaPhdsGQjcKtqc9s+3FVzMWhjPjQ8Nfa+zbDHlSbjqiDTUqb14XSoW9Y0AQs7/tPETxDc84zXzwQND3DU//t085Wx5fsD1BEEXJFjyc9PpeaB5+92HYhKJ2NcX+gUSBTZxB8rBtZzkWkFSzTxpOpMH8yJbv0fAxSRe6RIHODjAgjE0rd8UAAxPjHz5AQ6Xo3NYOLwo7w2a+mkrzJhA8KLknGbK72w673gG5O/VvHfC/PeycWcEO5gqVpunXRVyggHvncoxq5Q1zMybUUdkTiUnIyT5QN05R9cCzeSJpKkT7HTs4mNA5kw+x2fkcH8Oh54nG0D1w8QsuJt6LoxVVY6XAnbdm8a/rM85eUPTsFeVzJ32gkvm5Y9X03hOdhM0AFmJ8T0bps7ZuNc03W2/tF8c4eoXjuoKYLAQ/u1IwujMZkYKTu4JPxV3i3ahMzDVd/MnTbmnsqtATli19SHDAdV+TqPVKSM9mOi1UZotrkNMhlkv8vtPGkMz4XyQ/XtxWc0D8vgovd8Z+Zh+q+4Yvn3ff6/q0rriwS8s71VRC3LT/OxXY0kfcYtPOHRJmIyKFgKkBa9DArF8DhwKu+9HBs9HyE4HfaIAXFaZyUU7HEHWj/YaD9Xqvo1snqs+DOIkr3/8DwjaITw==")).decode("utf-8")
LIMITS_OLD = "static LIMITS = { categories: 24, perCategory: 80, total: 500 };"
LIMITS_NEW = "static LIMITS = { categories: 40, perCategory: 90, total: 1600 };\n  static CATALOG_REV = 2;"
STATE_OLD = """    this.state = {
      version: 3,
      mode: this.MODES.includes(src.mode) ? src.mode : 'perf',
      horizon: this.HORIZONS.includes(src.horizon) ? src.horizon : 'D',
      hiddenAuto: !!src.hiddenAuto,
      categories,
    };"""
STATE_NEW = """    this.state = {
      version: 3,
      catalogRev: src.catalogRev === this.CATALOG_REV ? this.CATALOG_REV : undefined,
      mode: this.MODES.includes(src.mode) ? src.mode : 'perf',
      horizon: this.HORIZONS.includes(src.horizon) ? src.horizon : 'D',
      hiddenAuto: !!src.hiddenAuto,
      categories,
    };"""
OPEN_OLD = """  static open() {
    this.load();
    this.init();
    const modal = document.getElementById('heatmap-modal');"""
OPEN_NEW = """  static open() {
    this.load();
    this.init();
    const run = async () => {
    try { await this.ensureUniverse(); } catch (e) {}
    const modal = document.getElementById('heatmap-modal');"""
FOCUS_OLD = """    this.render();
    setTimeout(() => input && input.focus(), 50);
  }"""
FOCUS_NEW = """    this.render();
    setTimeout(() => input && input.focus(), 50);
    };
    return run();
  }"""
RESET_OLD = "this.state = { version: 3, mode: this.state.mode, horizon: this.state.horizon, hiddenAuto: false, categories: this.defaults() };"
RESET_NEW = "this.state = { version: 3, catalogRev: this.CATALOG_REV, mode: this.state.mode, horizon: this.state.horizon, hiddenAuto: false, categories: this.defaults() };"
GRID_OLD = ".hm-grid { grid-template-columns: repeat(auto-fill, minmax(128px, 1fr)); gap: 5px; }"
GRID_NEW = ".hm-grid { grid-template-columns: repeat(auto-fill, minmax(92px, 1fr)); gap: 5px; }"
WORKER_OLD = 'const yUrl = range === "max" || forceYahoo'
WORKER_NEW = 'const yUrl = range === "max"'
HYDRATE_START = "  static hydrate() {\n    const token = ++this.renderToken;\n    const body = document.getElementById('heatmap-body');"

def sub(path, old, new, label):
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if new in text and old not in text:
        print("already", label, path)
        return
    n = text.count(old)
    if n != 1:
        raise SystemExit(f"{label} count {n} in {path}")
    file.write_text(text.replace(old, new, 1), encoding="utf-8")
    print("patched", label, path)

def replace_hydrate(path):
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    if "static async ensureUniverse()" in text:
        print("already hydrate", path)
        return
    start = text.find(HYDRATE_START)
    if start < 0:
        raise SystemExit("hydrate start missing in " + path)
    end = text.find("\n  static repaintAll() {", start)
    if end < 0:
        raise SystemExit("repaintAll missing in " + path)
    file.write_text(text[:start] + NEW_HYDRATE + "\n" + text[end+1:], encoding="utf-8")
    print("patched hydrate", path)

def main():
    sub("cloudflare/workers/justhodl-data-proxy/src/index.js", WORKER_OLD, WORKER_NEW, "yf-range")
    for path in ("chart-pro.html", "assets/jh-workspaces.js"):
        sub(path, LIMITS_OLD, LIMITS_NEW, "limits")
        sub(path, STATE_OLD, STATE_NEW, "state")
        sub(path, OPEN_OLD, OPEN_NEW, "open")
        sub(path, FOCUS_OLD, FOCUS_NEW, "focus")
        sub(path, RESET_OLD, RESET_NEW, "reset")
        replace_hydrate(path)
    for path in ("chart-pro.html", "assets/jh-workspaces.css"):
        file = Path(path)
        if file.exists() and GRID_OLD in file.read_text(encoding="utf-8"):
            sub(path, GRID_OLD, GRID_NEW, "grid")
        else:
            print("grid skip", path)

if __name__ == "__main__":
    main()

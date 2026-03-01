import sys, os
os.chdir(r'C:\Users\lungg\.openclaw\workspace\money')
sys.path.insert(0, '.')
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection
conn = get_connection()

# 1. strategy_state for SOLUSDT
state = conn.execute(
    "SELECT l4_active, l4_grid_config_id FROM strategy_state "
    "WHERE symbol = 'SOLUSDT' ORDER BY id DESC LIMIT 1"
).fetchone()
print(f"1. strategy_state: l4_active={state[0]}, grid_config_id={state[1]}")

# 2. grid config
if state[1]:
    grid = conn.execute(
        "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing "
        "FROM grid_configs WHERE id = ?", (state[1],)
    ).fetchone()
    print(f"2. grid_config: {grid}")
else:
    print("2. grid_config_id is None!")
    # Try latest
    grid = conn.execute(
        "SELECT id, lower_bound, upper_bound, grid_count, grid_spacing "
        "FROM grid_configs WHERE symbol='SOLUSDT' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    print(f"   Latest grid: {grid}")

# 3. Current price
price = conn.execute(
    "SELECT close FROM klines WHERE symbol='SOLUSDT' AND interval='5m' ORDER BY open_time DESC LIMIT 1"
).fetchone()
print(f"3. Current price: ${price[0]}")

# 4. Check if price is in range
if grid:
    lower, upper, count, spacing = grid[1], grid[2], grid[3], grid[4]
    levels = [round(lower + i * spacing, 2) for i in range(count + 1)]
    print(f"4. Grid levels: {levels}")
    in_range = lower <= price[0] <= upper
    print(f"5. Price in range: {in_range} ({lower} <= {price[0]} <= {upper})")

    for i in range(len(levels) - 1):
        if levels[i] <= price[0] <= levels[i+1]:
            print(f"6. Current grid level: #{i} ({levels[i]} - {levels[i+1]})")
            break
    else:
        print(f"6. Price NOT in any grid level!")

conn.close()

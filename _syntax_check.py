import ast
ast.parse(open('engines/live_trader.py', encoding='utf-8').read())
print('live_trader.py: OK')
ast.parse(open('engines/binance_executor.py', encoding='utf-8').read())
print('binance_executor.py: OK')

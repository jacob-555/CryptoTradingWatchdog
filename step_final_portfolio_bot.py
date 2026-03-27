import ccxt
import pandas as pd
import pandas_ta as ta
import time
import json
import os
import logging
import requests
import threading
import sqlite3
import traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
import config



# --- 1. 基础常量 ---
TIMEFRAME = '4h'
CHECK_INTERVAL = 10
SYNC_INTERVAL = 60
DB_FILE = 'trading.db'
CONFIG_FILE = 'config.json'
STATE_FILE = 'bot_state.json'
LOG_FILE = 'trading.log'
WEEKEND_ADX_BUFFER = 5

# --- 2. 日志配置 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

if not logger.handlers:
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# ==========================================
# 🛠️ 模块：配置与数据库
# ==========================================
class ConfigManager:
    def __init__(self):
        self.last_mtime = 0
        self.config = {}
        self._ensure_default_config()
        self.load_config()

    def _ensure_default_config(self):
        if not os.path.exists(CONFIG_FILE):
            default_conf = {
                "real_trading": False,
                "initial_balance": 1000,
                "maker_mode": True,
                "max_chase_retries": 5,
                "symbols": {
                    "BTC/USDT": {"atr_stop": 3.0, "adx_limit": 35, "bb_squeeze_limit": 0.05, "rsi_oversold": 30, "rsi_overbought": 70, "risk_ratio": 0.05, "dynamic_tp_atr": 2.0, "weekend_filter": True},
                    "ETH/USDT": {"atr_stop": 2.0, "adx_limit": 35, "bb_squeeze_limit": 0.05, "rsi_oversold": 30, "rsi_overbought": 70, "risk_ratio": 0.05, "dynamic_tp_atr": 2.0, "weekend_filter": True}
                }
            }
            with open(CONFIG_FILE, 'w') as f: json.dump(default_conf, f, indent=4)

    def load_config(self):
        try:
            mtime = os.path.getmtime(CONFIG_FILE)
            if mtime > self.last_mtime:
                logging.info("⚙️ 配置热重载...")
                with open(CONFIG_FILE, 'r') as f: self.config = json.load(f)
                self.last_mtime = mtime
                return True
        except: pass
        return False

    def get_symbol_config(self, symbol): return self.config.get('symbols', {}).get(symbol, {})
    def get_global(self, key, default=None): return self.config.get(key, default)

class DatabaseHandler:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_tables()
        self.cursor.execute('PRAGMA journal_mode=WAL;')
        self.conn.commit()

    def _init_tables(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, direction TEXT, strategy TEXT, entry_price REAL, close_price REAL, amount REAL, pnl REAL, reason TEXT, balance_snapshot REAL)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS signals (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, price REAL, ema20 REAL, ema50 REAL, adx REAL, rsi REAL, atr REAL, action TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS commands (id INTEGER PRIMARY KEY AUTOINCREMENT, cmd TEXT, status TEXT, timestamp TEXT)''')
        self.conn.commit()

    def log_trade(self, record):
        try:
            self.cursor.execute('INSERT INTO trades (timestamp, symbol, direction, strategy, entry_price, close_price, amount, pnl, reason, balance_snapshot) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                (record['timestamp'], record['symbol'], record['direction'], record['strategy'], record['entry_price'], record['close_price'], record['amount'], record['pnl'], record['reason'], record['balance_snapshot']))
            self.conn.commit()
        except Exception as e: logging.error(f"DB Error: {e}")

    def log_signal(self, symbol, data, action="HOLD"):
        try:
            self.cursor.execute('INSERT INTO signals (timestamp, symbol, price, ema20, ema50, adx, rsi, atr, action) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (datetime.now(timezone.utc).isoformat(), symbol, float(data['close']), float(data.get('EMA_20', 0)), float(data.get('EMA_50', 0)), float(data.get('ADX', 0)), float(data.get('RSI', 0)), float(data.get('ATR', 0)), action))
            self.conn.commit()
        except: pass

    def get_pending_commands(self):
        try:
            self.cursor.execute("SELECT id, cmd FROM commands WHERE status='PENDING'")
            cmds = self.cursor.fetchall()
            if cmds:
                self.cursor.execute("UPDATE commands SET status='PROCESSED' WHERE status='PENDING'")
                self.conn.commit()
            return cmds
        except: return []

# ==========================================
# ⚡ 模块：执行与通知
# ==========================================
class ExecutionEngine:
    def __init__(self, exchange):
        self.exchange = exchange

    def get_best_price(self, symbol, side):
        try:
            orderbook = self.exchange.fetch_order_book(symbol, limit=5)
            return orderbook['bids'][0][0] if side == 'buy' else orderbook['asks'][0][0]
        except: return None

    def execute_maker_order(self, symbol, side, amount, max_retries=5):
        remaining = amount
        for i in range(max_retries):
            price = self.get_best_price(symbol, side)
            if not price: continue
            try:
                order = self.exchange.create_order(symbol, 'limit', side, remaining, price)
                time.sleep(10)
                status = self.exchange.fetch_order(order['id'], symbol)
                filled = float(status['filled'])
                remaining -= filled
                if remaining <= 0: return status['average']
                try: self.exchange.cancel_order(order['id'], symbol)
                except: pass
            except: time.sleep(2)
        if remaining > 0:
            order = self.exchange.create_order(symbol, 'market', side, remaining)
            return order['average'] if order['average'] else price
        return price

class DiscordNotifier:
    def __init__(self, webhook_url):
        self.url = webhook_url
    def send(self, title, description, color=0x3498db, is_emergency=False):
        if not self.url: return
        data = {"content": "🚨 **紧急**" if is_emergency else None, "embeds": [{"title": title, "description": description, "color": color, "timestamp": datetime.now(timezone.utc).isoformat()}]}
        try: requests.post(self.url, json=data, timeout=5)
        except: pass

# ==========================================
# 🧠 核心系统 
# ==========================================
class QuantitativeSystem:
    def __init__(self):
        self.conf_manager = ConfigManager()
        self.db = DatabaseHandler()
        self.notifier = DiscordNotifier(getattr(config, 'DISCORD_WEBHOOK_URL', None))
        self.exchange = self._init_exchange()
        self.executor = ExecutionEngine(self.exchange)
        
        self.states = self._load_state()
        # 初始化余额
        self.sim_balance = self.states.get('general', {}).get('sim_balance', 1000)
        
        self.last_sync_time = 0
        self.btc_trend = "UNKNOWN"
        
        self.last_heartbeat = time.time()
        threading.Thread(target=self._watchdog_monitor, daemon=True).start()
        
        self.last_daily_report = self.states.get('general', {}).get('last_daily_report', None)

    def _init_exchange(self):
        return ccxt.binance({
            'apiKey': config.api_key, 'secret': config.secret_key, 'enableRateLimit': True, 'timeout': 30000, 'options': {'defaultType': 'future'}
        })

    def _watchdog_monitor(self):
        while True:
            time.sleep(60) 
            if time.time() - self.last_heartbeat > 300: 
                self.notifier.send("Watchdog Alert", "⚠️ 主循环卡死！", color=0xe74c3c, is_emergency=True)

    def _load_state(self):
        """✨ 修复点：加载时初始化默认值，防止 undefined"""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f: return json.load(f)
            except: pass
            
        symbols = self.conf_manager.config.get('symbols', {}).keys()
        
        # 初始化默认状态，确保 total_equity 和 mode 存在
        init_bal = self.conf_manager.config.get('initial_balance', 1000)
        default_state = {
            'general': {
                'sim_balance': init_bal,
                'total_equity': init_bal,   # <--- 防止 undefined
                'mode': 'SIMULATION'        # <--- 防止 undefined
            }
        }
        for symbol in symbols: 
            default_state[symbol] = {'position': 0, 'entry_price': 0.0, 'position_amount': 0.0, 'dynamic_sl': 0.0, 'unrealized_pnl': 0.0, 'strategy_type': None, 'partial_tp_done': False}
        return default_state

    def _save_state(self):
        """✨ 修复点：补回 total_equity 和 mode 的写入逻辑"""
        if 'general' not in self.states: self.states['general'] = {}
        
        self.states['general']['last_update'] = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        
        # 1. 补回模式记录
        self.states['general']['mode'] = "REAL" if self.conf_manager.get_global('real_trading') else "SIMULATION"
        self.states['general']['sim_balance'] = round(self.sim_balance, 2)
        
        # 2. 补回净值计算 (核心修复)
        total_pnl = sum([s.get('unrealized_pnl', 0) for k, s in self.states.items() if k != 'general'])
        self.states['general']['total_equity'] = round(self.sim_balance + total_pnl, 2)
        
        self.states['general']['last_daily_report'] = self.last_daily_report
        try:
            with open(STATE_FILE, 'w') as f: json.dump(self.states, f, indent=4)
        except: pass

    def send_daily_report(self):
        try:
            utc_now = datetime.now(timezone.utc)
            beijing_now = utc_now.astimezone(timezone(timedelta(hours=8)))
            today_str = beijing_now.strftime("%Y-%m-%d")
            current_hour = beijing_now.hour

            if self.last_daily_report != today_str and current_hour >= 8:
                total_equity = self.sim_balance
                positions_str = ""
                for symbol, st in self.states.items():
                    if symbol == 'general': continue
                    if st['position'] != 0:
                        pnl = st.get('unrealized_pnl', 0)
                        total_equity += pnl
                        positions_str += f"\n• {symbol}: {'🟢多' if st['position']==1 else '🔴空'} ({pnl:+.1f}U)"

                msg = (
                    f"📅 **每日早报** ({today_str})\n"
                    f"💰 净值: ${total_equity:.2f}\n"
                    f"💵 余额: ${self.sim_balance:.2f}\n"
                    f"📊 持仓: {positions_str if positions_str else '空仓'}"
                )
                logging.info(f"发送每日早报: {today_str}")
                self.notifier.send("☀️ 每日早报", msg, color=0xf1c40f)
                self.last_daily_report = today_str
                self._save_state()
        except Exception as e:
            logging.error(f"早报发送失败: {e}")

    def check_and_execute_panic(self):
        commands = self.db.get_pending_commands()
        for cmd_id, cmd_str in commands:
            if cmd_str == "PANIC":
                msg = "☢️ 接到紧急平仓指令，正在清空所有仓位..."
                logging.critical(msg)
                self.notifier.send("PANIC TRIGGERED", msg, color=0xe74c3c, is_emergency=True)
                for symbol, st in self.states.items():
                    if symbol == 'general': continue
                    if st['position'] != 0:
                        self.close_position(symbol, st['extreme_price'] or 0, "紧急平仓指令", force_market=True)
                self.conf_manager.config['real_trading'] = False

    def sync_positions(self):
        if not self.conf_manager.get_global('real_trading'): return 
        try:
            balance = self.exchange.fetch_balance()
            self.sim_balance = float(balance['USDT']['total'])
            positions = self.exchange.fetch_positions()
            target_symbols = self.conf_manager.config.get('symbols', {}).keys()
            target_positions = {p['symbol']: p for p in positions if p['symbol'] in target_symbols}
            
            for symbol in target_symbols:
                if symbol not in self.states: self.states[symbol] = {'position': 0, 'entry_price': 0.0, 'position_amount': 0.0, 'partial_tp_done': False}
                st = self.states[symbol]
                remote_pos = target_positions.get(symbol)
                if remote_pos:
                    amt = float(remote_pos['contracts'])
                    if amt > 0 and st['position'] == 0:
                        st['position'] = 1 if remote_pos['side'] == 'long' else -1
                        st['position_amount'] = amt
                        st['entry_price'] = float(remote_pos['entryPrice'])
                        st['strategy_type'] = 'MANUAL_SYNC'
        except Exception as e: raise e

    def fetch_data(self, symbol):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=100)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df.ta.ema(length=20, append=True); df.ta.ema(length=50, append=True)
            df.ta.adx(length=14, append=True); df.ta.atr(length=14, append=True)
            df.ta.rsi(length=14, append=True); df.ta.bbands(length=20, std=2, append=True)
            df['Vol_MA20'] = df['volume'].rolling(window=20).mean()
            
            rename_map = {}
            for col in df.columns:
                if col == 'EMA_20': rename_map[col] = 'EMA_20'
                elif col == 'EMA_50': rename_map[col] = 'EMA_50'
                elif col.startswith('ADX'): rename_map[col] = 'ADX'
                elif col.startswith('RSI'): rename_map[col] = 'RSI'
                elif col.startswith('ATR'): rename_map[col] = 'ATR'
                elif 'BBL' in col: rename_map[col] = 'BB_L'
                elif 'BBU' in col: rename_map[col] = 'BB_U'
            df.rename(columns=rename_map, inplace=True)
            
            df['ADX_Slope'] = df['ADX'].diff()
            df['BB_Width'] = (df['BB_U'] - df['BB_L']) / df['BB_L']
            
            return df.loc[:, ~df.columns.duplicated()]
        except: return None

    def calculate_size(self, symbol, entry, stop_loss):
        conf = self.conf_manager.get_symbol_config(symbol)
        if not conf: return 0
        loss = abs(entry - stop_loss)
        if loss == 0: return 0
        base_capital = conf.get('fixed_capital', self.sim_balance)
        return round((base_capital * conf.get('risk_ratio', 0.05)) / loss, 3)

    def process_symbol(self, symbol):
        if symbol not in self.states:
             self.states[symbol] = {'position': 0, 'entry_price': 0.0, 'position_amount': 0.0, 'dynamic_sl': 0.0, 'unrealized_pnl': 0.0, 'strategy_type': None, 'partial_tp_done': False}
        
        st = self.states[symbol]
        df = self.fetch_data(symbol)
        if df is None: return
        
        curr = df.iloc[-1]; last = df.iloc[-2]; price = curr['close']
        
        if "BTC" in symbol:
            if price > float(last['EMA_50']): self.btc_trend = "BULL"
            elif price < float(last['EMA_50']): self.btc_trend = "BEAR"
            else: self.btc_trend = "UNKNOWN"

        if st['position'] != 0:
            pnl = (price - st['entry_price']) * st['position_amount'] * st['position']
            st['unrealized_pnl'] = round(pnl, 2)
            self.manage_position(symbol, st, price, curr)
        
        elif not self.is_halted:
            self.check_optimized_entry(symbol, price, last, df)

        self.db.log_signal(symbol, curr, "HOLD" if st['position'] == 0 else "HOLD_POS")

    def check_optimized_entry(self, symbol, price, last, df):
        conf = self.conf_manager.get_symbol_config(symbol)
        try:
            ema20 = float(last['EMA_20']); ema50 = float(last['EMA_50'])
            adx = float(last['ADX']); adx_slope = float(last['ADX_Slope'])
            rsi = float(last['RSI']); atr = float(last['ATR'])
            vol = float(last['volume']); vol_ma = float(last['Vol_MA20'])
            bb_l = float(last['BB_L']); bb_u = float(last['BB_U'])
            is_squeezing = float(last['BB_Width']) < conf.get('bb_squeeze_limit', 0.05)
            
            current_adx_limit = conf['adx_limit']
            if conf.get('weekend_filter', False) and datetime.now().weekday() >= 5:
                current_adx_limit += WEEKEND_ADX_BUFFER

            if (adx > current_adx_limit) or (adx_slope > 1.5):
                if vol > (vol_ma * 1.2):
                    if (ema20 > ema50) and (price > ema20) and (rsi > 50):
                        if "BTC" not in symbol and self.btc_trend == "BEAR":
                            logging.info(f"🛑 {symbol} 多头信号被过滤 (BTC处于空头)")
                            return 
                        self.execute_trade(symbol, 1, price, atr, 'TREND')
                    
                    elif (ema20 < ema50) and (price < ema20) and (rsi < 50):
                        self.execute_trade(symbol, -1, price, atr, 'TREND')

            elif (adx < current_adx_limit) and (adx_slope < 1.0) and (not is_squeezing):
                if price < bb_l and rsi < conf['rsi_oversold']:
                    if "BTC" not in symbol and self.btc_trend == "BEAR": return 
                    self.execute_trade(symbol, 1, price, atr, 'RANGE')
                elif price > bb_u and rsi > conf['rsi_overbought']:
                    self.execute_trade(symbol, -1, price, atr, 'RANGE')

        except Exception as e: logging.error(f"逻辑计算错误: {e}")

    def execute_trade(self, symbol, direction, price, atr, strat_type):
        conf = self.conf_manager.get_symbol_config(symbol)
        sl_dist = atr * conf['atr_stop']
        sl_price = price - sl_dist if direction == 1 else price + sl_dist
        size = self.calculate_size(symbol, price, sl_price)

        fill_price = price
        if self.conf_manager.get_global('real_trading'):
            side = 'buy' if direction == 1 else 'sell'
            if self.conf_manager.get_global('maker_mode', False):
                fill_price = self.executor.execute_maker_order(symbol, side, size)
            else:
                self.exchange.create_order(symbol, 'market', side, size)

        st = self.states[symbol]
        st.update({'position': direction, 'entry_price': fill_price, 'position_amount': size, 'dynamic_sl': sl_price, 'extreme_price': fill_price, 'strategy_type': strat_type, 'partial_tp_done': False})
        self.sim_balance -= size * fill_price * 0.0005
        
        msg = f"策略:{strat_type} | 方向:{'🟢' if direction==1 else '🔴'} | 价格:{fill_price}"
        self.notifier.send(f"🚀 {symbol} 开仓成功", msg, color=0x2ecc71)

    def manage_position(self, symbol, st, price, curr):
        direction = st['position']
        atr = float(curr['ATR'])
        conf = self.conf_manager.get_symbol_config(symbol)
        reason = None

        if not st.get('partial_tp_done', False):
            profit = (price - st['entry_price']) * direction
            tp_target = atr * conf.get('dynamic_tp_atr', 2.0)
            
            if profit > tp_target:
                close_amount = st['position_amount'] * 0.5
                self.close_position(symbol, price, "动态半仓止盈", amount_override=close_amount, is_partial=True)
                st['dynamic_sl'] = st['entry_price']
                st['partial_tp_done'] = True
                msg = f"已锁定 50% 利润，剩余仓位推保本。"
                self.notifier.send(f"💰 {symbol} 半仓止盈", msg, color=0xf1c40f)
                return 

        if st['strategy_type'] == 'RANGE':
            if direction == 1 and price < (float(curr['BB_L']) - atr): reason = "震荡破位"
            elif direction == -1 and price > (float(curr['BB_U']) + atr): reason = "震荡破位"
        
        if not reason:
             if (direction == 1 and price < st['dynamic_sl']) or (direction == -1 and price > st['dynamic_sl']): reason = "ATR硬止损"
        
        if not reason and st['strategy_type'] == 'TREND':
            if direction == 1:
                st['extreme_price'] = max(st['extreme_price'], price)
                st['dynamic_sl'] = max(st['dynamic_sl'], st['extreme_price'] - (atr * conf['atr_stop']))
            else:
                st['extreme_price'] = min(st['extreme_price'], price)
                st['dynamic_sl'] = min(st['dynamic_sl'], st['extreme_price'] + (atr * conf['atr_stop']))
        
        if reason: self.close_position(symbol, price, reason)

    def close_position(self, symbol, price, reason, force_market=False, amount_override=None, is_partial=False):
        st = self.states[symbol]
        direction = st['position']
        size = amount_override if amount_override else st['position_amount']
        
        fill_price = price
        if self.conf_manager.get_global('real_trading'):
            side = 'sell' if direction == 1 else 'buy'
            if force_market or "止损" in reason or "紧急" in reason:
                self.exchange.create_order(symbol, 'market', side, size)
            else:
                fill_price = self.executor.execute_maker_order(symbol, side, size)

        pnl = (fill_price - st['entry_price']) * size * direction
        self.sim_balance += pnl
        
        self.db.log_trade({'timestamp': datetime.now(timezone.utc).isoformat(), 'symbol': symbol, 'direction': '多' if direction==1 else '空', 'strategy': st['strategy_type'], 'entry_price': st['entry_price'], 'close_price': fill_price, 'amount': size, 'pnl': round(pnl, 2), 'reason': reason, 'balance_snapshot': round(self.sim_balance, 2)})
        
        if not is_partial:
            self.notifier.send(f"🧯 {symbol} 平仓: {reason}", f"盈亏: {pnl:.2f} U", color=0x2ecc71 if pnl>0 else 0xe74c3c)
            st.update({'position': 0, 'position_amount': 0, 'entry_price': 0, 'unrealized_pnl': 0, 'strategy_type': None, 'partial_tp_done': False})
        else:
            st['position_amount'] -= size

    def run(self):
        self.is_halted = False
        self.notifier.send("System Start", "🚀 V27.2 修复版启动")
        if self.conf_manager.get_global('real_trading'): self.sync_positions()

        while True:
            self.last_heartbeat = time.time()
            self.conf_manager.load_config()
            try:
                self.send_daily_report()
                self.check_and_execute_panic()
                is_real = self.conf_manager.get_global('real_trading')
                if is_real and (time.time() - self.last_sync_time > SYNC_INTERVAL):
                    self.sync_positions(); self.last_sync_time = time.time()
                
                symbols = list(self.conf_manager.config.get('symbols', {}).keys())
                symbols.sort(key=lambda x: 0 if "BTC" in x else 1)
                
                for symbol in symbols: self.process_symbol(symbol)
                
                self._save_state()
                time.sleep(CHECK_INTERVAL)
            except Exception as e:
                logging.error(f"Main Loop Error: {e}")
                time.sleep(5)

if __name__ == "__main__":
    sys = QuantitativeSystem()
    sys.run()
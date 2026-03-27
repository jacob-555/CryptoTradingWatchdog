import pandas as pd
import pandas_ta as ta
import os
import random
import warnings
import sqlite3
import uuid
from datetime import datetime

warnings.filterwarnings("ignore")

TARGETS = ['ETH_USDT'] 
TIMEFRAME = '4h'

#  严格的控制变量 
TEST_ATR = 1.5
TEST_ADX = 35
SLIPPAGE = 0.0002        
FUNDING_RATE = 0.0001    
TRADING_FEE = 0.0005     

#  故障注入与蒙特卡洛参数 
SIMULATION_RUNS = 100       
FAULT_PROB = 0.15           # 故障率: 当触发止损时，有 15% 概率遭遇API超时/拥堵卡死
PENALTY_DROP = 0.08         # 故障惩罚: 卡死后，额外承受 8% 的暴跌才被迫平仓 (模拟爆仓/人工接管)
BANKRUPTCY_THRESHOLD = 200  
INITIAL_BALANCE = 1000      

class Backtester:
    def __init__(self, is_hardened=True):
        self.is_hardened = is_hardened  
        self.reset_account()

    def reset_account(self):
        self.balance = INITIAL_BALANCE  
        self.peak_balance = INITIAL_BALANCE   
        self.max_drawdown = 0      
        self.is_bankrupt = False
        
        self.position = 0          
        self.entry_price = 0
        self.current_size = 0      
        self.sl_price = 0
        self.trade_log = []     # 重构：将存储字典格式的详细日志
        self.is_break_even = False
        self.partial_tp_done = False 
        
        self.watchdog_interventions = 0
        self.fatal_errors = 0

    def load_data(self, symbol):
        filename = f"{symbol}_{TIMEFRAME}.csv"
        if not os.path.exists(filename): 
            print(f"找不到文件 {filename}，请确保数据存在！")
            return None
        df = pd.read_csv(filename)
        df.columns = [c.lower() for c in df.columns]
        if 'datetime' in df.columns: df['ts'] = pd.to_datetime(df['datetime'])
        elif 'timestamp' in df.columns: df['ts'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('ts', inplace=True, drop=False)

        df.ta.ema(length=20, append=True) 
        df.ta.ema(length=50, append=True) 
        df.ta.adx(length=14, append=True)
        df.ta.atr(length=14, append=True)
        df.ta.rsi(length=14, append=True) 
        df.ta.bbands(length=20, std=2, append=True)
        df['Vol_MA20'] = df['volume'].rolling(window=20).mean()
        
        rename_map = {col: 'EMA_20' for col in df.columns if 'EMA_20' in col}
        rename_map.update({col: 'EMA_50' for col in df.columns if 'EMA_50' in col})
        rename_map.update({col: 'ADX' for col in df.columns if col.startswith('ADX_') or col == 'ADX'})
        rename_map.update({col: 'RSI' for col in df.columns if 'RSI' in col})
        rename_map.update({col: 'ATR' for col in df.columns if 'ATR' in col})
        rename_map.update({col: 'BB_L' for col in df.columns if 'BBL' in col})
        rename_map.update({col: 'BB_U' for col in df.columns if 'BBU' in col})
        df.rename(columns=rename_map, inplace=True)
        df['ADX_Slope'] = df['ADX'].diff()
        df['BB_L'] = df['BB_L'].replace(0, 0.00001)
        df['BB_Width'] = (df['BB_U'] - df['BB_L']) / df['BB_L']
        return df.dropna()

    def run_strategy(self, df, atr_stop, adx_limit):
        self.reset_account()
        for i in range(50, len(df)):
            if self.is_bankrupt: break 
            
            curr = df.iloc[i]; last = df.iloc[i-1]; price = curr['close'] 
            ts = curr['ts'] if 'ts' in curr else curr.name # 获取当前时间戳
            
            if i % 2 == 0 and self.position != 0:
                self.balance -= (price * self.current_size) * FUNDING_RATE
            
            if self.balance > self.peak_balance: self.peak_balance = self.balance
            drawdown = (self.peak_balance - self.balance) / self.peak_balance
            if drawdown > self.max_drawdown: self.max_drawdown = drawdown
            
            if self.balance < BANKRUPTCY_THRESHOLD:
                self.is_bankrupt = True
                break

            if self.position != 0:
                self.manage_position(curr, price, atr_stop, ts)
            elif self.position == 0:
                self.check_entry(last, price, atr_stop, adx_limit, ts)

    def check_entry(self, last, price, atr_stop, adx_limit, ts):
        ema20 = last['EMA_20']; ema50 = last['EMA_50']; adx = last['ADX']; adx_slope = last['ADX_Slope']
        vol = last['volume']; vol_ma = last['Vol_MA20']; rsi = last['RSI']
        is_bull = (ema20 > ema50) and (price > ema20)
        is_bear = (ema20 < ema50) and (price < ema20)
        
        if (adx > adx_limit) or (adx_slope > 1.5):
            if vol > (vol_ma * 1.2):
                if is_bull and rsi > 50: self.open_position(1, price, last['ATR'], atr_stop, ts)
                elif is_bear and rsi < 50: self.open_position(-1, price, last['ATR'], atr_stop, ts)

    def open_position(self, direction, price, atr, atr_stop, ts):
        real_entry_price = price * (1 + SLIPPAGE) if direction == 1 else price * (1 - SLIPPAGE)
        self.position = direction
        self.entry_price = real_entry_price
        self.sl_price = real_entry_price - (atr * atr_stop) if direction == 1 else real_entry_price + (atr * atr_stop)
        self.current_size = (self.balance * 0.98) / real_entry_price 
        self.is_break_even = False
        self.partial_tp_done = False 
        
        # 记录开仓日志
        self.trade_log.append({
            'timestamp': str(ts),
            'action': 'OPEN',
            'direction': direction,
            'price': real_entry_price,
            'size': self.current_size,
            'pnl': 0.0,
            'fee': 0.0,
            'note': 'Normal Entry'
        })

    def manage_position(self, row, price, atr_stop, ts):
        atr = row['ATR']
        
        # 1. 止损触发判定
        is_sl_hit = (self.position == 1 and row['low'] < self.sl_price) or \
                    (self.position == -1 and row['high'] > self.sl_price)

        if is_sl_hit:
            #  真实故障注入：在最危险的时刻 API 卡死 
            if random.random() < FAULT_PROB:
                if not self.is_hardened:
                    # 传统模型：止损失败，暴跌 8% 后才被平仓
                    penalty_price = self.sl_price * (1 - PENALTY_DROP) if self.position == 1 else self.sl_price * (1 + PENALTY_DROP)
                    self.close_position(penalty_price, ts, is_partial=False, note="FATAL: Fault Drop")
                    self.fatal_errors += 1
                    return
                else:
                    # 加固模型：Watchdog 介入，按原价成功止损
                    self.watchdog_interventions += 1
                    self.close_position(self.sl_price, ts, is_partial=False, note="WATCHDOG: Saved SL")
                    return
            else:
                self.close_position(self.sl_price, ts, is_partial=False, note="Normal SL")
                return

        # 2. 动态分批止盈
        if not self.partial_tp_done:
            profit = (price - self.entry_price) if self.position == 1 else (self.entry_price - price)
            if profit > (atr * 2.0):
                self.close_position(price, ts, is_partial=True, note="Partial TP")
                self.sl_price = self.entry_price
                self.is_break_even = True
                return 

    def close_position(self, price, ts, is_partial=False, note=""):
        size_to_close = self.current_size * 0.5 if is_partial else self.current_size
        real_close_price = price * (1 - SLIPPAGE) if self.position == 1 else price * (1 + SLIPPAGE)
        pnl = (real_close_price - self.entry_price) * size_to_close if self.position == 1 else (self.entry_price - real_close_price) * size_to_close
        fee = real_close_price * size_to_close * TRADING_FEE
        
        self.balance += (pnl - fee)
        
        # 记录平仓日志
        self.trade_log.append({
            'timestamp': str(ts),
            'action': 'CLOSE_PARTIAL' if is_partial else 'CLOSE_ALL',
            'direction': self.position,
            'price': real_close_price,
            'size': size_to_close,
            'pnl': pnl,
            'fee': fee,
            'note': note
        })

        if is_partial:
            self.current_size -= size_to_close
            self.partial_tp_done = True
        else:
            self.position = 0
            self.current_size = 0


# --- 数据库初始化函数 ---
def init_db(db_name="backtest_results.db"):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    # 建立模拟记录总表
    c.execute('''
        CREATE TABLE IF NOT EXISTS sim_records (
            run_id TEXT PRIMARY KEY,
            batch_id TEXT,
            model_name TEXT,
            run_index INTEGER,
            final_balance REAL,
            max_drawdown REAL,
            is_bankrupt BOOLEAN,
            watchdog_saves INTEGER,
            fatal_errors INTEGER
        )
    ''')
    # 建立逐笔交易明细表
    c.execute('''
        CREATE TABLE IF NOT EXISTS trade_logs (
            trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            timestamp DATETIME,
            action TEXT,
            direction INTEGER,
            price REAL,
            size REAL,
            pnl REAL,
            fee REAL,
            note TEXT,
            FOREIGN KEY(run_id) REFERENCES sim_records(run_id)
        )
    ''')
    conn.commit()
    return conn

# --- 主程序执行：蒙特卡洛模拟 ---
if __name__ == "__main__":
    print(f" 学术故障注入回测引擎 (蒙特卡洛 {SIMULATION_RUNS} 次)")
    print(f" 严格控制变量: ATR={TEST_ATR}, 滑点={SLIPPAGE*100}%, 止损故障率={FAULT_PROB*100}%")
    print("=" * 85)

    symbol = 'ETH_USDT'
    df = Backtester().load_data(symbol)
    
    if df is not None:
        # 1. 连接数据库
        db_conn = init_db()
        db_cursor = db_conn.cursor()
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S") # 本次回测批次号
        
        for model_name, is_hardened in [("传统基准模型 (无监控)", False), ("加固模型 (SQLite+Watchdog)", True)]:
            print(f"\n⏳ 正在运行 {model_name}...")
            
            final_balances = []
            max_drawdowns = []
            bankruptcies = 0
            total_watchdog_saves = 0
            
            for run in range(SIMULATION_RUNS):
                bt = Backtester(is_hardened=is_hardened)
                bt.run_strategy(df, atr_stop=TEST_ATR, adx_limit=TEST_ADX)
                
                # 为单次蒙特卡洛路径生成唯一 ID
                run_id = str(uuid.uuid4())
                
                final_balances.append(bt.balance)
                max_drawdowns.append(bt.max_drawdown)
                if bt.is_bankrupt: bankruptcies += 1
                total_watchdog_saves += bt.watchdog_interventions
                
                # 2. 写入单次跑分的汇总数据到 SQLite
                db_cursor.execute('''
                    INSERT INTO sim_records 
                    (run_id, batch_id, model_name, run_index, final_balance, max_drawdown, is_bankrupt, watchdog_saves, fatal_errors)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (run_id, batch_id, model_name, run, bt.balance, bt.max_drawdown, bt.is_bankrupt, bt.watchdog_interventions, bt.fatal_errors))
                
                # 3. 批量写入该次跑分的逐笔交易日志
                if bt.trade_log:
                    trades_data = [
                        (run_id, t['timestamp'], t['action'], t['direction'], t['price'], t['size'], t['pnl'], t['fee'], t['note'])
                        for t in bt.trade_log
                    ]
                    db_cursor.executemany('''
                        INSERT INTO trade_logs 
                        (run_id, timestamp, action, direction, price, size, pnl, fee, note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', trades_data)
                    
            # 提交数据库事务（每个模型跑完提交一次以提升性能）
            db_conn.commit()
            
            avg_balance = sum(final_balances) / SIMULATION_RUNS
            avg_dd = sum(max_drawdowns) / SIMULATION_RUNS
            bankruptcy_prob = (bankruptcies / SIMULATION_RUNS) * 100
            
            print(f"✅ 结果统计:")
            print(f"  - 平均最终资金: {avg_balance:.2f} USDT")
            print(f"  - 平均最大回撤: {avg_dd*100:.2f}%")
            print(f"  - 破产概率(Ruin): {bankruptcy_prob:.1f}%")
            if is_hardened:
                print(f"  - Watchdog 成功拦截异常: 共计 {total_watchdog_saves} 次 (恢复率100%)")
                
        # 跑完全部模型后关闭数据库连接
        db_conn.close()
        print(f"\n💾 所有蒙特卡洛模拟数据及逐笔明细已成功保存至 backtest_results.db (批次号: {batch_id})")
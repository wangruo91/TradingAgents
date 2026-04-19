from typing import Annotated
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import sys

# 尝试导入国内数据源库
try:
    import tushare as ts
except ImportError:
    ts = None

try:
    import akshare as ak
except ImportError:
    ak = None

try:
    import baostock as bs
except ImportError:
    bs = None

from .stockstats_utils import _clean_dataframe, yf_retry, load_ohlcv, filter_financials_by_date

def get_china_stock_data(
    symbol: Annotated[str, "ticker symbol of the company"],
    start_date: Annotated[str, "Start date in yyyy-mm-dd format"],
    end_date: Annotated[str, "End date in yyyy-mm-dd format"],
):
    """从国内数据源获取股票数据"""
    tried_sources = []
    
    try:
        # 检查日期是否是未来日期
        today = datetime.now().strftime("%Y-%m-%d")
        if end_date > today:
            end_date = today
        
        # 尝试使用 BaoStock（不需要 API Key，在国内最稳定）
        if bs is not None:
            try:
                result = _get_baostock_data(symbol, start_date, end_date)
                if not result.startswith("Error:") and not result.startswith("No data found"):
                    return result
                tried_sources.append(f"BaoStock: {result[:50]}")
            except Exception as e:
                tried_sources.append(f"BaoStock: {str(e)[:50]}")
        
        # 尝试使用 AkShare（增加重试机制）
        if ak is not None:
            for attempt in range(3):  # 最多重试3次
                try:
                    result = _get_akshare_data(symbol, start_date, end_date)
                    if not result.startswith("Error:") and not result.startswith("No data found"):
                        return result
                    tried_sources.append(f"AkShare(尝试{attempt+1}): {result[:50]}")
                    if attempt < 2:
                        import time
                        time.sleep(2)  # 重试前等待2秒
                except Exception as e:
                    tried_sources.append(f"AkShare(尝试{attempt+1}): {str(e)[:50]}")
                    if attempt < 2:
                        import time
                        time.sleep(2)
        
        # 尝试使用 Tushare（作为最后备选，需要积分）
        if ts is not None:
            result = _get_tushare_data(symbol, start_date, end_date)
            if not result.startswith("Error:") and not result.startswith("No data found"):
                return result
            tried_sources.append(f"Tushare: {result[:50]}")
        
        return f"Error: All Chinese data sources failed. Tried: {tried_sources}"
    except Exception as e:
        return f"Error retrieving China stock data: {str(e)}"

def _get_akshare_data(symbol, start_date, end_date):
    """使用 AkShare 获取股票数据"""
    try:
        # 处理股票代码格式
        if symbol.endswith('.SH'):
            symbol = symbol.replace('.SH', '')
        elif symbol.endswith('.SZ'):
            symbol = symbol.replace('.SZ', '')
        
        # 获取股票历史数据
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        
        if df.empty:
            return f"No data found for symbol '{symbol}' between {start_date} and {end_date}"
        
        # 重命名列以匹配标准格式
        df = df.rename(columns={
            '日期': 'Date',
            '开盘': 'Open',
            '最高': 'High',
            '最低': 'Low',
            '收盘': 'Close',
            '成交量': 'Volume',
            '成交额': 'Amount'
        })
        
        # 转换日期格式
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
        
        # 排序
        df = df.sort_index()
        
        # 转换为 CSV 字符串
        csv_string = df.to_csv()
        
        # 添加头部信息
        header = f"# Stock data for {symbol} from {start_date} to {end_date}\n"
        header += f"# Total records: {len(df)}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        return header + csv_string
    except Exception as e:
        return f"Error with AkShare: {str(e)}"

def _get_baostock_data(symbol, start_date, end_date):
    """使用 BaoStock 获取股票数据"""
    try:
        # 处理股票代码格式（ BaoStock 格式：sh.600519）
        code = symbol
        if symbol.endswith('.SH'):
            code = "sh." + symbol.replace('.SH', '')
        elif symbol.endswith('.SZ'):
            code = "sz." + symbol.replace('.SZ', '')
        
        # 登录 BaoStock
        lg = bs.login()
        if lg.error_code != '0':
            return f"BaoStock login failed: {lg.error_msg}"
        
        # 获取股票历史数据
        rs = bs.query_history_k_data_plus(
            code=code,
            fields="date,open,high,low,close,volume",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3"
        )
        
        # 转换为 DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 登出
        bs.logout()
        
        if df.empty:
            return f"No data found for symbol '{symbol}' between {start_date} and {end_date}"
        
        # 重命名列
        df = df.rename(columns={
            'date': 'Date',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'volume': 'Volume'
        })
        
        # 转换数据类型
        df['Date'] = pd.to_datetime(df['Date'])
        df['Open'] = pd.to_numeric(df['Open'])
        df['High'] = pd.to_numeric(df['High'])
        df['Low'] = pd.to_numeric(df['Low'])
        df['Close'] = pd.to_numeric(df['Close'])
        df['Volume'] = pd.to_numeric(df['Volume'])
        
        df = df.set_index('Date')
        df = df.sort_index()
        
        # 转换为 CSV 字符串
        csv_string = df.to_csv()
        
        # 添加头部信息
        header = f"# Stock data for {symbol} from {start_date} to {end_date}\n"
        header += f"# Total records: {len(df)}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        return header + csv_string
    except Exception as e:
        return f"Error with BaoStock: {str(e)}"

def _get_baostock_fundamentals(ticker, curr_date):
    """使用 BaoStock 获取基本面数据"""
    try:
        # 处理股票代码格式
        code = ticker
        if ticker.endswith('.SH'):
            code = "sh." + ticker.replace('.SH', '')
        elif ticker.endswith('.SZ'):
            code = "sz." + ticker.replace('.SZ', '')
        
        # 登录 BaoStock
        lg = bs.login()
        if lg.error_code != '0':
            return f"BaoStock login failed: {lg.error_msg}"
        
        # 获取股票基本信息
        rs = bs.query_stock_basic(code=code)
        
        # 转换为 DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        # 登出
        bs.logout()
        
        if not data_list:
            return f"No fundamentals data found for symbol '{ticker}'"
        
        # 构建结果
        lines = []
        for data in data_list:
            lines.append(f"{data[0]}: {data[1]}")
        
        header = f"# Company Fundamentals for {ticker}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        return header + "\n".join(lines)
    except Exception as e:
        return f"Error with BaoStock fundamentals: {str(e)}"

def _get_tushare_data(symbol, start_date, end_date):
    """使用 Tushare 获取股票数据"""
    try:
        # 检查 Tushare API Key
        ts_token = os.getenv('TUSHARE_TOKEN')
        if not ts_token:
            return "Error: TUSHARE_TOKEN environment variable not set"
        
        ts.set_token(ts_token)
        pro = ts.pro_api()
        
        # 处理股票代码格式
        if symbol.endswith('.SH'):
            ts_code = symbol
        elif symbol.endswith('.SZ'):
            ts_code = symbol
        else:
            # 尝试自动判断
            if symbol.startswith('6'):
                ts_code = symbol + '.SH'
            else:
                ts_code = symbol + '.SZ'
        
        # 获取股票历史数据
        df = pro.daily(
            ts_code=ts_code,
            start_date=start_date.replace('-', ''),
            end_date=end_date.replace('-', '')
        )
        
        if df.empty:
            return f"No data found for symbol '{symbol}' between {start_date} and {end_date}"
        
        # 检查必要的列是否存在
        required_columns = ['trade_date', 'open', 'high', 'low', 'close', 'vol']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return f"Error: Missing required columns: {missing_columns}"
        
        # 重命名列
        df = df.rename(columns={
            'trade_date': 'Date',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
            'vol': 'Volume'
        })
        
        # 转换日期格式
        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
        df = df.set_index('Date')
        
        # 排序
        df = df.sort_index()
        
        # 转换为 CSV 字符串
        csv_string = df.to_csv()
        
        # 添加头部信息
        header = f"# Stock data for {symbol} from {start_date} to {end_date}\n"
        header += f"# Total records: {len(df)}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        return header + csv_string
    except Exception as e:
        return f"Error with Tushare: {str(e)}"

def get_china_fundamentals(
    ticker: Annotated[str, "ticker symbol of the company"],
    curr_date: Annotated[str, "current date"] = None
):
    """获取中国股票基本面数据"""
    tried_sources = []

    # 首先尝试使用 BaoStock（最稳定）
    if bs is not None:
        try:
            result = _get_baostock_fundamentals(ticker, curr_date)
            if not result.startswith("Error:") and not result.startswith("No fundamentals"):
                return result
            tried_sources.append(f"BaoStock: {result[:50]}")
        except Exception as e:
            tried_sources.append(f"BaoStock: {str(e)[:50]}")

    # 尝试使用 AkShare（增加重试机制）
    if ak is not None:
        import time
        for attempt in range(5):
            try:
                result = _get_akshare_fundamentals(ticker, curr_date)
                if not result.startswith("Error:") and not result.startswith("No fundamentals"):
                    return result
                tried_sources.append(f"AkShare(尝试{attempt+1}): {result[:50]}")
                if attempt < 4:
                    time.sleep(3)
            except Exception as e:
                tried_sources.append(f"AkShare(尝试{attempt+1}): {str(e)[:50]}")
                if attempt < 4:
                    time.sleep(3)

    # 尝试使用 Tushare（需要积分）
    if ts is not None:
        result = _get_tushare_fundamentals(ticker, curr_date)
        if not result.startswith("Error:") and not result.startswith("No fundamentals"):
            return result
        tried_sources.append(f"Tushare: {result[:50]}")

    return f"Error: No Chinese data sources available for fundamentals. Tried: {tried_sources}"

def _get_akshare_fundamentals(ticker, curr_date):
    """使用 AkShare 获取基本面数据"""
    try:
        # 处理股票代码格式
        if ticker.endswith('.SH'):
            symbol = ticker.replace('.SH', '')
        elif ticker.endswith('.SZ'):
            symbol = ticker.replace('.SZ', '')
        else:
            symbol = ticker
        
        # 获取股票基本信息
        stock_info = ak.stock_individual_info_em(symbol=symbol)
        
        if stock_info.empty:
            return f"No fundamentals data found for symbol '{ticker}'"
        
        # 构建结果
        lines = []
        for _, row in stock_info.iterrows():
            lines.append(f"{row['item']}: {row['value']}")
        
        header = f"# Company Fundamentals for {ticker}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        return header + "\n".join(lines)
    except Exception as e:
        return f"Error with AkShare fundamentals: {str(e)}"

def _get_tushare_fundamentals(ticker, curr_date):
    """使用 Tushare 获取基本面数据"""
    try:
        # 检查 Tushare API Key
        ts_token = os.getenv('TUSHARE_TOKEN')
        if not ts_token:
            return "Error: TUSHARE_TOKEN environment variable not set"
        
        ts.set_token(ts_token)
        pro = ts.pro_api()
        
        # 处理股票代码格式
        if not ticker.endswith('.SH') and not ticker.endswith('.SZ'):
            if ticker.startswith('6'):
                ts_code = ticker + '.SH'
            else:
                ts_code = ticker + '.SZ'
        else:
            ts_code = ticker
        
        # 获取股票基本信息
        df = pro.stock_basic(ts_code=ts_code)
        
        if df.empty:
            return f"No fundamentals data found for symbol '{ticker}'"
        
        # 构建结果
        lines = []
        stock_data = df.iloc[0]
        lines.append(f"Name: {stock_data.get('name', 'N/A')}")
        lines.append(f"Industry: {stock_data.get('industry', 'N/A')}")
        lines.append(f"Market: {stock_data.get('market', 'N/A')}")
        
        # 获取财务数据
        try:
            fin_data = pro.fina_indicator(ts_code=ts_code, period='20231231', limit=1)
            if not fin_data.empty:
                fin = fin_data.iloc[0]
                lines.append(f"PE Ratio: {fin.get('pe', 'N/A')}")
                lines.append(f"PB Ratio: {fin.get('pb', 'N/A')}")
                lines.append(f"ROE: {fin.get('roe', 'N/A')}")
        except:
            pass
        
        header = f"# Company Fundamentals for {ticker}\n"
        header += f"# Data retrieved on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        return header + "\n".join(lines)
    except Exception as e:
        return f"Error with Tushare fundamentals: {str(e)}"

def get_china_indicators(
    symbol: Annotated[str, "ticker symbol of the company"],
    indicator: Annotated[str, "technical indicator to get the analysis and report of"],
    curr_date: Annotated[str, "The current trading date you are trading on, YYYY-mm-dd"],
    look_back_days: Annotated[int, "how many days to look back"],
) -> str:
    """获取中国股票技术指标"""
    try:
        # 先获取股票数据
        end_date = curr_date
        curr_date_dt = datetime.strptime(curr_date, "%Y-%m-%d")
        start_date = (curr_date_dt - relativedelta(days=look_back_days)).strftime("%Y-%m-%d")
        
        # 检查日期是否是未来日期
        today = datetime.now().strftime("%Y-%m-%d")
        if end_date > today:
            end_date = today
        
        # 获取股票数据
        stock_data = get_china_stock_data(symbol, start_date, end_date)
        
        # 解析 CSV 数据
        lines = stock_data.split('\n')
        csv_lines = [line for line in lines if not line.startswith('#') and line.strip()]
        if not csv_lines:
            return f"No data available for {symbol}"
        
        # 构建 DataFrame
        df = pd.read_csv(pd.io.common.StringIO('\n'.join(csv_lines)))
        if 'Date' not in df.columns:
            return f"Error: 'Date' column not found in data for {symbol}"
        
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date')
        
        # 计算技术指标
        result_str = f"## {indicator} values from {start_date} to {end_date}:\n\n"

        # 简单移动平均线
        if indicator == 'close_50_sma':
            df['SMA50'] = df['Close'].rolling(window=50).mean()
            for date, row in df.iterrows():
                if not pd.isna(row['SMA50']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['SMA50']:.2f}\n"
        elif indicator == 'close_200_sma':
            df['SMA200'] = df['Close'].rolling(window=200).mean()
            for date, row in df.iterrows():
                if not pd.isna(row['SMA200']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['SMA200']:.2f}\n"
        # RSI 指标
        elif indicator == 'rsi':
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            df['RSI'] = rsi
            for date, row in df.iterrows():
                if not pd.isna(row['RSI']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['RSI']:.2f}\n"
        # MACD 指标
        elif indicator in ['macd', 'macds', 'macdh']:
            exp12 = df['Close'].ewm(span=12, adjust=False).mean()
            exp26 = df['Close'].ewm(span=26, adjust=False).mean()
            macd = exp12 - exp26
            signal = macd.ewm(span=9, adjust=False).mean()
            hist = macd - signal
            df['MACD'] = macd
            df['MACD_Signal'] = signal
            df['MACD_Hist'] = hist
            for date, row in df.iterrows():
                if indicator == 'macd' and not pd.isna(row['MACD']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['MACD']:.2f}\n"
                elif indicator == 'macds' and not pd.isna(row['MACD_Signal']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['MACD_Signal']:.2f}\n"
                elif indicator == 'macdh' and not pd.isna(row['MACD_Hist']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['MACD_Hist']:.2f}\n"
        # ATR 指标
        elif indicator == 'atr':
            high_low = df['High'] - df['Low']
            high_close = (df['High'] - df['Close'].shift()).abs()
            low_close = (df['Low'] - df['Close'].shift()).abs()
            true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr = true_range.rolling(window=14).mean()
            df['ATR'] = atr
            for date, row in df.iterrows():
                if not pd.isna(row['ATR']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['ATR']:.2f}\n"
        # 布林带指标
        elif indicator in ['boll', 'boll_ub', 'boll_lb']:
            middle_band = df['Close'].rolling(window=20).mean()
            std_dev = df['Close'].rolling(window=20).std()
            upper_band = middle_band + (std_dev * 2)
            lower_band = middle_band - (std_dev * 2)
            df['Bollinger_Middle'] = middle_band
            df['Bollinger_Upper'] = upper_band
            df['Bollinger_Lower'] = lower_band
            for date, row in df.iterrows():
                if indicator == 'boll' and not pd.isna(row['Bollinger_Middle']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['Bollinger_Middle']:.2f}\n"
                elif indicator == 'boll_ub' and not pd.isna(row['Bollinger_Upper']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['Bollinger_Upper']:.2f}\n"
                elif indicator == 'boll_lb' and not pd.isna(row['Bollinger_Lower']):
                    result_str += f"{date.strftime('%Y-%m-%d')}: {row['Bollinger_Lower']:.2f}\n"
        # EMA 指标
        elif indicator.startswith('close_') and indicator.endswith('_ema'):
            try:
                window = int(indicator.replace('close_', '').replace('_ema', ''))
                df[f'EMA{window}'] = df['Close'].ewm(span=window, adjust=False).mean()
                for date, row in df.iterrows():
                    if not pd.isna(row[f'EMA{window}']):
                        result_str += f"{date.strftime('%Y-%m-%d')}: {row[f'EMA{window}']:.2f}\n"
            except ValueError:
                result_str += f"Indicator {indicator} not supported for China stocks"
        else:
            result_str += f"Indicator {indicator} not supported for China stocks"

        return result_str
    except Exception as e:
        return f"Error calculating China indicators: {str(e)}"

def get_china_balance_sheet(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency of data: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None
):
    """获取中国股票资产负债表"""
    return f"Balance sheet data not yet implemented for China stocks"

def get_china_cashflow(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency of data: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None
):
    """获取中国股票现金流量表"""
    return f"Cash flow data not yet implemented for China stocks"

def get_china_income_statement(
    ticker: Annotated[str, "ticker symbol of the company"],
    freq: Annotated[str, "frequency of data: 'annual' or 'quarterly'"] = "quarterly",
    curr_date: Annotated[str, "current date in YYYY-MM-DD format"] = None
):
    """获取中国股票利润表"""
    return f"Income statement data not yet implemented for China stocks"

def get_china_news(
    ticker: Annotated[str, "ticker symbol of the company"]
):
    """获取中国股票新闻"""
    return f"News data not yet implemented for China stocks"

def get_china_global_news():
    """获取中国市场全球新闻"""
    return f"Global news data not yet implemented for China market"

def get_china_insider_transactions(
    ticker: Annotated[str, "ticker symbol of the company"]
):
    """获取中国股票内幕交易"""
    return f"Insider transactions data not yet implemented for China stocks"

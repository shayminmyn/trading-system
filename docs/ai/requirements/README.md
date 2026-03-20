# Trading System — Requirements

**Date**: 20 Mar 2026  
**Status**: Draft  
**Author**: Min

---

## 1. Tổng quan Hệ thống (System Overview)

Xây dựng một hệ thống trading tự động (Automated Trading Signal System) hỗ trợ Forex và Vàng (XAUUSD), có khả năng:

- Lấy dữ liệu thị trường realtime và lịch sử.
- Phân tích kỹ thuật và sinh tín hiệu Mua/Bán.
- Tính toán khối lượng giao dịch (Lot size), Stop Loss, Take Profit theo quản lý vốn.
- Gửi tín hiệu giao dịch đã định dạng về Telegram.
- Hỗ trợ backtesting chiến lược trên dữ liệu lịch sử.

---

## 2. Các Module Chính (Core Modules)

| Module | Trách nhiệm |
|---|---|
| **Data Module** | Lấy dữ liệu realtime (tick/OHLCV) và tải dữ liệu lịch sử |
| **Strategy Module** | Phân tích kỹ thuật, sinh tín hiệu BUY/SELL/NONE |
| **Risk Management Module** | Tính toán Lot size, SL, TP theo % rủi ro tài khoản |
| **Execution/Notifier Module** | Định dạng và gửi tín hiệu về Telegram |
| **Backtest Engine** | Chạy thử chiến lược trên dữ liệu lịch sử, xuất báo cáo |

---

## 3. Yêu cầu Chi tiết

### 3.1 Data Module

#### Nguồn dữ liệu Realtime
- **Giải pháp chính**: Thư viện `MetaTrader5` (Python) kết nối trực tiếp vào MT5 Terminal.
  - Đăng nhập tài khoản Demo của sàn (Exness, IC Markets, v.v.)
  - Lấy dữ liệu tick và OHLCV theo khung thời gian bất kỳ
  - Độ chính xác 100%, miễn phí, không delay
- **Giải pháp thay thế**: TwelveData API (khi không có MT5)

#### Nguồn dữ liệu Lịch sử (≥ 2 năm)
- **HistData.com**: Dữ liệu Forex/XAUUSD miễn phí, khung M1 trở lên, dạng CSV
- **Dukascopy**: Dùng thư viện `dukascopy-node` hoặc `tickstory`, chất lượng cao, miễn phí

#### Cặp giao dịch được hỗ trợ
- XAUUSD (Vàng)
- EURUSD
- Mở rộng được qua `config.yaml`

#### Khung thời gian (Timeframes)
- M5, M15, H1, H4, D1

---

### 3.2 Strategy Module

#### Thiết kế OOP
Tất cả các strategy kế thừa từ `BaseStrategy`:

```python
class BaseStrategy:
    def __init__(self, symbol, timeframe, parameters): ...
    def update_data(self, new_data): ...
    def calculate_indicators(self): ...   # Abstract
    def generate_signal(self): ...        # Abstract — trả về Signal object
```

#### Signal Object
Mỗi `generate_signal()` trả về một dict/object chứa:
```python
{
    "action": "BUY" | "SELL" | "NONE",
    "entry": float,
    "sl_pips": float,   # Khoảng cách SL tính bằng Pips
    "strategy_name": str
}
```

#### Các Strategy Cụ thể

| Strategy | Class | Nguyên lý |
|---|---|---|
| MACD Crossover | `MACDCrossoverStrategy` | Giao cắt histogram MACD + xác nhận hướng |
| RSI + EMA | `RSI_EMA_Strategy` | EMA cross + RSI extreme recovery |
| **SonicR (Value Zone)** | `SonicRStrategy` | Mean reversion về cụm EMA 34/89 theo xu hướng chính |

---

### 3.2b SonicR Strategy — Yêu cầu Chi tiết

**Nguyên lý**: Mean Reversion kết hợp Trend Continuation.
Bắt các con sóng tiếp diễn xu hướng sau khi giá hội tụ về cụm EMA 34/89.

#### Chỉ báo
- **EMA 34** — cụm nến ngắn hạn
- **EMA 89** — cụm nến dài hạn, xác định xu hướng chính
- **ATR 14** — đo độ biến động, xác định "vùng quá xa" và tính SL

#### Logic Vào Lệnh BUY

| Bước | Điều kiện |
|---|---|
| 1. Xu hướng | EMA 34 > EMA 89, cả hai dốc lên (slope dương trong N bar gần nhất) |
| 2. Không sideway | EMA 34 − EMA 89 ≥ `min_ema_separation_atr × ATR` |
| 3. Far value zone | Trong cửa sổ lookback, có ít nhất 1 nến `High > EMA34 + atr_mult_far × ATR` |
| 4. Pullback về EMA | Sau khi extension, có nến `Low ≤ EMA34 + 0.5×ATR` (giá "đục qua") |
| 5. Không đảo chiều | Trong pullback, không có nến đóng cửa dưới `EMA89 − 0.5×ATR` |
| 6. Không new lower-low | Đáy vùng pullback ≥ swing low trước extension (thị trường vẫn tăng) |
| 7. Entry trigger | Nến hiện tại đóng cửa **trên EMA 34**, nến trước đóng cửa ≤ EMA 34 |
| 8. RR filter | `(swing_high − entry) / (entry − SL) ≥ min_rr` (mặc định 1.0) |

#### Logic Vào Lệnh SELL — Mirror của BUY

| Bước | Điều kiện |
|---|---|
| 1. Xu hướng | EMA 34 < EMA 89, cả hai dốc xuống |
| 3. Far value zone | `Low < EMA34 − atr_mult_far × ATR` |
| 4. Correction về EMA | `High ≥ EMA34 − 0.5×ATR` (giá "đục qua lên") |
| 5. Không đảo chiều | Không có nến đóng cửa trên `EMA89 + 0.5×ATR` |
| 6. Không new higher-high | Đỉnh vùng correction ≤ swing high trước extension |
| 7. Entry trigger | Nến hiện tại đóng cửa **dưới EMA 34**, nến trước ≥ EMA 34 |

#### Stop Loss
- **BUY**: `min(pullback_low, EMA89) − sl_buffer_atr × ATR`
- **SELL**: `max(correction_high, EMA89) + sl_buffer_atr × ATR`

#### Take Profit
- **TP1** (RR 1:1.5): Đỉnh/đáy cũ gần nhất — chốt một phần để bảo vệ vốn
- **TP2** (RR 1:3+): Gồng lệnh theo sóng 5 Elliott; dời SL về breakeven sau TP1;
  đóng khi `EMA34 cắt EMA89` hoặc đạt target

#### Bộ Lọc (Filters)

| Bộ lọc | Mô tả |
|---|---|
| **Sideway filter** | Loại tín hiệu khi EMAs đang thu hẹp khoảng cách |
| **Slope filter** | Cả EMA34 và EMA89 phải cùng chiều trong N bar gần nhất |
| **RR filter** | Bỏ qua tín hiệu nếu RR < `min_rr` (mặc định 1.0) |

#### Tham số Cấu hình (config.yaml)
```yaml
strategies:
  SonicR:
    ema_fast: 34                # EMA ngắn (cụm nến ngắn hạn)
    ema_slow: 89                # EMA dài (xác định xu hướng chính)
    atr_period: 14
    atr_mult_far: 2.0           # Hệ số ATR xác định "far value zone"
    sl_buffer_atr: 0.3          # Buffer ATR cộng thêm vào SL
    min_ema_separation_atr: 0.5 # Khoảng tối thiểu EMAs / ATR (lọc sideway)
    slope_lookback: 5           # Số nến đo slope EMA
    pullback_lookback: 30       # Cửa sổ tìm pullback
    extension_lookback: 20      # Cửa sổ tìm extension trước pullback
    min_rr: 1.0                 # RR tối thiểu để chấp nhận lệnh
```

#### Ưu điểm & Nhược điểm

| | Mô tả |
|---|---|
| **Ưu** | Bắt sóng tiếp diễn lớn; lợi nhuận cao khi sóng 5 Elliott chạy mạnh |
| **Nhược** | Thị trường sideway tạo tín hiệu giả → đã có bộ lọc EMA separation |
| **Nhược** | Gồng lệnh TP2 cần tâm lý vững → hỗ trợ bằng quy tắc breakeven |

#### Thư viện Indicator
- `pandas-ta` hoặc `TA-Lib` để tính toán chỉ báo kỹ thuật

---

### 3.3 Risk Management Module

#### Cấu hình (config.yaml)
```yaml
risk_management:
  account_balance: 10000        # USD
  risk_per_trade_percent: 1.5   # % tài khoản rủi ro mỗi lệnh
  default_rr_ratio: 2           # Reward:Risk = 1:2
```

#### Công thức tính Lot Size
```
Tiền rủi ro (USD) = account_balance × risk_per_trade_percent / 100
Lot Size = Tiền rủi ro / (SL_pips × pip_value_per_lot)
```

Giá trị Pip Value theo loại tài sản:
- **Forex** (EURUSD): 1 lot standard = $10/pip
- **Vàng** (XAUUSD): 1 lot standard = $10/pip (1 pip = $0.01 di chuyển giá)

#### Output: Signal Object Hoàn chỉnh
```python
{
    "symbol": "XAUUSD",
    "timeframe": "H1",
    "action": "BUY LIMIT",
    "entry": 2150.50,
    "sl": 2145.00,
    "sl_pips": 55,
    "tp1": 2161.50,
    "tp2": 2172.00,
    "volume": 0.27,
    "risk_percent": 1.5,
    "strategy_name": "MACD_RSI_V1"
}
```

---

### 3.4 Execution / Notifier Module

#### Telegram Bot
- Thư viện: `python-telegram-bot`
- Gửi tin nhắn định dạng Markdown khi có tín hiệu mới

#### Mẫu tin nhắn Telegram
```
🚨 TÍN HIỆU GIAO DỊCH 🚨
🔹 Cặp: XAUUSD
⏱ Khung TG: H1
📈 Lệnh: BUY LIMIT
💰 Entry: 2150.50
🛑 Stoploss: 2145.00 (55 pips)
✅ Take Profit 1: 2161.50 (RR 1:2)
⚖️ Volume Khuyến nghị: 0.27 Lot (Risk: 1.5%)
🤖 Strategy: MACD_RSI_V1
```

---

### 3.5 Backtest Engine

#### Thư viện
- **Ưu tiên**: `vectorbt` — tích hợp tốt với Pandas, biểu đồ trực quan
- **Thay thế**: `Backtrader`

#### Báo cáo Backtest cần có
| Chỉ số | Mô tả |
|---|---|
| Winrate | % lệnh thắng |
| Max Drawdown | Drawdown tối đa |
| Sharpe Ratio | Tỷ lệ lợi nhuận/rủi ro |
| Total Return | Tổng lợi nhuận |
| Total Trades | Tổng số lệnh |

---

## 4. Yêu cầu Cấu hình (config.yaml)

```yaml
trading_pairs:
  - symbol: "XAUUSD"
    timeframes: ["M15", "H1"]
  - symbol: "EURUSD"
    timeframes: ["M5"]

risk_management:
  account_balance: 10000
  risk_per_trade_percent: 1.5
  default_rr_ratio: 2

telegram:
  bot_token: "YOUR_BOT_TOKEN"
  chat_id: "YOUR_CHAT_ID"

mt5:
  login: 12345678
  password: "your_password"
  server: "Exness-MT5Real"
```

---

## 5. Yêu cầu Phi chức năng (Non-Functional Requirements)

| Yêu cầu | Mô tả |
|---|---|
| **Modular** | Các module độc lập, dễ thay thế hoặc mở rộng |
| **Extensible** | Thêm strategy mới chỉ cần tạo class mới kế thừa `BaseStrategy` |
| **Configurable** | Tất cả tham số quan trọng trong `config.yaml` |
| **Reliable** | Xử lý lỗi kết nối, tự reconnect khi mất kết nối MT5 |
| **24/7 Ready** | Deploy được lên VPS (AWS EC2 T2 Micro / DigitalOcean) |
| **Testable** | Strategy có thể backtest độc lập với data thật |

---

## 6. Ràng buộc Kỹ thuật (Technical Constraints)

- **Ngôn ngữ**: Python 3.10+
- **OS**: Windows (MT5 native) hoặc Linux qua Wine (VPS)
- **MT5**: Yêu cầu cài MetaTrader5 terminal và tài khoản Demo/Real
- **Chi phí**: Ưu tiên giải pháp miễn phí (MT5 Demo, HistData, Dukascopy)

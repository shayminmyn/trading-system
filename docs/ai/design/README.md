# Trading System — System Design & Architecture

**Date**: 20 Mar 2026  
**Status**: Draft  
**Version**: 1.0

---

## 1. Kiến trúc Tổng quan (High-Level Architecture)

Hệ thống theo kiến trúc **Micro-architecture** — các module độc lập, giao tiếp qua interface rõ ràng, dễ thay thế và mở rộng.

```mermaid
graph TB
    subgraph "Data Layer"
        MT5[MetaTrader5 API]
        CSV[Historical CSV\nHistData / Dukascopy]
        DM[Data Module\ndata_manager.py]
    end

    subgraph "Strategy Layer"
        BS[BaseStrategy\nbase_strategy.py]
        S1[MACDCrossoverStrategy]
        S2[RSI_EMA_Strategy]
        SN[... more strategies]
        BS --> S1
        BS --> S2
        BS --> SN
    end

    subgraph "Risk Layer"
        RM[Risk Manager\nrisk_manager.py]
        CFG[config.yaml]
        CFG --> RM
    end

    subgraph "Output Layer"
        TG[Telegram Notifier\ntelegram_notifier.py]
        LOG[Logger]
    end

    subgraph "Backtest Engine"
        BE[Backtest Runner\nbacktest_engine.py]
        RPT[Report Generator\nWinrate / Drawdown / Sharpe]
    end

    MT5 --> DM
    CSV --> DM
    DM --> BS
    BS --> RM
    RM --> TG
    RM --> LOG
    DM --> BE
    BS --> BE
    BE --> RPT
```

---

## 2. Luồng Dữ liệu Realtime (Realtime Data Flow)

```mermaid
sequenceDiagram
    participant MT5 as MT5 Terminal
    participant DM as Data Module
    participant ST as Strategy
    participant RM as Risk Manager
    participant TG as Telegram Bot

    loop Every new candle / tick
        MT5->>DM: OHLCV data (new bar closed)
        DM->>DM: Append to DataFrame, validate
        DM->>ST: update_data(new_candle)
        ST->>ST: calculate_indicators()
        ST->>ST: generate_signal()
        alt Signal = BUY or SELL
            ST->>RM: raw_signal {action, entry, sl_pips}
            RM->>RM: calculate_lot_size()
            RM->>RM: calculate_tp(rr_ratio)
            RM->>TG: complete_signal object
            TG->>TG: format_message()
            TG-->>User: 📱 Telegram notification
        else Signal = NONE
            ST->>ST: No action
        end
    end
```

---

## 3. Cấu trúc Thư mục Dự án (Project Structure)

```
trading-system/
├── config.yaml                  # Tất cả tham số cấu hình
├── main.py                      # Entry point — chạy realtime
├── backtest.py                  # Entry point — chạy backtest
│
├── src/
│   ├── data/
│   │   ├── __init__.py
│   │   ├── data_manager.py      # Kết nối MT5, tải OHLCV
│   │   └── historical_loader.py # Tải CSV từ HistData/Dukascopy
│   │
│   ├── strategies/
│   │   ├── __init__.py
│   │   ├── base_strategy.py     # Abstract BaseStrategy class
│   │   ├── macd_crossover.py    # MACDCrossoverStrategy
│   │   └── rsi_ema.py           # RSI + EMA Strategy
│   │
│   ├── risk/
│   │   ├── __init__.py
│   │   └── risk_manager.py      # Lot size, SL/TP calculator
│   │
│   ├── notifier/
│   │   ├── __init__.py
│   │   └── telegram_notifier.py # Gửi tín hiệu Telegram
│   │
│   └── backtest/
│       ├── __init__.py
│       ├── backtest_engine.py   # Chạy backtest với vectorbt
│       └── report_generator.py  # Xuất báo cáo
│
├── data/
│   └── historical/              # Lưu file CSV lịch sử
│
├── logs/
│   └── trading.log
│
├── docs/
│   └── ai/                      # AI-assisted dev docs
│
├── tests/
│   ├── test_risk_manager.py
│   ├── test_strategies.py
│   └── test_data_manager.py
│
└── requirements.txt
```

---

## 4. Thiết kế Chi tiết Từng Module

### 4.1 Data Module (`src/data/data_manager.py`)

```mermaid
classDiagram
    class DataManager {
        +symbol: str
        +timeframe: str
        +mt5_connected: bool
        +connect_mt5() bool
        +disconnect_mt5()
        +get_realtime_ohlcv(n_bars) DataFrame
        +get_tick() dict
        +stream_on_new_bar(callback)
    }

    class HistoricalLoader {
        +load_from_csv(filepath) DataFrame
        +load_from_dukascopy(symbol, start, end) DataFrame
        +resample_timeframe(df, tf) DataFrame
    }
```

**Quyết định thiết kế**: Sử dụng callback pattern (`stream_on_new_bar`) — mỗi khi nến mới đóng, gọi callback để tránh polling liên tục tiêu tốn CPU.

---

### 4.2 Strategy Module (`src/strategies/`)

```mermaid
classDiagram
    class BaseStrategy {
        <<abstract>>
        +symbol: str
        +timeframe: str
        +parameters: dict
        +data: DataFrame
        +update_data(new_data: DataFrame)
        +calculate_indicators()* 
        +generate_signal()* Signal
    }

    class MACDCrossoverStrategy {
        +fast_period: int
        +slow_period: int
        +signal_period: int
        +calculate_indicators()
        +generate_signal() Signal
    }

    class RSI_EMA_Strategy {
        +rsi_period: int
        +ema_period: int
        +rsi_overbought: int
        +rsi_oversold: int
        +calculate_indicators()
        +generate_signal() Signal
    }

    class Signal {
        +action: str
        +entry: float
        +sl_pips: float
        +strategy_name: str
        +timestamp: datetime
    }

    BaseStrategy <|-- MACDCrossoverStrategy
    BaseStrategy <|-- RSI_EMA_Strategy
    BaseStrategy ..> Signal : generates
```

---

### 4.3 Risk Management Module (`src/risk/risk_manager.py`)

```mermaid
classDiagram
    class RiskManager {
        +account_balance: float
        +risk_percent: float
        +rr_ratio: float
        +calculate_lot_size(symbol, sl_pips) float
        +calculate_tp(entry, sl, action, rr) float
        +get_pip_value(symbol) float
        +build_complete_signal(raw_signal) CompleteSignal
    }

    class CompleteSignal {
        +symbol: str
        +timeframe: str
        +action: str
        +entry: float
        +sl: float
        +sl_pips: float
        +tp1: float
        +tp2: float
        +volume: float
        +risk_percent: float
        +strategy_name: str
    }

    RiskManager ..> CompleteSignal : produces
```

**Công thức Pip Value**:
- FOREX (EURUSD, GBPUSD): `pip_value = 10 USD/pip/lot` (standard lot 100,000 units)
- XAUUSD (Vàng): `pip_value = 1 USD/pip/lot` (1 lot = 100 oz, 1 pip = $0.01)
  - Thực tế: `pip_value = contract_size × pip_size / quote_price × 1` — lấy từ MT5 symbol info

---

### 4.4 Telegram Notifier (`src/notifier/telegram_notifier.py`)

```mermaid
sequenceDiagram
    participant RM as Risk Manager
    participant TN as TelegramNotifier
    participant API as Telegram API

    RM->>TN: send_signal(complete_signal)
    TN->>TN: format_message(signal)
    TN->>API: POST /sendMessage (bot_token, chat_id, text)
    API-->>TN: 200 OK
    TN->>TN: log success
```

---

### 4.5 Backtest Engine (`src/backtest/`)

```mermaid
flowchart LR
    CSV[Historical CSV] --> HL[HistoricalLoader]
    HL --> DF[Pandas DataFrame]
    DF --> ST[Strategy.calculate_indicators]
    ST --> SIG[Signal Series]
    SIG --> VBT[vectorbt Portfolio]
    VBT --> RPT[Report\nWinrate / Drawdown\nSharpe / Return]
    VBT --> CHART[Interactive Chart]
```

---

## 5. Sơ đồ Trạng thái Hệ thống (System State Diagram)

```mermaid
stateDiagram-v2
    [*] --> Initializing
    Initializing --> Connecting : load config
    Connecting --> Running : MT5 connected + Telegram OK
    Connecting --> Error : connection failed
    Running --> Analyzing : new bar closed
    Analyzing --> SignalGenerated : BUY/SELL detected
    Analyzing --> Running : NONE signal
    SignalGenerated --> Notifying : risk calc complete
    Notifying --> Running : message sent
    Error --> Connecting : retry after 30s
    Running --> Stopped : manual stop / crash
    Stopped --> [*]
```

---

## 6. Quyết định Kiến trúc Quan trọng (Architecture Decision Records)

| # | Quyết định | Lý do |
|---|---|---|
| ADR-01 | Dùng MT5 Python library thay vì REST API | Không delay, miễn phí, data chính xác từ sàn |
| ADR-02 | Config tập trung trong `config.yaml` | Không cần chạm vào code khi thay đổi tham số |
| ADR-03 | OOP với BaseStrategy abstract class | Dễ mở rộng thêm strategy mới, tái sử dụng backtest |
| ADR-04 | `vectorbt` cho backtest | Hiệu năng cao (vectorized), tích hợp Pandas, biểu đồ đẹp |
| ADR-05 | Callback pattern cho realtime stream | Tránh polling CPU liên tục, event-driven sạch hơn |
| ADR-06 | Modular notifier | Dễ thêm kênh thông báo mới (Discord, Email) sau này |

# Trading System

Hệ thống giao dịch theo kiến trúc module: **dữ liệu** → **chiến lược** → **quản trị rủi ro** → **thông báo Telegram**. Hỗ trợ **backtest** trên dữ liệu lịch sử (CSV) và chạy **realtime** qua MetaTrader 5 (Windows) hoặc dữ liệu mock khi phát triển trên macOS/Linux.

---

## Tính năng chính

| Module | Mô tả |
|--------|--------|
| **Data** | MT5 (realtime), mock stream, đọc CSV lịch sử (`HistoricalLoader`) |
| **Strategy** | OOP: `BaseStrategy`, `MACDCrossover`, `RSI_EMA` (chỉ báo `ta`) |
| **Risk** | Tính lot, SL/TP, RR theo `config.yaml` |
| **Notifier** | Gửi tín hiệu đã format qua Telegram (`httpx`) |
| **Backtest** | Engine vector hóa + báo cáo HTML/JSON + biểu đồ Plotly |

Tài liệu chi tiết (yêu cầu, thiết kế, lộ trình): [`docs/ai/`](docs/ai/).

---

## Yêu cầu hệ thống

- **Python 3.12** (khuyến nghị — môi trường ổn định)
- **Conda** (Miniconda / Anaconda) — dùng `environment.yml`
- **MetaTrader 5** + thư viện `MetaTrader5`: chỉ trên **Windows** khi cần dữ liệu live thật
- Trên **macOS/Linux**: dùng `fallback_source: mock` hoặc CSV trong `data/historical/`

---

## Cài đặt nhanh

```bash
cd trading-system
chmod +x setup.sh
./setup.sh
```

Script sẽ:

1. Tạo/cập nhật conda env **`trading-system`** từ [`environment.yml`](environment.yml)
2. Sao chép `config.example.yaml` → `config.yaml` nếu chưa có
3. Tạo thư mục `logs/`, `data/historical/`, `backtest_results/`

Kích hoạt môi trường:

```bash
conda activate trading-system
```

---

## Cấu hình

### `config.yaml`

Sao chép từ `config.example.yaml` và chỉnh:

- **`trading_pairs`**: symbol, khung thời gian (`M5`, `M15`, `H1`, `H4`, …), danh sách strategy
- **`risk_management`**: số dư, % rủi ro mỗi lệnh, RR, min/max lot
- **`mt5`**: login, password, server (chỉ khi dùng MT5 trên Windows)
- **`telegram`**: `bot_token`, `chat_id` — có thể override bằng biến môi trường (xem `ConfigLoader`)
- **`data`**: `historical_dir`, `warmup_bars`, `fallback_source` (`mock` / phù hợp với code), `poll_interval_seconds`
- **`backtest`**: vốn ban đầu, slippage, thư mục kết quả

**Không commit** `config.yaml` chứa mật khẩu/token thật (đã có trong `.gitignore`).

### `.env` (tùy chọn)

Dự án dùng `python-dotenv`: có thể đặt token/chat ID trong `.env` và map qua override trong `ConfigLoader` (theo quy ước trong code).

---

## Chạy chương trình

### Realtime (`main.py`)

Luồng: `DataManager` → strategy → `RiskManager` → `TelegramNotifier`.

```bash
conda activate trading-system
python main.py
```

- Trên Windows với MT5 đã cấu hình: dữ liệu lấy từ terminal MT5 (theo `config.yaml`).
- Trên macOS/Linux: thường dùng mock/CSV tùy `data.fallback_source`.

Dừng: `Ctrl+C`.

### Backtest (`backtest.py`)

Đọc CSV tại `data/historical/{SYMBOL}_{TF}.csv` (ví dụ `XAUUSD_H1.csv`). Nếu không có file → tạo dữ liệu synthetic để demo.

```bash
python backtest.py
python backtest.py --symbol XAUUSD --tf H1
python backtest.py --strategy MACDCrossover
python backtest.py --config config.yaml --mock-bars 5000
```

Kết quả: `backtest_results/` (HTML, JSON, biểu đồ).

---

## Tải dữ liệu lịch sử (backtest)

Script [`scripts/download_historical.py`](scripts/download_historical.py) hỗ trợ:

- **Yahoo Finance** (`yfinance`): Forex, vàng (futures proxy), chỉ số, dầu…
- **Dukascopy** (HTTP công khai): Forex + kim loại, M1 → resample các khung cao hơn

Độ sâu mặc định:

- **D1, H4, H1**: khoảng **2–3 năm** (H4 được tạo từ H1 khi dùng yfinance)
- **M15, M5**: **~3 tháng** (mục tiêu); với yfinance, intraday bị giới hạn API (~60 ngày) — cần đủ 90 ngày thì nên dùng **Dukascopy**

Ví dụ:

```bash
python scripts/download_historical.py --list
python scripts/download_historical.py --symbols XAUUSD EURUSD --source all
python scripts/download_historical.py --source yfinance --timeframes D1 H4 H1 M15 M5
python scripts/download_historical.py --source dukascopy --symbols EURUSD XAUUSD --timeframes M15 M5
```

File lưu tại: `data/historical/`.

---

## Kiểm thử

```bash
conda activate trading-system
pytest tests/ -v --cov=src
```

---

## Cấu trúc thư mục (rút gọn)

```
trading-system/
├── config.example.yaml    # Mẫu cấu hình
├── config.yaml            # Cấu hình local (không commit secrets)
├── environment.yml        # Conda env (Python 3.12)
├── setup.sh               # Tạo/cập nhật env
├── main.py                # Entry realtime
├── backtest.py            # Entry backtest
├── scripts/
│   └── download_historical.py
├── src/
│   ├── data/              # MT5, mock, historical loader, data manager
│   ├── strategies/        # Base + MACD, RSI+EMA
│   ├── risk/
│   ├── notifier/
│   ├── backtest/
│   └── utils/             # config, logger, GIL info
├── tests/
├── data/historical/       # CSV do script tải về
├── logs/
└── backtest_results/
```

---

## Ghi chú hiệu năng (GIL / Python tương lai)

- Mặc định dùng **Python 3.12** (GIL bật). Code có kiểm tra `sys._is_gil_enabled` cho bản build **free-threaded** (3.13+/3.14) nếu sau này bạn chuyển sang.
- `setup.sh` có thể in trạng thái GIL; chạy song song strategy vẫn dùng `ThreadPoolExecutor` — với GIL bật, CPU-bound vẫn chủ yếu một luồng tại một thời điểm.

---

## License & tuyên bố rủi ro

Dự án phục vụ **học tập và nghiên cứu**. Không phải tư vấn đầu tư. Giao dịch thực có rủi ro; bạn chịu trách nhiệm với cấu hình, broker và tuân thủ pháp luật địa phương.

# `_normalize_row` 与 `主力占比%` 说明

## 1. `_normalize_row` 的作用

`_normalize_row` 用于把上传数据中的“原始列”标准化为数据库字段，并基于关键指标计算派生字段。

函数位置：`stockdb2/dbinsert.py`

输入：
- `row`: 单行原始数据（来自上传文件）
- `target_date`: 从文件名推断出的交易日期
- `source_filename`: 原始文件名

输出：
- 一个标准化 `dict`，供后续 `_upsert_stock` / `_upsert_daily` 入库使用。

---

## 2. 原始列与标准字段映射

直接映射（经过清洗转换）：

- `名称` -> `name`
- `细分行业` -> `industry`（为空时回退为 `ETF`）
- `现价` -> `price`
- `换手%` -> `turnover_pct`
- `现量` -> `current_volume`
- `主力净额` -> `net_inflow_100m`
- `主力占比%` -> `relative_flow_pct`
- `攻击波%` -> `large_flow_pct`
- `流通市值` -> `free_float_market_cap_100m`
- `流通股(亿)` -> `free_float_shares_100m`
- `涨幅%` -> `change_pct`
- `涨速%` -> `speed_pct`
- `量比` -> `volume_ratio`
- `连涨天` -> `streak_days`
- 参数 `target_date` -> `date`
- 参数 `source_filename` -> `source_filename`

---

## 3. 派生字段计算关系（明确原始列来源）

设：

- $N$ = `主力净额`（转成亿元后，即 `net_inflow_100m`）
- $R$ = `主力占比%`（即 `relative_flow_pct`）
- $L$ = `攻击波%`（即 `large_flow_pct`）
- $P$ = `现价`（即 `price`）

### 3.1 由 `主力净额` + `主力占比%` 反推成交额

$$
\text{turnover\_amount\_100m} = \frac{N}{R/100}
$$

对应代码逻辑：
- 若 `N` 或 `R` 缺失，或 `R == 0`，则返回 `None`。

### 3.2 由成交额与净流入拆分流入/流出额

$$
\text{inflow\_amount\_100m} = \frac{\text{turnover\_amount\_100m} + N}{2}
$$

$$
\text{outflow\_amount\_100m} = \frac{\text{turnover\_amount\_100m} - N}{2}
$$

### 3.3 使用 `现价` 将金额换算为“量”

$$
\text{trade\_volume\_100m} = \frac{\text{turnover\_amount\_100m}}{P}
$$

$$
\text{inflow\_volume\_100m} = \frac{\text{inflow\_amount\_100m}}{P}
$$

$$
\text{outflow\_volume\_100m} = \frac{\text{outflow\_amount\_100m}}{P}
$$

对应代码逻辑：
- 当分母 `P` 为 `None` 或 `0` 时，结果为 `None`。

### 3.4 由 `攻击波%` 估算大宗流入额

$$
\text{large\_inflow\_100m} = \text{turnover\_amount\_100m} \times \frac{L}{100}
$$

---

## 4. `主力占比%` 的含义（重点）

在本项目语义中，`主力占比%` 表示：

- 主力净流入（或净流出）占当日成交额的比例（百分比）。

因此它既是“资金强度”指标，也是反推成交额的关键因子。

### 4.1 解释方式

- 为正：主力净流入占比为正，偏流入。
- 为负：主力净流出占比为负，偏流出。
- 绝对值越大：主力资金对当日成交的影响越强。

### 4.2 计算示例

如果：
- `主力净额 = 1.2亿`
- `主力占比% = 12`

则：

$$
\text{成交额} = 1.2 \div 0.12 = 10\text{亿}
$$

即主力净流入约占成交额的 12%。

---

## 5. 数据质量与空值处理

- `主力占比%` 为 `0` 或空值时，`turnover_amount_100m` 无法计算，相关派生字段会连带为 `None`。
- `现价` 为空或为 `0` 时，基于除法换算的量字段会是 `None`。
- 金额类原始字符串支持中文单位（如 `亿`、`万`），统一转为“亿元”尺度。

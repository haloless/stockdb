(function () {
    function buildQuery(params) {
        var searchParams = new URLSearchParams();
        Object.keys(params).forEach(function (key) {
            var value = params[key];
            if (value !== null && value !== undefined && value !== "") {
                searchParams.set(key, value);
            }
        });
        return searchParams.toString();
    }

    function selectedValues(selectEl) {
        return Array.prototype.slice.call(selectEl.options)
            .filter(function (opt) { return opt.selected; })
            .filter(function (opt) { return (opt.value || "").trim() !== ""; })
            .map(function (opt) { return opt.value; });
    }

    function numberText(value) {
        if (value === null || value === undefined || Number.isNaN(value)) {
            return "";
        }
        return Number(value).toFixed(4);
    }

    function renderTable(tableEl, rows, headerMapper) {
        var thead = tableEl.querySelector("thead");
        var tbody = tableEl.querySelector("tbody");
        if (!rows || rows.length === 0) {
            thead.innerHTML = "";
            tbody.innerHTML = "<tr><td>无数据</td></tr>";
            return;
        }

        var keys = Object.keys(rows[0]);
        var mapHeader = headerMapper || function (key) { return key; };
        thead.innerHTML = "<tr>" + keys.map(function (key) {
            return '<th data-key="' + key + '">' + mapHeader(key) + '</th>';
        }).join("") + "</tr>";

        tbody.innerHTML = rows.map(function (row) {
            return "<tr>" + keys.map(function (key) {
                var value = row[key];
                if (typeof value === "number") {
                    value = numberText(value);
                }
                return "<td>" + (value === null || value === undefined ? "" : value) + "</td>";
            }).join("") + "</tr>";
        }).join("");
    }

    function getTimeseriesHeaderLabel(key) {
        var labels = {
            date: "日期",
            symbol: "股票代码",
            name: "股票名称",
            industry: "行业",
            net_inflow_100m: "净流入(亿元)",
            relative_flow_pct: "相对流量%",
            large_flow_pct: "大宗流量%",
            turnover_amount_100m: "成交额(亿元)",
            inflow_amount_100m: "流入额(亿元)",
            outflow_amount_100m: "流出额(亿元)",
            trade_volume_100m: "成交量(亿股)",
            large_inflow_100m: "大宗流入(亿元)"
        };

        if (labels[key]) {
            return labels[key];
        }

        var avgSuffix = "_roll_avg";
        var sumSuffix = "_roll_sum";
        var cumSuffix = "_cum";
        if (key.endsWith(avgSuffix)) {
            var avgBase = key.slice(0, -avgSuffix.length);
            return (labels[avgBase] || avgBase) + "(滚动均值)";
        }
        if (key.endsWith(sumSuffix)) {
            var sumBase = key.slice(0, -sumSuffix.length);
            return (labels[sumBase] || sumBase) + "(滚动累计)";
        }
        if (key.endsWith(cumSuffix)) {
            var cumBase = key.slice(0, -cumSuffix.length);
            return (labels[cumBase] || cumBase) + "(累计)";
        }

        return key;
    }

    function getChartMetricKey(metric, window, useCumulative) {
        if (useCumulative) {
            return metric + "_cum";
        }
        if (window > 1) {
            return metric + "_roll_avg";
        }
        return metric;
    }

    function getStatsHeaderLabel(key) {
        var labels = {
            group_key: "分组键",
            group_name: "名称",
            days: "统计天数",
            avg_net_inflow_100m: "平均净流入(亿元)",
            cum_net_inflow_100m: "累计净流入(亿元)",
            avg_relative_flow_pct: "平均相对流量%",
            avg_large_flow_pct: "平均大宗流量%",
            avg_turnover_amount_100m: "平均成交额(亿元)",
            cum_turnover_amount_100m: "累计成交额(亿元)",
            avg_trade_volume_100m: "平均成交量(亿股)",
            cum_trade_volume_100m: "累计成交量(亿股)"
        };

        return labels[key] || key;
    }

    function sortRows(rows, sortState) {
        if (!sortState || !sortState.key) {
            return rows.slice();
        }

        var direction = sortState.direction === "desc" ? -1 : 1;
        var key = sortState.key;

        return rows.slice().sort(function (left, right) {
            var leftValue = left[key];
            var rightValue = right[key];

            if (leftValue === null || leftValue === undefined) {
                return rightValue === null || rightValue === undefined ? 0 : 1;
            }
            if (rightValue === null || rightValue === undefined) {
                return -1;
            }

            if (typeof leftValue === "number" && typeof rightValue === "number") {
                return (leftValue - rightValue) * direction;
            }

            return String(leftValue).localeCompare(String(rightValue), "zh-CN", {
                numeric: true,
                sensitivity: "base"
            }) * direction;
        });
    }

    function decorateSortableHeader(tableEl, sortState, headerMapper, onSortChange) {
        var headerCells = tableEl.querySelectorAll("thead th");
        Array.prototype.forEach.call(headerCells, function (cell) {
            var key = cell.getAttribute("data-key");
            if (!key) {
                return;
            }

            var label = headerMapper(key);
            if (sortState.key === key) {
                label += sortState.direction === "desc" ? " ▼" : " ▲";
            }

            cell.textContent = label;
            cell.style.cursor = "pointer";
            cell.addEventListener("click", function () {
                var nextDirection = "asc";
                if (sortState.key === key && sortState.direction === "asc") {
                    nextDirection = "desc";
                }
                onSortChange({key: key, direction: nextDirection});
            });
        });
    }

    function loadMeta(fillIndustrySelects, fillDateDefaults) {
        return fetch("/api2/meta")
            .then(function (resp) { return resp.json(); })
            .then(function (meta) {
                var industries = meta.industries || [];
                fillIndustrySelects.forEach(function (el) {
                    if (!el) {
                        return;
                    }
                    var defaultOption = '<option value="" selected>不限行业（全部）</option>';
                    var options = industries.map(function (item) {
                        return '<option value="' + item + '">' + item + '</option>';
                    }).join("");
                    el.innerHTML = defaultOption + options;
                });

                if (fillDateDefaults && meta.date_range) {
                    fillDateDefaults(meta.date_range.start, meta.date_range.end);
                }

                return meta;
            });
    }

    function renderIndustrySelect(selectEl, industries, keyword) {
        if (!selectEl) {
            return;
        }

        var selectedSet = {};
        Array.prototype.forEach.call(selectEl.options, function (opt) {
            if (opt.selected) {
                selectedSet[opt.value] = true;
            }
        });

        var text = (keyword || "").trim().toLowerCase();
        var filtered = (industries || []).filter(function (item) {
            return !text || String(item).toLowerCase().indexOf(text) !== -1;
        });

        var options = ['<option value="" selected>不限行业（全部）</option>'];
        filtered.forEach(function (item) {
            var selectedAttr = selectedSet[item] ? " selected" : "";
            options.push('<option value="' + item + '"' + selectedAttr + '>' + item + '</option>');
        });
        selectEl.innerHTML = options.join("");
    }

    function initUploadPage() {
        var fileInput = document.getElementById("fileInput");
        var uploadBtn = document.getElementById("uploadBtn");
        var resultEl = document.getElementById("uploadResult");

        uploadBtn.addEventListener("click", function () {
            if (!fileInput.files || fileInput.files.length === 0) {
                resultEl.textContent = "请先选择文件。";
                return;
            }

            var fd = new FormData();
            Array.prototype.forEach.call(fileInput.files, function (file) {
                fd.append("files", file);
            });

            resultEl.textContent = "导入中...";
            fetch("/api2/upload", {
                method: "POST",
                body: fd
            })
                .then(function (resp) { return resp.json(); })
                .then(function (result) {
                    resultEl.textContent = JSON.stringify(result, null, 2);
                })
                .catch(function (err) {
                    resultEl.textContent = "导入失败: " + err;
                });
        });
    }

    function initStatsPage() {
        var symbolsInput = document.getElementById("symbolsInput");
        var industriesSelect = document.getElementById("industriesSelect");
        var startDate = document.getElementById("startDate");
        var endDate = document.getElementById("endDate");
        var groupBy = document.getElementById("groupBy");
        var statsBtn = document.getElementById("statsBtn");
        var table = document.getElementById("statsTable");
        var statsRows = [];
        var statsSortState = {key: null, direction: "asc"};

        function renderStatsTable() {
            var rows = sortRows(statsRows, statsSortState);
            renderTable(table, rows, getStatsHeaderLabel);
            decorateSortableHeader(table, statsSortState, getStatsHeaderLabel, function (nextSortState) {
                statsSortState = nextSortState;
                renderStatsTable();
            });
        }

        loadMeta([industriesSelect], function (start, end) {
            startDate.value = start || "";
            endDate.value = end || "";
        });

        statsBtn.addEventListener("click", function () {
            var q = buildQuery({
                symbols: symbolsInput.value.trim(),
                industries: selectedValues(industriesSelect).join(","),
                start_date: startDate.value,
                end_date: endDate.value,
                group_by: groupBy.value
            });

            fetch("/api2/stats?" + q)
                .then(function (resp) { return resp.json(); })
                .then(function (result) {
                    statsRows = result.rows || [];
                    statsSortState = {key: null, direction: "asc"};
                    renderStatsTable();
                });
        });
    }

    function initTimeseriesPage() {
        var symbolsInput = document.getElementById("tsSymbolsInput");
        var industrySearchInput = document.getElementById("tsIndustrySearch");
        var industriesSelect = document.getElementById("tsIndustriesSelect");
        var startDate = document.getElementById("tsStartDate");
        var endDate = document.getElementById("tsEndDate");
        var windowInput = document.getElementById("windowInput");
        var metricSelect = document.getElementById("metricSelect");
        var cumulativeToggle = document.getElementById("cumulativeToggle");
        var tsBtn = document.getElementById("tsBtn");
        var table = document.getElementById("timeseriesTable");
        var allIndustries = [];

        function applyIndustrySearch() {
            renderIndustrySelect(industriesSelect, allIndustries, industrySearchInput ? industrySearchInput.value : "");
        }

        loadMeta([industriesSelect], function (start, end) {
            startDate.value = start || "";
            endDate.value = end || "";
        }).then(function (meta) {
            allIndustries = (meta && meta.industries) ? meta.industries.slice() : [];
            applyIndustrySearch();
        });

        if (industrySearchInput) {
            industrySearchInput.addEventListener("input", applyIndustrySearch);
        }

        tsBtn.addEventListener("click", function () {
            var metric = metricSelect.value;
            var q = buildQuery({
                symbols: symbolsInput.value.trim(),
                industries: selectedValues(industriesSelect).join(","),
                start_date: startDate.value,
                end_date: endDate.value,
                window: windowInput.value || "1",
                metrics: metric
            });

            fetch("/api2/timeseries?" + q)
                .then(function (resp) { return resp.json(); })
                .then(function (result) {
                    var rows = result.rows || [];
                    renderTable(table, rows.slice(0, 200), getTimeseriesHeaderLabel);
                    renderChart(rows, metric, Number(windowInput.value || "1"), Boolean(cumulativeToggle && cumulativeToggle.checked));
                });
        });
    }

    function renderChart(rows, metric, window, useCumulative) {
        var grouped = {};
        var namesBySymbol = {};
        var yKey = getChartMetricKey(metric, window, useCumulative);
        rows.forEach(function (row) {
            if (!grouped[row.symbol]) {
                grouped[row.symbol] = [];
            }
            grouped[row.symbol].push(row);
            if (!namesBySymbol[row.symbol] && row.name) {
                namesBySymbol[row.symbol] = row.name;
            }
        });

        var traces = Object.keys(grouped).map(function (symbol) {
            var series = grouped[symbol];
            var stockName = namesBySymbol[symbol] || "";
            var legendLabel = stockName ? (symbol + " " + stockName) : symbol;
            return {
                name: legendLabel,
                mode: "lines+markers",
                x: series.map(function (r) { return r.date; }),
                y: series.map(function (r) { return r[yKey]; })
            };
        });

        Plotly.newPlot("chart", traces, {
            margin: {t: 30, r: 20, b: 50, l: 60},
            xaxis: {},
            yaxis: {title: getTimeseriesHeaderLabel(yKey)},
            legend: {orientation: "h"}
        }, {responsive: true});
    }

    window.Stock2 = {
        initUploadPage: initUploadPage,
        initStatsPage: initStatsPage,
        initTimeseriesPage: initTimeseriesPage
    };
})();

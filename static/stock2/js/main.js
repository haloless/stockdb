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
            return "<th>" + mapHeader(key) + "</th>";
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
        if (key.endsWith(avgSuffix)) {
            var avgBase = key.slice(0, -avgSuffix.length);
            return (labels[avgBase] || avgBase) + "(滚动均值)";
        }
        if (key.endsWith(sumSuffix)) {
            var sumBase = key.slice(0, -sumSuffix.length);
            return (labels[sumBase] || sumBase) + "(滚动累计)";
        }

        return key;
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
            });
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
                    renderTable(table, result.rows || []);
                });
        });
    }

    function initTimeseriesPage() {
        var symbolsInput = document.getElementById("tsSymbolsInput");
        var industriesSelect = document.getElementById("tsIndustriesSelect");
        var startDate = document.getElementById("tsStartDate");
        var endDate = document.getElementById("tsEndDate");
        var windowInput = document.getElementById("windowInput");
        var metricSelect = document.getElementById("metricSelect");
        var tsBtn = document.getElementById("tsBtn");
        var table = document.getElementById("timeseriesTable");

        loadMeta([industriesSelect], function (start, end) {
            startDate.value = start || "";
            endDate.value = end || "";
        });

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
                    renderChart(rows, metric, Number(windowInput.value || "1"));
                });
        });
    }

    function renderChart(rows, metric, window) {
        var grouped = {};
        var namesBySymbol = {};
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
            var yKey = window > 1 ? metric + "_roll_avg" : metric;
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
            yaxis: {title: metric + (window > 1 ? " (滚动均值)" : "")},
            legend: {orientation: "h"}
        }, {responsive: true});
    }

    window.Stock2 = {
        initUploadPage: initUploadPage,
        initStatsPage: initStatsPage,
        initTimeseriesPage: initTimeseriesPage
    };
})();

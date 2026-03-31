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

    // Format dates as yyyy-mm-dd
    function formatDate(dateString) {
        var date = new Date(dateString);
        var year = date.getFullYear();
        var month = String(date.getMonth() + 1).padStart(2, '0');
        var day = String(date.getDate()).padStart(2, '0');
        return year + '-' + month + '-' + day;
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

        // Get all keys and prioritize key columns at the beginning
        var allKeys = Object.keys(rows[0]);
        var keys = [];
        
        // Priority columns for different tables
        // Stats table: group_key, group_name
        // Timeseries table: symbol, name
        if (allKeys.includes("group_key")) {
            // Custom column order for stats table
            // Follow the required order: group_key, group_name, days, cum_net_inflow_100m, cum_net_inflow_rel, current_price, free_float_market_cap_100m
            var customOrder = [
                "group_key",
                "group_name",
                "days",
                "cum_net_inflow_100m",
                "cum_net_inflow_rel",
                "current_price",
                "free_float_market_cap_100m"
            ];
            
            // Add keys in custom order if they exist
            customOrder.forEach(function(key) {
                if (allKeys.includes(key)) {
                    keys.push(key);
                }
            });
        } else if (allKeys.includes("symbol")) {
            keys.push("symbol");
            if (allKeys.includes("name")) {
                keys.push("name");
            }
            
            // Add remaining keys for timeseries table
            allKeys.forEach(function(key) {
                if (!keys.includes(key)) {
                    keys.push(key);
                }
            });
        } else {
            // Default behavior for other tables
            if (allKeys.includes("group_name")) {
                keys.push("group_name");
            }
            if (allKeys.includes("name")) {
                keys.push("name");
            }
            
            // Add remaining keys
            allKeys.forEach(function(key) {
                if (!keys.includes(key)) {
                    keys.push(key);
                }
            });
        }
        
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
            large_inflow_100m: "大宗流入(亿元)",
            free_float_market_cap_100m: "流通市值(亿元)",
            free_float_shares_100m: "流通股(亿股)"
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
            group_key: "股票代码",
            group_name: "名称",
            days: "统计天数",
            cum_net_inflow_100m: "累计净流入(亿元)",
            cum_net_inflow_rel: "累计净流入相对值%",
            current_price: "现价",
            free_float_market_cap_100m: "流通市值(亿元)"
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
        var positiveDays = document.getElementById("positiveDays");
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
            var queryParams = {
                symbols: symbolsInput.value.trim(),
                industries: selectedValues(industriesSelect).join(","),
                start_date: startDate.value,
                end_date: endDate.value,
                // Always group by stock, so no need for user selection
                group_by: "stock"
            };
            
            // Add positive days filter if value is provided
            var positiveDaysValue = positiveDays.value;
            if (positiveDaysValue && !isNaN(positiveDaysValue) && Number(positiveDaysValue) > 0) {
                queryParams.positive_days = positiveDaysValue;
            }
            
            var q = buildQuery(queryParams);

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
        var cumulativeToggle = document.getElementById("cumulativeToggle");
        var relativeValueToggle = document.getElementById("relativeValueToggle");
        var tsBtn = document.getElementById("tsBtn");
        var exportCsvBtn = document.getElementById("exportCsvBtn");
        var table = document.getElementById("timeseriesTable");
        var metricCheckboxesContainer = document.getElementById("metricCheckboxes");
        // New filter elements
        var sortOrder = document.getElementById("sortOrder");
        var limitCount = document.getElementById("limitCount");
        var positiveDays = document.getElementById("positiveDays");
        var sortOrderCheck = document.getElementById("sortOrderCheck");
        var limitCountCheck = document.getElementById("limitCountCheck");
        var positiveDaysCheck = document.getElementById("positiveDaysCheck");
        var allIndustries = [];
        var currentChartData = null;

        // Function to get selected metric checkboxes
        function getSelectedMetrics() {
            // Always include net_inflow_100m as it's mandatory
            var metrics = ['net_inflow_100m'];
            
            // Add other selected metrics
            var otherChecked = Array.prototype.slice.call(metricCheckboxesContainer.querySelectorAll('input[type="checkbox"]:checked'))
                .map(function(checkbox) { return checkbox.value; });
            
            // Combine and remove duplicates
            return [...new Set([...metrics, ...otherChecked])];
        }

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
            var metrics = getSelectedMetrics();
            var windowValue = Number(windowInput.value || "1");
            var useCumulative = Boolean(cumulativeToggle && cumulativeToggle.checked);
            var useRelativeValue = Boolean(relativeValueToggle && relativeValueToggle.checked);
            
            // Validate that at least one metric is selected
            if (metrics.length === 0) {
                alert("请至少选择一个主展示指标");
                return;
            }
            
            // Validate filter inputs only if their checkboxes are checked
            var limitCountValue = limitCount.value;
            if (limitCountCheck.checked && limitCountValue && (isNaN(limitCountValue) || Number(limitCountValue) < 1)) {
                alert("显示数量(N)必须是大于0的整数");
                limitCount.focus();
                return;
            }
            
            var positiveDaysValue = positiveDays.value;
            if (positiveDaysCheck.checked && positiveDaysValue && (isNaN(positiveDaysValue) || Number(positiveDaysValue) < 1)) {
                alert("连续净流入天数(N)必须是大于0的整数");
                positiveDays.focus();
                return;
            }
            
            // Build query parameters with checkbox validation
            var queryParams = {
                symbols: symbolsInput.value.trim(),
                industries: selectedValues(industriesSelect).join(","),
                start_date: startDate.value,
                end_date: endDate.value,
                window: windowInput.value || "1",
                metrics: metrics.join(","),
                use_cumulative: useCumulative,
                use_relative: useRelativeValue
            };
            
            // Add filter parameters only if their checkboxes are checked
            if (sortOrderCheck.checked && sortOrder.value) {
                queryParams.sort_order = sortOrder.value;
            }
            
            if (limitCountCheck.checked && limitCountValue) {
                queryParams.limit_count = limitCountValue;
            }
            
            if (positiveDaysCheck.checked && positiveDaysValue) {
                queryParams.positive_days = positiveDaysValue;
            }
            
            var q = buildQuery(queryParams);

            fetch("/api2/timeseries?" + q)
                .then(function (resp) { return resp.json(); })
                .then(function (result) {
                    var rows = result.rows || [];
                    renderTable(table, rows.slice(0, 200), getTimeseriesHeaderLabel);
                    renderChart(rows, metrics, windowValue, useCumulative, useRelativeValue);
                    
                    // Store chart data for export
                    currentChartData = {
                        rows: rows,
                        metric: metrics,
                        window: windowValue,
                        useCumulative: useCumulative,
                        useRelativeValue: useRelativeValue
                    };
                });
        });

        exportCsvBtn.addEventListener("click", function () {
            if (!currentChartData) {
                alert("请先生成图表数据");
                return;
            }
            
            var csvContent = convertChartDataToCsv(currentChartData);
            downloadCsv(csvContent, "stock_timeseries_data.csv");
        });

        function convertChartDataToCsv(chartData) {
            var rows = chartData.rows;
            var metrics = chartData.metric;
            var window = chartData.window;
            var useCumulative = chartData.useCumulative;
            var useRelativeValue = chartData.useRelativeValue;
            
            // Ensure metrics is an array
            if (!Array.isArray(metrics)) {
                metrics = [metrics];
            }
            
            // Group data by symbol
            var grouped = {};
            var dates = [];
            var stockInfo = []; // Array of {symbol, name} objects
            
            rows.forEach(function (row) {
                if (!grouped[row.symbol]) {
                    grouped[row.symbol] = {};
                    stockInfo.push({symbol: row.symbol, name: row.name || row.symbol});
                }
                grouped[row.symbol][row.date] = row;
                if (dates.indexOf(row.date) === -1) {
                    dates.push(row.date);
                }
            });
            
            // Sort dates
            dates.sort();
            
            // Build CSV header
            var header = ["日期"];
            stockInfo.forEach(function (stock) {
                metrics.forEach(function (metric) {
                    var metricLabel = getTimeseriesHeaderLabel(metric).replace(/\([^)]*\)/g, ''); // Remove unit
                    var columnName = stock.symbol + " " + stock.name + " - " + metricLabel;
                    header.push(columnName);
                });
            });
            
            // Build CSV rows
            var csvRows = [header.join(",")];
            
            dates.forEach(function (date) {
                var row = [formatDate(date)];
                stockInfo.forEach(function (stock) {
                    metrics.forEach(function (metric) {
                        var yKey = getChartMetricKey(metric, window, useCumulative);
                        var dataRow = grouped[stock.symbol][date];
                        if (dataRow) {
                            var value = dataRow[yKey];
                            if (useRelativeValue) {
                                // Enhanced relative value calculation based on metric type
                                if (metric === 'trade_volume_100m' && dataRow.free_float_shares_100m && dataRow.free_float_shares_100m > 0) {
                                    // Normalize volume by free float shares
                                    value = (value / dataRow.free_float_shares_100m) * 100;
                                } else if ([
                                    'net_inflow_100m', 
                                    'turnover_amount_100m', 
                                    'inflow_amount_100m', 
                                    'outflow_amount_100m'
                                ].includes(metric) && dataRow.free_float_market_cap_100m && dataRow.free_float_market_cap_100m > 0) {
                                    // Normalize amount by free float market cap
                                    value = (value / dataRow.free_float_market_cap_100m) * 100;
                                }
                            }
                            row.push(value);
                        } else {
                            row.push("");
                        }
                    });
                });
                csvRows.push(row.join(","));
            });
            
            return csvRows.join("\n");
        }

        function downloadCsv(content, filename) {
            var blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
            var link = document.createElement("a");
            if (link.download !== undefined) {
                var url = URL.createObjectURL(blob);
                link.setAttribute("href", url);
                link.setAttribute("download", filename);
                link.style.visibility = 'hidden';
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            }
        }
    }

    function renderChart(rows, metrics, window, useCumulative, useRelativeValue) {
        // Ensure metrics is an array
        if (!Array.isArray(metrics)) {
            metrics = [metrics];
        }

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

        // Generate traces for all metrics and symbols
        var allTraces = [];
        metrics.forEach(function (metric) {
            var yKey = getChartMetricKey(metric, window, useCumulative);
            
            Object.keys(grouped).forEach(function (symbol) {
                var series = grouped[symbol];
                var stockName = namesBySymbol[symbol] || "";
                var legendLabel = stockName ? (symbol + " " + stockName) : symbol;
                var metricLabel = getTimeseriesHeaderLabel(metric).replace(/\([^)]*\)/g, ''); // Remove unit from label
                var fullLabel = legendLabel + " - " + metricLabel;
                
                var yValues = series.map(function (r) {
                    var value = r[yKey];
                    if (useRelativeValue) {
                        // Enhanced relative value calculation based on metric type
                        if (metric === 'trade_volume_100m' && r.free_float_shares_100m && r.free_float_shares_100m > 0) {
                            // Normalize volume by free float shares
                            return (value / r.free_float_shares_100m) * 100;
                        } else if ([
                            'net_inflow_100m', 
                            'turnover_amount_100m', 
                            'inflow_amount_100m', 
                            'outflow_amount_100m'
                        ].includes(metric) && r.free_float_market_cap_100m && r.free_float_market_cap_100m > 0) {
                            // Normalize amount by free float market cap
                            return (value / r.free_float_market_cap_100m) * 100;
                        }
                        // For percentage metrics (relative_flow_pct, large_flow_pct), no normalization needed
                    }
                    return value;
                });
                
                allTraces.push({
                    name: fullLabel,
                    mode: "lines+markers",
                    x: series.map(function (r) { return formatDate(r.date); }),
                    y: yValues
                });
            });
        });

        // Set appropriate y-axis label
        var yLabel = metrics.length === 1 ? getTimeseriesHeaderLabel(metrics[0]) : "指标值";
        if (useRelativeValue) {
            yLabel = yLabel + "(相对值, %)";
        }

        Plotly.newPlot("chart", allTraces, {
            margin: {t: 30, r: 20, b: 50, l: 60},
            xaxis: {
                tickformat: '%Y-%m-%d', // Plotly format for consistency
                dtick: "D1",           // Set tick interval to exactly 1 day
            },
            yaxis: {title: yLabel},
            legend: {
                orientation: "v",
                xanchor: "left",
                yanchor: "top",
                x: 1.02,
                y: 1
            }
        }, {responsive: true});
    }

    window.Stock2 = {
        initUploadPage: initUploadPage,
        initStatsPage: initStatsPage,
        initTimeseriesPage: initTimeseriesPage
    };
})();

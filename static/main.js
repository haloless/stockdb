
function split_by_space(s) {
    s = s.trim();
    if (s)
        return s.split(/\s+/);
    else
        return [];
}

function register_df_to_table(selector) {
    return function (stockdata) {
        $(selector).DataTable({
            destroy: true,
            data: stockdata.data,
            // language: {
            //     url: "https://cdn.datatables.net/plug-ins/3cfcc339e89/i18n/Chinese.json"
            // },
            columns: function () {
                var cols = [];
                // console.log(stockdata.schema)
                for (var i = 0; i < stockdata.schema.fields.length; i++) {
                    var f = stockdata.schema.fields[i];
                    cols.push({ 'data': f.name, 'title': f.name });
                }
                // console.log(cols)
                return cols;
            }(),

            aLengthMenu: [
                [10, 25, 50, 100, 200, -1],
                [10, 25, 50, 100, 200, "All"]
            ]
        });
    }
}



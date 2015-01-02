var ElasticSearch = exports;

var config = require("./config");
var http = require("http");

ElasticSearch.query = function (query, result_callback, error_callback) {
    var esquery_options = {
        hostname: config.ES_HOST,
        port: config.ES_PORT,
        path: '/' + config.ES_INDEX + '/doc/_search',
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(query, 'utf8')
        }
    };

    var req = http.request(esquery_options, function (response) {
        var raw_data = '';
        response.on('data', function (chunk) {
            raw_data += chunk;
        });
        response.on('end', function () {
            var json_data = JSON.parse(raw_data);
            if (response.statusCode == 200) {
                result_callback(json_data);
            } else {
                error_callback(json_data);
            }
        });
    });

    req.on('error', function (error) {
        error_callback(error);
    });
    req.write(query);
    req.end();
};


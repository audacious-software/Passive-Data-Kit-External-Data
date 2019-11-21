requirejs.config({
    baseUrl: "/static/pdk_external",
    paths: {
		bootstrap: "lib/bootstrap4/js/bootstrap.bundle.min",
		jquery: 'lib/jquery/jquery-3.4.1.min',
		"js-cookie": 'lib/js.cookie-2.2.1.min'
    },
    shim: {
        "jquery": {
            exports: "$"
        },
        "moment": {
            exports: "moment"
        },
        "js-cookie": {
            exports: "Cookie"
        },
        "bootstrap": {
            deps: ["jquery", "js-cookie"],
            exports: "bootstrap"
        },
        "bootstrap-typeahead": {
            deps: ["bootstrap"],
        },
        "bootstrap-table": {
            deps: ["bootstrap"],
        },
        "bootstrap-datepicker": {
            deps: ["bootstrap"],
        },
        "bootstrap-timepicker": {
            deps: ["bootstrap"],
        },
        "rickshaw": {
            deps: ["d3-layout", "jquery"],
            exports: "Rickshaw"
        },
        "d3-layout": {
            deps: ["d3", "jquery"]
        },
        "d3": {
            exports: "d3"
        }
    }
});

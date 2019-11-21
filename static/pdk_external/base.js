//Load common code that includes config, then load the app logic for this page.
requirejs(['./common'], function (common) {
    requirejs(["bootstrap"], function (popper, bootstrap) {
		$.ajaxSetup({ 
			beforeSend: function(xhr, settings) {
				if (!(/^http:.*/.test(settings.url) || /^https:.*/.test(settings.url))) {
					xhr.setRequestHeader("X-CSRFToken", $.cookie('csrftoken'));
				}
			}
		});
	}); 
});
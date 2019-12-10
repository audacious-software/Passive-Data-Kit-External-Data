//Load common code that includes config, then load the app logic for this page.
requirejs(['./common'], function (common) {
    requirejs(["bootstrap", "ekko-lightbox"], function () {
		$.ajaxSetup({ 
			beforeSend: function(xhr, settings) {
				if (!(/^http:.*/.test(settings.url) || /^https:.*/.test(settings.url))) {
					xhr.setRequestHeader("X-CSRFToken", $.cookie('csrftoken'));
				}
			}
		});

		$(document).on('click', '[data-toggle="lightbox"]', function(event) {
			event.preventDefault();
			$(this).ekkoLightbox();
		});
	}); 
});
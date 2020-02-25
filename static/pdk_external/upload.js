//Load common code that includes config, then load the app logic for this page.
requirejs(['./common'], function (common) {
    requirejs(["bootstrap", 'js-cookie'], function (bootstrap, Cookies) {
		$.ajaxSetup({ 
			beforeSend: function(xhr, settings) {
				if (!(/^http:.*/.test(settings.url) || /^https:.*/.test(settings.url))) {
					xhr.setRequestHeader("X-CSRFToken", Cookies.get('csrftoken'));
				}
			}
		});

		$('#upload-form').submit(function(e) {
			var message = null;
			
			$('input[type=file]').each(function() {
				var filename = $(this).val();
				
				if (filename != "" && filename.toLowerCase().endsWith(".zip") == false) {
					message = "Please verify that you have only selected ZIP files for upload.";
				};
			});

			if (message != null) {
				e.preventDefault();

				alert(message);

				return false;
			}

			return true;
		});
	}); 
});
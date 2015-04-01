function populate_boxes(links) {
	$('.list-group').each(function() {
		if ($(this).find('.list-group-item').length === 0) {
			var html = ""
			$.each(links, function(key,value) {
				var edge_class = value.is_in_edge == true ? 'inlink' : 'outlink';
				var topic_class = value.href.substring(7,13) == 'topics' ? 'topic' : 'nontopic'
				html += '<a href="'+value.href+'" class="list-group-item '+edge_class+' '+topic_class+'">';
				html += '<h4 class="list-group-item-heading">'+value.title+'</h4>';
				html += '<p class="list-group-item-text">'+value.publish_date+'</p>';
				if (value.wordcount) {
					html += '<p class="list-group-item-text"><em>Word count: '+value.wordcount+'</em></p>';
				}
				if (value.anchor) {
					html += '<p class="list-group-item-text"><strong>'+value.anchor+'</strong> / &para; '+value.para+'</p>';
				}
				html += '</a>';
			})
			$(this).html(html);
			return false;
		}
	})
}

function call_ajax_primary(event) {
	var url = $(this).attr('href');
	if (! url) {
		var url = $('#url').val();
		var nextCols = $('.col-sm-3')
	}
	else {
		var nextCols = $(this).offsetParent().nextAll('.col-sm-3')
	}
	var nextCol = nextCols[0];
	nextCols.find('ul').empty();
	var spinner = new Spinner().spin(nextCol);
	var radioMode = $('input[name=radio-opt]:checked').val();
	var radioSortMode = $('input[name=radio-sort-opt]:checked').val();
	var spiderLevel = $('.spiderSelect option:selected').val();
    $.ajax({
          type: "GET",
          url: "http://localhost:5000/hello.json?url="+url+"&mode="+radioMode+"&sortMode="+radioSortMode+"&spider="+spiderLevel,
          success: function(data) {
          	spinner.stop();
          	populate_boxes(data['results']);
          },
          dataType: 'json'
    });
    return false;
}

$(document).ready(function() {
	$('.btn-primary').on('click', call_ajax_primary);
	$('.list-group').on('click', '.list-group-item', call_ajax_primary);
})
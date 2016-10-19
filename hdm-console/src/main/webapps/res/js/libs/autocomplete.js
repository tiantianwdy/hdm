
define(["jquery", "../plugins/jquery-ui-1.9.2.widgets.min"], function ($) {
    "use strict";
    //auto complete
    $.fn.auto = function (id, options) {
        var defaults = {
            url: "",		//默认当前页面
            method: "POST",		//通信方式
            pageNo: 1,		//当前页码
            pageSize: 20,		//每页的条数
            data: {},		//AJAX要传送的数据
            isJSONP: false,	//是否为JSONP
            minLength: 2,//最小响应
            multiple: false,//是否多选
            cached: true,//是否缓存查询
            itemformat: function (item) {
                var type = $.type(item);
                var val = "";
                if (type === "undefined")
                    val = "";
                else if (type === "string") {
                    val = item;
                } else if (type === "object" && item.id && item.name) {
                    val = item.id+"-"+item.name;
                } else
                    val += item;
                return val;
            },
            dataformat: function (item) {
                return options.multiple ? item.id : item.name;
            }
        };
        var options = $.extend(defaults, options);
        $("#" + id).each(function (obj) {
            var $self = $(this);
            var cache = {};
            $self.bind("keydown",function (event) {
                if (event.keyCode === $.ui.keyCode.TAB &&
                    $(this).data("ui-autocomplete").menu.active) {
                    event.preventDefault();
                }
            }).autocomplete({
                    minLength: options.minLength,
                    source: function (request, response) {
                        var term = options.multiple ? extractLast(request.term) : request.term;
                        options.data.value = term;
                        if (options.cached && term in cache) {
                            response(cache[ term ]);
                            return;
                        }
                        //请求服务器
                        options.data['page.size'] = options.pageSize;
                        $.ajax({
                            type: options.method,
                            url: options.url,
                            data: options.data,
                            success: function (data) {
                                cache[ term ] = data;
                                response(data);
                            },
                            dataType: "json"
                        });
                    },
                    messages: {
                        noResults: '',
                        results: function () {
                            //console.log(arguments);
                        }
                    },
                    focus: function () {
                        return false;
                    },
                    select: function (event, ui) {
                        if ($.type(options.select) === "function") {
                            options.select(event, ui, $(this));
                            return false;
                        } else {
                            var val = ($.type(options.dataformat) === "function") ? options.dataformat(ui.item) : (($.type(options.itemformat) === "function") ? options.itemformat(ui.item) : ui.item.name);
                            if (options.multiple) {
                                var terms = split(this.value);//输入框中的值
                                terms.pop();// 删除当前输入的值
                                terms.push(val);// 加入当前选择的值
                                terms.push("");// add placeholder to get the comma-and-space at the end
                                this.value = terms.join(", ");
                            } else {
                                $(this).val(val);
                            }
                            return false;
                        }
                    }
                }).data("ui-autocomplete")._renderItem = function (ul, item) {
                return $("<li>").append("<a>" + options.itemformat(item) + "</a>").appendTo(ul);
            };
        });
        function split(val) {
            return val.split(/,\s*/);
        }

        function extractLast(term) {
            return split(term).pop();
        }
    };
});//end require
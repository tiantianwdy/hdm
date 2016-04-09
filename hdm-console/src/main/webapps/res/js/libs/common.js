
//global
var ajax_base = "",
    CONS = {
        vars: {
            ajax_load: 'data loading......',	//loading文字
            ajax_error: 'data loading failed.'
        },
        //AJAX地址列表
        ajax: {

        },
        fns: {
            //ajax工具
            ajax: function (options) {
                if ($("#mini_load").length === 0) {
                    $("body").append('<div id="mini_load"></div>');
                }
                $("#mini_load").html(CONS.vars.ajax_load).fadeIn(300);
                $.ajax({
                    url: options.url,
                    data: options.data,
                    dataType: "json",
                    cache: false,
                    timeout: 5000,
                    type: (options.type == undefined) ? "POST" : options.type,
                    contentType: (!options.contentType) ? undefined : options.contentType,
                    processData: true,
                    error: function (XMLHttpRequest, textStatus, errorThrown) {
                        //alert('请求失败,错误类型：'+textStatus);
                       /* $("#mini_load").html(CONS.vars.ajax_error);
                        window.setTimeout(function () {
                            $("#mini_load").stop().fadeOut(300);
                        }, 2000);*/
                    },
                    success: function (result) {
                        $("#mini_load").stop().fadeOut(300);
                        if (typeof(options.success) == "function" && (result.code == 200)) {
                            options.success(result);
                        } else {
                            if (!result.message) {
                                console.info("返回结果格式不正确", result);
                            } else {
                                alert(result.message);
                            }
                            return;
                        }
                    }
                });
            },   //end ajax

            //ajax方式提交表单-表单或DIV的ID，[是否发送（默认否，返回JSON值）,ajax请求参数（url,type,cb）](只支持text hidden radio checkbox select textarea)
            formAjax: function (id, send, ajax) {
                if (!id || typeof id !== 'string') {
                    alert('未传入ID值');
                    return;
                }
                send = send || false;
                var data = {};
                $("#" + id + " input,#" + id + " textarea,#" + id + " select").each(function () {
                    if (this.name) {
                        switch (this.type) {
                            case "text":
                            case "password":
                            case "hidden":
                                data[this.name] = $(this).val();
                                break;
                            case "radio":
                                data[this.name] = $("input[name=" + this.name + "]:checked").val();
                                break;
                            case "checkbox":
                                if ($(this).attr("checked")) {
                                    if (typeof data[this.name] != 'object') {
                                        data[this.name] = [];
                                    }
                                    data[this.name].push($(this).val());
                                }
                                break;
                            default:
                                if (this.tagName.toUpperCase() === "TEXTAREA") {
                                    data[this.name] = $(this).val();
                                }
                                if (this.tagName.toUpperCase() === "SELECT") {
                                    data[this.name] = $(this).val();
                                }
                        }
                    }
                });
                //console.log(data);
                if (!send) {
                    return data;
                }
                else {
                    if (typeof ajax == "undefined" || !ajax.url || !$.toJSON) {
                        alert('未传入AJAX请求地址或未引入tojson.js文件');
                        return;
                    }
                    var d = {url: ajax.url, data: (!ajax.dataType || ajax.dataType.toUpperCase() == 'json') ? ($.toJSON(data)) : data};
                    if (ajax.type) {
                        d.type = ajax.type;
                    }
                    if (ajax.contentType) {
                        d.contentType = ajax.contentType;
                    }
                    if (ajax.cb) {
                        d.success = ajax.cb;
                    }
                    CONS.fns.ajax(d);
                }

            },

            //ajax或JSON填充表单-要填充表单或DIV的ID，要填充的JSON数据或者AJAX发送的参数，data是否作为AJAX发送参数（默认否）
            fillForm: function (id, data, send) {
                send = send || false;
                if (!id || typeof id !== 'string') {
                    alert('未传入要填充的表单或者DIV的ID值');
                    return;
                }
                //填充数据data
                var fill = function (data) {
                    if (!data) {
                        return;
                    }
                    $("#" + id + " input,#" + id + " textarea,#" + id + " a").each(function () {
                        if (this.name && data[this.name] !== 'undefined') {
                            var value = data[this.name];
                            switch (this.type) {
                                case "text":
                                case "password":
                                case "hidden":
                                    $(this).val(value);
                                    break;
                                case "radio":
                                case "checkbox":
                                    if (Object.prototype.toString.call(data[this.name]) === '[object Array]' && String.prototype.indexOf.call(['', value, ''], ',' + $(this).val() + ',') !== -1) {
                                        $(this).attr("checked", true);
                                    } else {
                                        if ($(this).val() == value || $(this).val() === 'true') {
                                            $(this).attr("checked", true);
                                        }
                                    }
                                    break;
                                default:
                                    if (this.tagName.toUpperCase() === "TEXTAREA") {
                                        $(this).val(value);
                                    }
                                    if (this.tagName.toUpperCase() === "A") {
                                        $(this).text(value);
                                    }
                                    if (this.tagName.toUpperCase() === "SELECT") {
                                        $(this).val(value);
                                    }
                            }
                        }
                    });
                };
                if (!send) {
                    fill(data);
                }
                else {
                    if (typeof data != 'object') {
                        alert('请传入AJAX请求参数');
                        return;
                    }
                    var c = data.cb ? data.cb : function () {
                    };
                    data.success = function (re) {
                        if (!re.data) {
                            return;
                        }
                        fill(re.data); //此处可配置
                        c(re);
                    };
                    CONS.fns.ajax(data);
                }
            },

            /*
             *@name:creatHTML-根据JSON数据创建DOM结构——返回生成的DOM结构字符串
             *creatDom({
             *	[id:'d1'],		-要插入的节点的ID
             *	[ajax:true],	-是否发送AJAX请求，默认否
             *	data:data,		-AJAX请求参数或者JSON数据源
             *	str:'<li>{data.run.pp}</li><li>{data.run.pp2}</li>'	-要生成的DOM结构字符串
             *});
             */
            creatHTML: function (options) {
                if (typeof options.data !== 'object' && typeof options.str !== 'string') {
                    alert('参数传入有误。');
                    return;
                }
                var str = options.str.replace(/["]/g, '``').replace(/[{]/g, "\"+trance(").replace(/[}]/g, ")+\""),//要操作的字符串
                    data = options.data;
                var trance = function (s) {
                    return String(s).replace(/\</g, "&lt;").replace(/\>/g, "&gt;").replace(/\'/g, "&#39;").replace(/\"/g, '&quot;')
                };
                //根据数据生成DOM结构并处理
                var op = function (data) {
                    var htmls = '';
                    if (Object.prototype.toString.call(data) === '[object Array]') {
                        var html = [],
                            fn = Function.call(null, 'data', 'i ', 'trance', 'return "' + str + '";');
                        $.each(data, function (i, j) {
                            html[i] = fn(j, i, trance);
                        });
                        htmls = html.join('').replace(/(\`\`)/g, '"');
                    }
                    else {
                        htmls = Function.call(null, 'data', 'trance', 'return "' + str + '";')(data, trance).replace(/(\`\`)/g, '"');
                    }
                    if (options.id) {
                        $("#" + options.id).html(htmls);
                    }
                    return htmls;
                };
                //判断是否需要发送AJAX获取数据
                if (options.ajax === true) {
                    if (!data.url) {
                        alert('AJAX参数的url未传入。');
                        return;
                    }
                    //此处可以改成项目通用的AJAX请求
                    CONS.fns.ajax({
                        url: data.url,
                        data: data.data ? data.data : '',
                        type: data.type ? data.type : 'POST',
                        success: function (result) {
                            if (typeof data.cb == 'function') {
                                cb(result);
                            }
                            return op(result);
                        }
                    });
                } else {
                    return op(data);
                }
            },

            //加入收藏
            addba: function (title, url) {
                if (window.sidebar) {
                    window.sidebar.addPanel(title, url, "");
                } else if (document.all) {
                    window.external.AddFavorite(url, title);
                } else {
                    alert('您的浏览器不支持此操作，请按Ctrl+D手动完成。');
                }
            },


            //循环遍历函数 要插入节点ID/处理的数组，要处理的字符串，[AJAX参数]
            creatDom: function (id, str, ajax) {
                //if(!id || typeof str!='string'){alert('参数传入错误。');return;}
                if (typeof id == 'string') {
                    var fn = ajax.cb ? ajax.cb : function () {
                    };//原先回调
                    ajax.success = function (re) {
                        var html = '';
                        $.each(re.data, function (i, j) {
                            var str_arr = str.split(/[{}]/);
                            $.each(str_arr, function (m, n) {
                                if (j[n]) {
                                    str_arr[m] = j[n];
                                }
                            });
                            html += str_arr.join('');
                        });
                        $("#" + id).html(html);
                        fn(re);
                    };
                    CONS.fns.ajax(ajax);
                } else {
                    var html = '';
                    $.each(id, function (i, j) {
                        var str_arr = str.split(/[{}]/);
                        $.each(str_arr, function (m, n) {
                            if (j[n]) {
                                str_arr[m] = j[n];
                            }
                        });
                        html += str_arr.join('');
                    });
                    return html;
                }
            },

            //得到地址栏get变量
            getUrlValue: function (key) {
                var svalue = location.search.match(new RegExp("[\?\&]" + key + "=([^\&]*)(\&?)", "i"));
                return svalue ? svalue[1] : svalue;
            },

            //得到字符串长度
            getLength: function (str) {
                return str.replace(/[^\x00-\xff]/g, "aa").length;
            }


        }
    };

//页面公共JS
require(["jquery"], function () {

    Number.prototype.toPercent = function(n){
        n = n || 0;return ( Math.round( this * Math.pow( 10, n + 2 ) ) / Math.pow( 10, n ) ).toFixed( n ) + '%';
    }
    $(function () {

        //初始化主菜单
        var navs = ['index', 'self-', 'sys-', 'log-', 'pro-', 'help-'],
            url = window.location + '',
            nav = $("#header_nav li");
        $.each(navs, function (i, j) {
            if (url.search(j) > -1) {
                nav.eq(i).addClass('cur');
            }
        });

        //初始化bar/bar_hand/bar_child
        var bar = $("#bar");
        bar.css("height", $(document).height() - 96);
        bar.on("click", "#bar_hand",function () {
            var self = $(this);
            if (self.hasClass("bar_s")) {
                bar.animate({marginLeft: 0}, function () {
                    self.removeClass("bar_s");
                });
                $("#tex").animate({marginLeft: 200});
            } else {
                bar.animate({marginLeft: -180}, function () {
                    self.addClass("bar_s");
                });
                $("#tex").animate({marginLeft: 20});
            }
        }).on("click", ".hasc", function () {
                var self = $(this);
                self.css({backgroundPosition: '0px -169px'});
                if ($(this).children(".bar_child").is(":visible")) {
                    $(this).children(".bar_child").slideUp("fast", function () {
                        self.css({backgroundPosition: '0 -139px'});
                    });
                }
                else {
                    $(this).children(".bar_child").slideDown("fast");
                }
            });

        //退出登录
        $("#user").on("click", "#user_name",function () {
            $(this).siblings(".quit").show();
        }).on("mouseleave", ".quit", function () {
                $(this).hide();
            });

        //关闭按钮
        $("#con").on("click", ".oc_hand", function () {
            var ta = $(this).attr('ta');
            if (!ta) {
                return;
            }
            if ($(this).hasClass("oc")) {
                if ($(this).hasClass("oc_close")) {
                    $("#" + ta).slideDown("fast");
                    $(this).removeClass("oc_close").text('收起');
                } else {
                    $("#" + ta).slideUp("fast");
                    $(this).addClass("oc_close").text('展开');
                }
                return;
            }
            ;
            $("#" + ta).slideUp("fast");
        });

    });


});//end ready

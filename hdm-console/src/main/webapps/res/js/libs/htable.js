
/*
 usage：$(".htable").htable({
 url:"data.json",
 model:[{key:"id",value:"ID",width:100,repla:xxx},{key:"name",value:"姓名",width:100},{key:"class",value:"班级",width:200},{key:"date",value:"日期",width:300}]
 //xxx为定义好的替换函数，如：var xxx=function(str){return str.replace(/1/g,'yy');};
 });
 */
(function ($) {
    $.fn.htable = function (options) {
        var defaults = {
            url: "",		//通信URL
            method: "POST",		//通信方式
            pageNo: 1,		//当前页码
            pageSize: 20,		//每页的条数
            data: {},		//AJAX要传送的数据
            model: [],		//表格要显示的参数
            keyWord: "",		//搜索关键字
            page: "pager",		//页码容器的ID，为空不显示页码
            isJSONP: false,		//是否为JSONP
            pageSizeArr:[5,10,20,50,100],//pageSize
            cb: function () {
            }		//回调函数
        };
        options = $.extend(defaults, options);
        this.each(function () {
            var et = $(this);
            //////////////////////////////存放结果，配置变量///////////////////////////////////
            var data = this.data = {
                //AJAX存放结果
                result: {a: 13, b: {b1: 200, b2: []}},
                //是否请求成功
                dataSuccess: function () {
                    var result = data.result;
                    if(result.data && result.data.errors && result.data.errors.length){
                        var message = "获取数据错误!";
                        if ($.isArray(result.data.errors) && result.data.errors.length > 0) {
                            message = result.data.errors[0].defaultMessage;
                        }else{
                            if (result.message) {
                                message = result.message;
                            }
                        }
                        alert(message);
                        return false;
                    }
                    else {
                        return true;
                    }
                },
                //请求地址
                url: options.url,
                //当前页数
                pageNo: options.pageNo,
                //配置POST/GET要传递的数据
                getPostData: options.data,
                //配置总页数
                getTotalPages: function () {
                    return data.result.data.totalPages;
                },
                //配置记录总条数
                getNums: function () {
                    return data.result.data.totalCount;
                },
                //配置表格数据
                getGridData: function () {
                    return data.result.data.result;
                },
                //loading开始
                loading: function () {
                    //et.closest(".mod").addClass("hd_loading");
                },
                //loading结束
                loaded: function () {
                    //et.closest(".bd").removeClass("hd_loading");
                },
                //add by Ryoma 2013-07-27
                getRow: function (rowId) {//获取给定行的数据
                    return data.getGridData()[rowId - 1];
                },
                getSelectedRowId: function () {//获取选中的行
                    return selectedRowId;
                },
                getSelectedId: function () {
                    return this.getRow(this.getSelectedRowId()).id;
                }
            };
            //add by Ryoma 2013-07-27
            var selectedRowId;
            //生成head
            var creatHead = function () {
                var head = '';
                $.each(options.model, function (i, j) {
                    if (j.width) {
                        head += '<td width="' + j.width + '">' + j.value + '</td>';
                    }
                    else {
                        head += '<td>' + j.value + '</td>';
                    }
                });
                return '<thead><tr>' + head + '</tr></thead>';
            };
            //console.log(creatHead());
            //生成body
            var creatBody = function (data) {
                var body = $('<div></div>'),
                    len = data.length;
                if (len === 0) {
                    return ['<tr class="odd"><td colspan="' + options.model.length + '">No records</td></tr>'];
                }
                for (var i = 0; i < len; i++) {
                    if (i >= options.pageSize && options.page) {
                        break;
                    }
                    var tr_data = '';
                    for (var j = 0; j < options.model.length; j++) {//构造每行的数据
                        //options.model[j].key=options.model[j].key?options.model[j].key:data[j];//当没有设置key的时候是个bug
                        options.model[j].key = options.model[j].key ? options.model[j].key : "_rowId";
                        var val = (!options.model[j].key || options.model[j].key == '_rowId') ? i + 1 : getJsonValue(data[i], options.model[j].key.split("."));
                        val = !val?"":val;
                        if (!options.model[j].repla) {
                            tr_data += '<td title="' + val + '">' + val + '</td>';
                        } else {
                            tr_data += '<td>' + options.model[j].repla(i + 1, val,data[i]) + '</td>';
                        }
                    }
                    if (i % 2 === 0) {
                        tr_data = '<tr class="odd">' + tr_data + '</tr>';
                    }
                    else {
                        tr_data = '<tr class="even">' + tr_data + '</tr>';
                    }
                    //添加事件
                    $(tr_data).on("click",function () {//TODO 为啥不好使呢?
                        selectedRowId = i;
                    }).appendTo(body);
                }
                return body.html();
            };
            var getJsonValue = function (data, keys) {
                if ($.isArray(keys) && keys.length > 0) {
                    var newData = data[keys.shift()];
                    if (!newData || keys.length == 0) return newData;
                    return getJsonValue(newData, keys);
                }
            }
            //console.log(creatBody(dd).join(''));

            //Ajax取得数据 请求地址，请求参数[{url:'',data:{key:value}}]
            var getAjaxData = this.update = function (dd) {
                data.getPostData.pageNo = data.pageNo;
                if (dd && dd.url) {
                    data.url = dd.url;
                }
                if (dd && typeof dd.data == 'object') {
                    $.each(dd.data, function (m, n) {
                        data.getPostData[m] = n;
                    });
                    data.getPostData.pageNo = 1;
                }
                var url = data.url;
                data.getPostData.pageSize = options.pageSize;	//发送每页显示数目
                //console.log(url);console.log(data.getPostData);
                et.animate({opacity: 0}, 200, function () {
                    if (!options.isJSONP) {
                        $.ajax({
                            url: url,
                            data: data.getPostData,
                            dataType: "json",
                            cache: false,
                            type: options.method,
                            beforeSend: function(request) {
                                request.setRequestHeader("key", "12345678");
                            },
                            error: function () {
                                console.log('AJAX数据获取失败。');
                            },
                            success: function (result) {
                                data.result = result;
                                gridUpdate();
                            }
                        });
                    } else {
                        $.getJSON(url, data.getPostData, function (result) {
                            data.result = result;
                            gridUpdate();
                        });
                    }
                });
            };

            //表格更新
            var gridUpdate = function () {
                data.loading();
                if (!data.dataSuccess()) {
                    return;
                }
                et.html(creatHead() + '<tbody></tbody>');
                et.find("tbody").html(creatBody(data.getGridData()));
                et.find("tbody").children().each(function (i, o) {
                    $(o).click(function () {
                        selectedRowId = i + 1;
                    });
                });
                page();
                et.animate({opacity: 1}, 200);
                //et.find("tr").each(function(){$(this).children().eq(0).addClass("th");	}); /////////前台新加
                data.loaded();
                options.cb();
            };

            //分页功能
            var page = function (n) {
                if (!options.page) {
                    return;
                }
                n = n || 2;
                var pager = '',
                    start = function () {
                        if (data.getTotalPages() <= 2 * n + 1) {
                            return 1;
                        }
                        if (data.pageNo + n > data.getTotalPages()) {
                            return data.getTotalPages() - 2 * n;
                        }
                        else {
                            return data.pageNo - n < 1 ? 1 : data.pageNo - n;
                        }
                    };
                for (var i = 0, j = start(); i < (2 * n + 1 > data.getTotalPages() ? data.getTotalPages() : 2 * n + 1); i++) {
                    if (j == data.pageNo) {
                        pager += '<a class="page pageNo" href="javascript:void(0)">' + j + '</a>';
                    }
                    else {
                        pager += '<a class="page" page="' + j + '" href="javascript:void(0)">' + j + '</a>';
                    }
                    j++;
                }
                //不显示首页和尾页
                //$("#"+options.page).html(pager);
                //显示首页和尾页
                $("#" + options.page).html('<a class="page_total page_w" href="javascript:void(0)">Total ' + data.getTotalPages() + ' page</a><a class="page page_w" page="1" href="javascript:void(0)">First</a>' + pager + '<a class="page page_w" page="' + data.getTotalPages() + '" href="javascript:void(0)">Last</a><a class="page_total page_w" href="javascript:void(0)">Total <span>' + data.getNums() + '</span> records</a>');
            };

            //初始化
            var girdInit = function () {
                if (!options.noSent || options.noSent === false)
                    getAjaxData();
                if (options.page) {
                    $("#" + options.page).on("click", ".page", function () {
                        if ($(this).hasClass("pageNo") || Number($(this).attr('page')) < 1) {
                            return;
                        }
                        data.pageNo = Number($(this).attr('page'));
                        getAjaxData();
                    });
                    var optionStr;
                    for(var a in options.pageSizeArr){
                        optionStr += "<option value='"+options.pageSizeArr[a]+"'>"+options.pageSizeArr[a]+"</option>";
                    }
                    var select = 'Records per page：<select class="select htable_select">'+optionStr+'</select>';
                    $("div .spage").append(select);
                    //外加，兼容每页显示数据
                    $(".htable_select").change(function () {
                        options.pageSize = $(this).val();
                        data.pageNo = 1;//切换pageSize从新开始查询
                        getAjaxData();
                    }).val((!options.pageSize) ? 20 : options.pageSize);//设置选中当前的pageSize
                }
            };
            girdInit();
        });
        return $(this);
    };
})(jQuery);






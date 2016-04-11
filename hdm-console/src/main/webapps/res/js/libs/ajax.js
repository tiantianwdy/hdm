/**
 * User: Dongyao Wu
 * Date: 13-7-18
 * Time: 上午2:15
 */

define(["require", "jquery"], function (require, $) {
    "use strict";

    var DefaultTipMsg = "正在处理...";

    function defaultBeforeSend(xhr, tipMessage) {
        var $miniload = $("#mini_load");
        if ($miniload.size() === 0) {
            $("body").append('<div id="mini_load"></div>');
        }
        $miniload.html(tipMessage).show();
    }

    function defaultAfterComplete(xhr, textStatus) {
        $("#mini_load").stop().fadeOut(300);
    }


    var defaults = {
        useAjaxSendTip: true,
        dataType: "json",
        tipMessage: DefaultTipMsg,
        beforeSend: function (xhr) {
        },
        afterComplete: function (xhr, textStatus) {
        },
        fail: function (jqXHR, textStatus, errorThrown) {
//            alert(textStatus);

            if(jqXHR.status - 0 == 600 ){
                alert("会话失效，请先登陆。");
                return ;
            }
        },
        onErrors: function (data) {
            try{
                var err = data.errors;
                if($.isArray(err)){
                    alert(data.errors[0].defaultMessage);
                }else{
                    alert(err);
                }
            }catch(e) {}
        },
        done: function (data) {
            alert("操作成功");
        },
        serializeFunction: function ($context) {
            return $context.serialize();
        },
        requestMethod: function ($form) {
            return $form.attr("method") || "post";
        },
        actionMethod: function ($form) {
            return $form.attr("action");
        }
    };

    function submitForm($form, opt) {
        var form = $form;
        var settings = $.extend({}, defaults, opt);
        var _method = settings.requestMethod($form);
        var _action = settings.actionMethod($form);
        var _data = settings.serializeFunction($form);
        return $.ajax({
            url: _action,
            dataType: settings.dataType,
            type: _method,
            data: _data,
            beforeSend: function (xhr) {
                if (settings.useAjaxSendTip) {
                    try {
                        defaultBeforeSend(xhr, settings.tipMessage);
                    } catch (e) {
                    }
                }
                settings.beforeSend(xhr);
            }
        }).done(function (data) {
            if(data['please-login']){
                alert("会话已经失效，请先登录。");
                return;
            }

            if (data.errors) {
                settings.onErrors(data, form);
            } else {
                settings.done(data, form);
            }
        }).fail(settings.fail).always(function (xhr, textstatus, errorThrown) {
            if (settings.useAjaxSendTip) {
                try {
                    defaultAfterComplete(xhr, textstatus);
                } catch (e) {
                }
            }

            settings.afterComplete(xhr, textstatus);
        });

    }

    var hdialogOnError = {
        onErrors: function (data, $form) {
            var errors = data.errors;
            var len = errors.length;
            if (len == 1 && !errors[0].field) {//如果是整个程序的错误，直接alert。
                alert(errors[0].defaultMessage);
                return;
            }
            $.each(errors, function (i, err) {
                var $input = resolveInput(err, $form);
                $input.change(function () {
                    $(this).siblings(".error").remove();
                });
                console.log($input);
                $input.siblings(".error").remove();
                $input.after('<div class="error">' + err.defaultMessage + '</div>')
            });

        },
        done: function (data) {
            alert("操作成功");
            location.reload();
        }
    };
    var resolveInput = function (err, $context) {
        var inputSelector = ".err-%s".replace("%s", err.field);
        var $find = $context.find(inputSelector);
        if ($find.size() > 0)
            return $find;
        var defaultSelector = ":input[name='%s']".replace("%s", err.field);
        return $context.find(defaultSelector);
    };

    return {
        submit: submitForm,
        hdialogOnError: hdialogOnError
    };

});
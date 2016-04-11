
define(["jquery", "../libs/tojson"], function($){
    "use strict";

    function _redirectByAttribute(selector, attr){
        var _attr = attr || "redirect";
        var target = $(selector).attr(_attr);
        if(target){
            window.location.href = target;
        }
    }

    function _resolveValidationMessage(err, errorContext){
//        var args = err.codes.slice(1);
        if(! err || !err.codes){
            return "格式不正确"
        }

        if( ! errorContext){
            return err.defaultMessage;
        }

        for(var i = 0; i < err.codes.length; i ++){
            var code = err.codes[i];
            var msg = errorContext[code];
            if(msg){
                return msg;
            }
        }

        return err.defaultMessage;
    }

    function _resolveInput(err, $context){
        if(!err){
            return $('.err-unknown-error');
        }

        var field = err.field || err.objectName || "global";

        var inputSelector  = ".err-%s".replace("%s", field);
        var $find = $context.find(inputSelector);

        if($find.size() > 0)
            return $find;

        var defaultSelector = ":input[name='%s']".replace("%s", field);
        return $context.find(defaultSelector);
    }

    function _showErrors(showFunc, errors, seperator){
        seperator = seperator || "\n";
        if($.isArray(errors)){
            if(errors.length == 0){ showFunc(defaultMessage);  return;}
            var msg = $.map(errors,function(e, i){
                return e.defaultMessage || null ;
            }).join(seperator);
           showFunc(msg);
        } else {
            showFunc(errors);
        }
    }

    function _extraPropFormSerialization($form){
        var basic = $form.find(":input:not(.extra-prop)").serialize();
        var $extraPropKV = $form.find('.extra-prop-line');

        var extraPropMap = {};

        $.each($extraPropKV, function(index, line){
            var $nameInput = $(line).find(":input.prop-name");
            var $valueInput = $(line).find(":input.prop-value");
            if(! $nameInput.val()){
                return true; // continue;
            }

            extraPropMap[$.trim($nameInput.val())] = $valueInput.val();
        });
        var propJson = {"properties" :  $.toJSON(extraPropMap) };
//                console.log("before param: ", extraPropMap, " -- ", propJson);
        var message = basic + "&" + $.param(propJson);
//                console.log(message);
        return message;
    }

    function _adjustFormListFieldIndex(prefix,itemPropAttrName, $lines ){
        $.each($lines, function(index,line){
            $(line).find(":input["+itemPropAttrName+"]").each(function(inputIndex, lineInput){
                var item = $(lineInput);
                var prop = item.attr(itemPropAttrName);
                var name = prefix + "[" + index+ "]"+ (prop ? '.' + prop : '');
                item.attr("name", name);
                return true;
            });
        });
    }


    function _registerPageEvent(pageButtons,pageInputName){
        var buttons = pageButtons || '.pager a[page]';
        var input = pageInputName || ':input[name="page.page"]';

        $(document).on('click', buttons ,function(){
            var page = $(this).attr('page') - 0;
            var $input = $(input);
            $input.val(page);
            $input.closest('form').submit();
        });

        $(document).on('click', '.submit-search', function(e){
            $(input).val(1);
            $(input).closest('form').submit();
        });

    }

    var defaultMessage = '服务器出现错误，请联系管理员或稍后再试。';
    var _Helpers = {
        redirectByAttr : _redirectByAttribute,
        resolveValidationMessage: _resolveValidationMessage,
        resolveInput: _resolveInput,
        showErrors: _showErrors,
        extraPropSerializer: _extraPropFormSerialization,
        adjustIndexedParameter: _adjustFormListFieldIndex,
        registerPageEvent : _registerPageEvent
    };

    return _Helpers;
});
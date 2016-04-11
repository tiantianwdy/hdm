
(function($){
    $.fn.hdialog = function(options){
        var defaults = {
        };
        var options = $.extend(defaults, options);
        this.each(function(){
            //初始化
            if(!$("#dialog_bg").length){$("body").prepend('<div id="dialog_bg"></div>');}
            var self=$(this),
                title=self.attr("title") || '',
                width=self.attr("width") || '560',
                height=self.attr("height") || 'auto',
                anim=self.attr("anim") || '1',
                hdialog_html='<div class="hhead lay"><div class="fl"></div><div class="fr"></div><h2>'+title+'</h2></div><div class="dialog_close hclose" title="关闭"></div><div class="hbody">'+self.html()+'</div>',
                isIE6=navigator.appVersion.indexOf("MSIE 6.0")!=-1?true:false,
                ie6Bug=function(){
                    if(isIE6){
                        var tt=$(window).scrollTop();
                        self.css({top:tt+$(window).height()/2});
                        $("#dialog_bg").css({top:tt});
                    }
                };
            self.attr("title","").html(hdialog_html).css({width:width,marginLeft:-width/2,marginTop:-height/2});
            if(height == 'auto'){ self.css({marginTop:-self.height()/2});}
            else{ self.children(".hbody").css({height:height-29});}
            self.find(".hclose").click(function(){hd.close();});

            //动画效果
            jQuery.extend( jQuery.easing,{
                eOutBack: function (x, t, b, c, d, s) {
                    if (s == undefined) s = 1.70158;
                    if ((t/=d/2) < 1) return c/2*(t*t*(((s*=(1.525))+1)*t - s)) + b;
                    return c/2*((t-=2)*t*(((s*=(1.525))+1)*t + s) + 2) + b;
                }
            });

            //操作弹层
            var hd=this.hd={
                open:function(){
                    switch(anim){
                        case '0':
                            self.show();
                            break;
                        case '1':
                            self.show(100);
                            break;
                        case '2':
                            self.css({top:-1000,display:'block'}).animate({top:'50%'},400,"eOutBack");
                            break;
                        default:
                            self.show();
                    }
                    $("#dialog_bg").show().animate({opacity:0.1},500);
                    ie6Bug();
                },
                close:function(){
                    switch(anim){
                        case '0':
                            self.hide();
                            break;
                        case '1':
                            self.hide(100);
                            break;
                        case '2':
                            self.animate({top:-1000},500,"eOutBack");
                            break;
                        default:
                            self.hide();
                    }
                    $("#dialog_bg").css({opacity:0}).hide();
                }
            };


            //兼容IE6
            if(isIE6){
                $("#dialog_bg").css({height:$(window).height()});
                $(window).scroll(function(){
                    if(self.css("display")=="none"){return;}
                    ie6Bug();
                });
            }


        });
    };
})(jQuery);






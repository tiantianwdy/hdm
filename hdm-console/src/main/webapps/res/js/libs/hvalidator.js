/*
使用：$.hvalidator({
		inputs:[{id:xx,type:xx,text:'您输入的XX格式不对',cb:function(result){}}],     //text和cb可以省略
		supply:{clas:xx,type:xx,text:'必填项'}//要检查的元素的类名，rules类型----supply为可选
	});
*/
(function($){
	$.hvalidator = $.hvalidator||function(options){

		options.inputs=options.inputs || [];

		//正则表达式
		var rules={
				required	: "^.+$",
				password	: "^.+$",
				loginname	: "^[a-z][a-z0-9-_.]+$",
				email		: "^[a-z0-9][a-z0-9.+]+[@][a-z0-9-]+[.][a-z]",
				cellphone	: "^1[3-8][0-9][0-9]{8}$",
				gphone		: "^[+]{0,1}(\d){1,4}[ ]{0,1}([-]{0,1}((\d)|[ ]){1,12})+$",	//座机号码
				english		: "^[A-Za-z]+$",
				user		: "^\w+$",	//数字英文下划线
				nums		: "^[0-9]*$",
				url			: "^http:\/\/.+$",
				sfz			: "^\d{15}|\d{}18$ "	//身份证

		},
			//检查单个对象
			check_opt=function(opt){
				if(opt.type!='required' && !$.trim(opt.id.val())){return true}
				var re=new RegExp(rules[opt.type],'g').test($.trim(opt.id.val()));
				op(opt,re);
				return re;
		},
			//结果处理
			op=function(opt,result){
				if(typeof opt.cb==="function"){opt.cb(result);}
				else{
					opt.text=opt.text||'输入格式错误。';
					if(result){
						if(opt.id.next(".hvalidator").length>0){opt.id.next(".hvalidator").remove();}
					}else{
						if(opt.id.next(".hvalidator").length===0){opt.id.after("<span class='hvalidator'>"+opt.text+"</span>");}
					}
				}

		},
			//初始化
			init=function(){
				if(options.supply){
					$("."+options.supply.clas).each(function(i,j){
						options.inputs.push({id:$(j),type:options.supply.type,text:options.supply.text});
					});
				}
				$.each(options.inputs,function(i,j){
					j.id=typeof(j.id)==='string'?$("#"+j.id):j.id;
					//result=check_opt(j) && result;
					j.id.focusout(function(){
						check_opt(j);
					});
				});

				//阻止表单
				if(options.inputs[0].id.parents("form").length!=0){
					options.inputs[0].id.parents("form").submit(function(){return check();});
				}
		};
		init();

		//全部检查
		var check=$.hvalidator.check=function(){
			    var result=true;
				$.each(options.inputs,function(i,j){
					result=check_opt(j) && result;
				});
				return result;
		};



	};
})(jQuery);






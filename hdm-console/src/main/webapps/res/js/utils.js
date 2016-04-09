
var utils = {

   collapse: function (cls, openLabel, closeLabel, callback) {
       $("#" + cls).click(function () {
           $header = $(this);
           //getting the next element
           $content = $header.next();
           //open up the content needed - toggle the slide- if visible, slide up, if not slidedown.
           $content.slideToggle(500, function () {
               //execute this after slideToggle is done
               //change text of header based on visibility of content div
               $header.text(function () {
                  //change text based on condition
                  return $content.is(":visible") ? closeLabel : openLabel;
               });
           });
           if(callback) callback();
       });
   },

   collapseNative: function (cls, openLabel, closeLabel, callback) {
       $("#" + cls).click(function () {
           $header = $(this);
           //getting the next element
           $content = $header.next();
           if(!$content.is(':hidden')){
             $content.hide(500)
             $header.text(openLabel)
           } else {
             $content.show(500)
             $header.text(closeLabel)
           };
           if(callback) callback();
       });
   },

   show: function(cls, callback){
     $("#" + cls).show(500);
     if(callback) callback();
   },

   hide: function(cls, speed, callback){
     $("#" + cls).hide(speed);
     if(callback) callback();
   }

};

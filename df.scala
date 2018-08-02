/*cc.select("user_id","ev_type")
val from=DateTime.parse("2018-03-24")
val to=DateTime.parse("2018-03-25")
val ev=getEvent("com.picsart.studio","editor_open",from,to)
val newvar=ev.select($"user_id", $"editor_sid",$"platform" $"country_code",$"device_id", to_date($"timestamp").as("date"))


val x=newvar.groupBy("country_code","date").agg(countDistinct("user_id").as("us_id"),countDistinct("device_id").as("dv_id"))


 x.groupBy("date").mean("us_id")

x.select(corr("us_id","dv_id"))
x.groupBy("date").mean("us_id","dv_id")
x.select(max("us_id")).show(20,false)
ev.select($"user_id").show(5,false)
  

  harc_1

val mi=ev.select("user_id","device_id")
mi.na

mi.na.fill(5555555,Array("user_id"))


newvar.filter("user_id".isNull).select("device_id")














harc_1_1
val from=DateTime.parse("2018-03-24")
val to=DateTime.parse("2018-03-25")
val open = getEvent("com.picsart.studio","editor_open",from,to).
    select($"user_id", $"platform", lower($"country_code").as("country_code"),$"device_id", to_date($"timestamp").as("d")).
    filter(length($"country_code")===2).
    groupBy($"platform", $"country_code", $"d").agg(countDistinct("device_id").as("d1")).
    filter($"d1" >= 50)

val done = getEvent("com.picsart.studio","editor_done_click",from,to).
    select($"user_id", $"platform", lower($"country_code").as("country_code"),$"device_id", to_date($"timestamp").as("d")).
    filter(length($"country_code")===2).
    groupBy($"platform", $"country_code", $"d").agg(countDistinct("device_id").as("d2"))

//1. JOIN open with DONE
//2. 

val joined_df=open.join(done,
    open("platform")===done("platform") && 
    open("country_code")===done("country_code") && open("d") === done("d"), "inner").
    select(
        open("platform"),
        open("country_code"),
        open("d"),
        open("d1"),
        done("d2"),
        (done("d2")/open("d1")).as("ratio")
    )

joined_df.
filter($"country_code" === "ar" && $"platform" === "android").
orderBy($"ratio".desc).show(100, false)
        




harc_2
val y=ev.select($"user_id",$"source",$"device_id", $"platform")
val p = y.
    groupBy($"source").pivot("platform").agg(
        countDistinct("user_id").as("users"),
        (count("user_id")/countDistinct("user_id")).as("ratio")
    ).
    //withColumn("ratio", $"count"/$"users").

    

    show(100, false)




    new_Task

val x = "camera_sid"
val from=DateTime.parse("2018-03-29")
val to=DateTime.parse("2018-03-29")

val cam_open=getEvent("com.picsart.studio","camera_open",from,from).
select($"platform",col(x).as("x")).
groupBy($"platform").agg(countDistinct($"x").as("count_open_sid"))

cam_open.show(false)

val cam_cap=getEvent("com.picsart.studio","camera_capture",from,from).
select($"platform",$"x").
groupBy($"platform").agg(countDistinct($"x").as("count_cap_sid"))

cam_cap.show(false)*/

/*
val joined_cam = cam_open.as("a").
    join(cam_cap.as("b"), $"a.platform" === $"b.platform", "inner").
    select(
        $"a.platform",
        $"a.count_open_sid",
        $"b.count_cap_sid",
        ($"b.count_cap_sid" / $"a.count_open_sid").as("ratio")
    )
val joined_cam=cam_open.join(cam_cap, cam_open("platform")===cam_cap("platform"), "inner").
   select(
            cam_open("platform"),
            cam_open("count_open_sid"),
            cam_cap("count_cap_sid"),

             (cam_cap("count_cap_sid")/cam_open("count_open_sid")).
            as("ratio")

         )
*/

/* val from = DateTime.parse("2018-03-20")
val to = DateTime.parse("2018-03-25")
val x = "session_id"
val cam_open = getEvent("com.picsart.studio","camera_open",from,to).
    select($"platform", col(x).as("x"),to_date($"timestamp").as("date_open")).
    distinct()
val cam_cap = getEvent("com.picsart.studio","camera_capture",from,to).
    select($"platform", col(x).as("x"),to_date($"timestamp").as("date_cap")).
    distinct()
val joined_cam = cam_open.as("a").
    join(cam_cap.as("b"), $"a.platform" === $"b.platform" && $"a.x"=== $"b.x" && $"a.date_open"=== $"b.date_cap","left").
    select($"a.platform", $"a.x".as("open_sid"), $"a.date_open",$"b.x".as("capture_sid")).
    groupBy($"platform",$"date_open").
    agg(
        countDistinct($"open_sid").as("open - " + x),
        countDistinct($"capture_sid").as("capture - " + x)).
    withColumn("ratio - " + x, col("capture - " + x) * 1.0/ col("open - " + x)).
    show(false)*/




/*val from = DateTime.parse("2018-03-01")
val to = DateTime.parse("2018-03-25")
val x = "device_id"
val cam_open = getEvent("com.picsart.studio","camera_open",from,to).
    select($"platform", col(x).as("x"),to_date($"timestamp").as("date_open")).
    distinct()
val cam_cap = getEvent("com.picsart.studio","camera_capture",from,to).
    select($"platform", col(x).as("x"),to_date($"timestamp").as("date_cap")).
    distinct()
val cam_done= getEvent("com.picsart.studio","camera_preview_close",from,to).filter($"action" === "done").
    select($"platform",col(x).as("x"),to_date($"timestamp").as("date_done")).
    distinct()



val joined_cam = cam_open.as("a").
    join(cam_cap.as("b"), 
        $"a.platform" === $"b.platform" && 
        $"a.x"=== $"b.x" && 
        $"a.date_open"=== $"b.date_cap","left").
    select($"a.platform", $"a.x".as("open_sid"), $"a.date_open",$"b.x".as("capture_sid")).as("c")
        .join(cam_done.as("d"),
            $"c.platform"===$"d.platform" && 
            $"c.date_open"=== $"d.date_done" && 
            $"c.capture_sid"=== $"d.x","left").
        select($"c.platform",$"c.date_open",$"c.open_sid",$"c.capture_sid",$"d.x".as("done_sid")).
        groupBy($"platform",$"date_open").
        agg(
            countDistinct($"open_sid").as("open - " + x),
            countDistinct($"capture_sid").as("capture - " + x),
            countDistinct($"done_sid").as("done - " + x)).
        withColumn("ratio capture/open", col("capture - " + x) * 1.0/ col("open - " + x)).
        withColumn("ratio done/capture", col("done - " + x) * 1.0/ col("capture - " + x)).
        show(50,false)*/
      
val from = DateTime.parse("2018-03-27")
val com =  getEvent("com.picsart.studio","common",from,from).
            select($"device_id",$"event_type",$"platform").
            groupBy($"device_id",$"platform").
            agg(countDistinct($"event_type").as("count")).
            filter($"count" <= 3)
println(com.count())
val com_event= getEvent("com.picsart.studio","common",from,from).
                select($"device_id",$"event_type",$"platform")


val joined_com = com_event.as("a").
                join(com.as("b"),
                    $"a.device_id"=== $"b.device_id" &&
                    $"a.platform"=== $"b.platform" , "inner").
                select($"a.device_id",$"a.event_type",$"a.platform").
                groupBy($"event_type").pivot("platform").agg(count($"event_type").as("count_event")).show(1000,false)





/*

def getMobileDevices(from:DateTime, to:DateTime): DataFrame = {
    val days = Days.daysBetween(from.withTimeAtStartOfDay(), to.withTimeAtStartOfDay()).getDays
    val files = (0 to days).map { d=>
        val dateStr = from.plusDays(d).toString(pathFormatter)
        s"/analytics/events/PARQUET/mobile_devices/$dateStr/"
    }
    sqlContext.read.parquet(files:_*)
}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import java.sql.Date
import scala.util.control.Breaks._

val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
val d1 = formatter.parseDateTime("2018-03-11")
val d2 = d1.minusDays(7)

//MOBILE DEVICES-------------------------------------------------------------------------------------------------------------------------------------------------------
var m1 = getMobileDevices(d1, d1).filter($"platform" === "android").filter(lower($"country_code").in("us", "in", "cn", "br")).filter($"app" === "com.picsart.studio").select(lower($"country_code").as("country"), $"device_id").distinct()
var m2 = getMobileDevices(d2, d2).filter($"platform" === "android").filter(lower($"country_code").in("us", "in", "cn", "br")).filter($"app" === "com.picsart.studio").select(lower($"country_code").as("country"), $"device_id").distinct()
//m1.groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)
//m2.groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)

//COMMON---------------------------------------------------------------------------------------------------------------------------------------------------------------
val c1 = getCommon("com.picsart.studio", d1, d1)
    .filter($"platform" === "android")
    .filter(lower($"country_code").in("us", "in", "cn", "br"))
    .filter($"app" === "com.picsart.studio")
    .select($"device_id", lower($"country_code").as("country"), $"event_type").distinct()
val c2 = getCommon("com.picsart.studio", d2, d2)
    .filter($"platform" === "android")
    .filter(lower($"country_code").in("us", "in", "cn", "br"))
    .filter($"app" === "com.picsart.studio")
    .select($"device_id", lower($"country_code").as("country"), $"event_type").distinct()
//c1.groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)
//c2.groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)

//COMMON WITHOUT SPECIFIC EVENT----------------------------------------------------------------------------------------------------------------------------------------
c2.filter(!($"event_type".in(
        //BACKGROUND
        "app_update", "push_received", "push_clicked", "onboarding_request",
        //SHOP
        "shop_request", "shop_response",
        //ADS
        "banner_ad_collapsed","banner_ad_expanded", "banner_ad_request", "banner_ad_response", "editor_ad_request", "editor_ad_response", "editor_complete_ad_request",
        "editor_complete_ad_response", "interstitial_ad_request", "interstitial_ad_response", "native_ad_request", "native_ad_response", "photo_choose_ad_request",
        "photo_choose_ad_response","rewarded_video_ad_response","rewarded_video_ad_request","three_swipes_ad_request","three_swipes_ad_response","subscription_status",
        //iMESSAGE
        "imessage_app_open", "imessage_blank_state_login", "imessage_camera_capture", "imessage_camera_opened", "imessage_cutout_selection", "imessage_gallery_open",
        "imessage_gallery_photo_selected","imessage_maximize_page","imessage_minimize_page","imessage_rotate","imessage_save_back_button_click","imessage_search_click"
        ,"imessage_search_close", "imessage_sticker_done", "imessage_sticker_save", "imessage_sticker_use",
        //STICKER KEYBOARD
        "key_app_open", "key_scroll", "key_search_tap", "key_sticker_category_open", "key_sticker_sent", "key_sticker_tap", "key_suggestion_view", "key_window_change"
    ))).groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)

//USERS THAT DID ONLY SPECIFIC EVENTS-------------------------------------------------------------------------------------------------------------------------------------------------
val specific_users_d1 = c1
    .withColumn("event_category", when($"event_type".in(
        //BACKGROUND
        "app_update", "push_received", "push_clicked", "onboarding_request",
        //SHOP
        "shop_request", "shop_response",
        //ADS
        "banner_ad_collapsed","banner_ad_expanded", "banner_ad_request", "banner_ad_response", "editor_ad_request", "editor_ad_response", "editor_complete_ad_request",
        "editor_complete_ad_response", "interstitial_ad_request", "interstitial_ad_response", "native_ad_request", "native_ad_response", "photo_choose_ad_request",
        "photo_choose_ad_response","rewarded_video_ad_response","rewarded_video_ad_request","three_swipes_ad_request","three_swipes_ad_response","subscription_status",
        //iMESSAGE
        "imessage_app_open", "imessage_blank_state_login", "imessage_camera_capture", "imessage_camera_opened", "imessage_cutout_selection", "imessage_gallery_open",
        "imessage_gallery_photo_selected","imessage_maximize_page","imessage_minimize_page","imessage_rotate","imessage_save_back_button_click","imessage_search_click"
        ,"imessage_search_close", "imessage_sticker_done", "imessage_sticker_save", "imessage_sticker_use",
        //STICKER KEYBOARD
        "key_app_open", "key_scroll", "key_search_tap", "key_sticker_category_open", "key_sticker_sent", "key_sticker_tap", "key_suggestion_view", "key_window_change"
        ), lit(1)).otherwise(lit(10))
    )
    .select($"country", $"device_id", $"event_category").distinct()
    .groupBy($"country", $"device_id").agg(sum($"event_category").as("events"))
    .filter($"events" === 1)
    .select($"country", $"device_id").distinct()
val specific_users_d2 = c2
    .withColumn("event_category", when($"event_type".in(
        //BACKGROUND
        "app_update", "push_received", "push_clicked", "onboarding_request",
        //SHOP
        "shop_request", "shop_response",
        //ADS
        "banner_ad_collapsed","banner_ad_expanded", "banner_ad_request", "banner_ad_response", "editor_ad_request", "editor_ad_response", "editor_complete_ad_request",
        "editor_complete_ad_response", "interstitial_ad_request", "interstitial_ad_response", "native_ad_request", "native_ad_response", "photo_choose_ad_request",
        "photo_choose_ad_response","rewarded_video_ad_response","rewarded_video_ad_request","three_swipes_ad_request","three_swipes_ad_response","subscription_status",
        //iMESSAGE
        "imessage_app_open", "imessage_blank_state_login", "imessage_camera_capture", "imessage_camera_opened", "imessage_cutout_selection", "imessage_gallery_open",
        "imessage_gallery_photo_selected","imessage_maximize_page","imessage_minimize_page","imessage_rotate","imessage_save_back_button_click","imessage_search_click"
        ,"imessage_search_close", "imessage_sticker_done", "imessage_sticker_save", "imessage_sticker_use",
        //STICKER KEYBOARD
        "key_app_open", "key_scroll", "key_search_tap", "key_sticker_category_open", "key_sticker_sent", "key_sticker_tap", "key_suggestion_view", "key_window_change"
        ), lit(1)).otherwise(lit(10))
    )
    .select($"country", $"device_id", $"event_category").distinct()
    .groupBy($"country", $"device_id").agg(sum($"event_category").as("events"))
    .filter($"events" === 1)
    .select($"country", $"device_id").distinct()
    
//specific_users_d1.groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)
//specific_users_d2.groupBy($"country").agg(countDistinct($"device_id")).orderBy($"country").show(false)

//EXCLUDED SPECIFIC EVENT USERS----------------------------------------------------------------------------------------------------------------------------------------
c1.as("a")
    .join(specific_users_d1.as("b"), $"a.country" === $"b.country" && $"a.device_id" === $"b.device_id", "left")
    .filter($"b.device_id".isNull).drop($"b.device_id").drop($"b.country")
    .groupBy($"event_type").pivot("country").agg(countDistinct($"device_id")).show(1000, false)
c2.as("a")
    .join(specific_users_d2.as("b"), $"a.country" === $"b.country" && $"a.device_id" === $"b.device_id", "left")
    .filter($"b.device_id".isNull).drop($"b.device_id").drop($"b.country")
    .groupBy($"event_type").pivot("country").agg(countDistinct($"device_id")).show(1000, false)

//EXCLUDED SPECIFIC EVENT USERS----------------------------------------------------------------------------------------------------------------------------------------
c1.as("a")
    .join(specific_users_d1.as("b"), $"a.country" === $"b.country" && $"a.device_id" === $"b.device_id", "inner")
    .drop($"b.device_id").drop($"b.country")
    .groupBy($"event_type").pivot("country").agg(countDistinct($"device_id")).show(false)
c2.as("a")
    .join(specific_users_d2.as("b"), $"a.country" === $"b.country" && $"a.device_id" === $"b.device_id", "inner")
    .drop($"b.device_id").drop($"b.country")
    .groupBy($"event_type").pivot("country").agg(countDistinct($"device_id")).show(false)
*/



val to = DateTime.parse("2018-04-02")
/*val from = DateTime.parse("2018-03-24")*/
/*val exp = getEvent("com.picsart.studio","experiment_participate",from,to).
    filter($"platform"==="android" && $"experiment_id"==="e111").
    select($"device_id", to_date($"timestamp").as("date"), $"variant").
    groupBy($"date").pivot("variant").agg(countDistinct($"device_id")).show(50,false)*/
val exp = getEvent("com.picsart.studio","experiment_participate",to,to).
    filter($"platform"==="android" && $"experiment_id"==="e111").
    select($"device_id", $"variant").distinct()


val toX = DateTime.parse("2018-03-13")
val fromX = DateTime.parse("2018-02-13")
val done  = getEvent("com.picsart.studio","editor_done_click",fromX,toX).
    filter($"platform"==="android").
    select($"device_id",to_date($"timestamp").as("date")).
    groupBy($"device_id").agg(countDistinct($"date").as("active_days")).
    withColumn("cohort",
        when($"active_days" >= 13, lit("active")).
        when($"active_days" <=3 && $"active_days" >=1, lit("passive")).
        otherwise(lit("-"))
    ).
    drop($"active_days").
    filter($"cohort" === "active" || $"cohort" === "passive")

var users = exp.as("a").
    join(done.as("b"), $"a.device_id" === $"b.device_id", "inner").
    drop($"b.device_id")
//users.groupBy($"variant").pivot("cohort").agg(countDistinct($"device_id")).show(false)




//------------------------------------------------------------------------------------------------------------------------------------
/*val event = getEvent("com.picsart.studio","edit_item_open", to, to).
    filter($"item"==="quick_brush")
    
event.select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"), $"a.device_id" === $"b.device_id", "inner").
    select($"b.device_id", $"b.variant", $"b.cohort", $"a.date").
    groupBy($"variant", $"cohort", $"date").
    agg(countDistinct($"device_id").as("devices"),(count($"device_id")/countDistinct($"device_id")).as("ratio")).
    groupBy($"variant", $"cohort").agg(avg($"devices"),avg($"ratio"))*/
 
//------------------------------------------------------------------------------------------------------------------------------------local





val to = DateTime.parse("2018-04-03")
val from = DateTime.parse("2018-03-24")

val open = getEvent("com.picsart.studio","edit_item_open", from, to).
    filter($"item"==="quick_brush").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"), $"a.device_id" === $"b.device_id", "inner").
    select($"b.device_id", $"b.variant", $"b.cohort", $"a.date").
    groupBy($"device_id", $"variant", $"cohort", $"date").count()

val apply = getEvent("com.picsart.studio","quick_brush_page_close", from, to).
    filter($"exit_action"==="apply").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.device_id",$"b.variant",$"b.cohort",$"a.date").
    groupBy($"a.device_id",$"b.variant",$"b.cohort",$"a.date").count()





val joined_open_apply = open.as("a").
join(apply.as("b"), $"a.device_id" === $"b.device_id" && $"a.date" === $"b.date", "left").
select(
    $"a.variant", $"a.cohort", $"a.date",
    $"a.device_id".as("open_device_id"), $"a.count".as("opens"),
    $"b.device_id".as("apply_device_id"), $"b.count".as("applies_1")).
withColumn("applies", when($"applies_1".isNull, lit("0")).otherwise($"applies_1")).drop($"applies_1").
groupBy($"variant", $"cohort", $"date").agg(
    countDistinct($"open_device_id").as("count_opens"),
    countDistinct($"apply_device_id").as("count_applies"),
    sum($"opens").as("opens"),
    sum($"applies").as("applies")
).

groupBy($"variant",$"cohort").agg(
    avg($"count_opens"),
    avg($"count_applies"),
    avg($"opens"),
    avg($"applies"),
    (avg($"opens")/avg($"count_opens")).as("ratio-1"),
    (avg($"applies")/avg($"count_applies")).as("ratio_2"),
    (avg($"count_applies")/avg($"count_opens")).as("apply_DAU/open_DAU"))

//---------------------------------------------------------------------------------------------------------------------------------------global

val open = getEvent("com.picsart.studio","edit_item_open",from,to).
filter($"item"==="quick_brush").
select($"device_id", to_date($"timestamp").as("date"))







//------------------------------------------------------------------------------------------------------------------------------------------------

val open = getEvent("com.picsart.studio","edit_item_open", to, to).
    filter($"item"==="quick_brush").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"), $"a.device_id" === $"b.device_id", "inner").
    select($"b.device_id", $"b.variant", $"b.cohort").
    groupBy($"device_id", $"variant", $"cohort").count()

val apply = getEvent("com.picsart.studio","quick_brush_page_close", to, to).
    filter($"exit_action"==="apply").
    select($"device_id",to_date($"timestamp").as("date")).as("a").
    join(users.as("b"),$"a.device_id"=== $"b.device_id","inner").
    select($"a.device_id",$"b.variant",$"b.cohort").
    groupBy($"a.device_id",$"b.variant",$"b.cohort").count()

val joined_open_apply = open.as("a").
join(apply.as("b"), $"a.device_id" === $"b.device_id", "left").
select($"a.device_id", $"a.variant", $"a.cohort", $"a.count".as("opens"), $"b.count".as("applies_1")).
withColumn("applies", when($"applies_1".isNull, lit("0")).otherwise($"applies_1")).
 withColumn("opens_grouped",
        when($"opens" <= 5, $"opens").
        when($"opens" <= 10 && $"opens" >= 6, lit("6-10")).
        when($"opens" <= 20 && $"opens" >= 11, lit("11-20")).
        when($"opens" <= 50 && $"opens" >= 21, lit("21-50")).
        when($"opens" >= 51, lit("51+"))
    ).
 withColumn("applies_grouped",
        when($"applies" <= 5, $"applies").
        when($"applies" <= 10 && $"applies" >= 6, lit("6-10")).
        when($"applies" <= 20 && $"applies" >= 11, lit("11-20")).
        when($"applies" <= 50 && $"applies" >= 21, lit("21-50")).
        when($"applies" >= 51, lit("51+"))
    )

 joined_open_apply.groupBy($"variant",$"cohort").pivot("opens_grouped").agg(countDistinct($"device_id").as("open_devices")).show(false)
 
 joined_open_apply.groupBy($"variant",$"cohort").pivot("applies_grouped").agg(countDistinct($"device_id").as("apply_devices"))
    
    







joined_open_apply.
    groupBy($"opens", $"cohort", $"variant").agg(countDistinct($"device_id").as("open_devices")).
    withColumn("opens_grouped",
        when($"opens" <= 5, $"opens").
        when($"opens" <= 10 && $"opens" >= 6, lit("6-10")).
        when($"opens" <= 20 && $"opens" >= 11, lit("11-20")).
        when($"opens" <= 50 && $"opens" >= 21, lit("21-50")).
        when($"opens" >= 51, lit("51+"))
    ).
    groupBy($"cohort", $"variant").pivot("opens_grouped").agg(sum($"open_devices")).
    show(false)


joined_open_apply.

    groupBy($"applies",$"cohort",$"variant").agg(countDistinct($"device_id").as("apply_devices")).
    withColumn("applies_grouped",
        when($"applies" <= 5, $"applies").
        when($"applies" <= 10 && $"applies">=6, lit("6-10")).
        when($"applies" <= 20 && $"applies">=11, lit("11-20")).
        when($"applies" <= 50 && $"applies">=21, lit("21-50")).
        when($"applies" >=51, lit("51+"))
    ).
    groupBy($"cohort",$"variant").pivot("applies_grouped").agg(sum($"apply_devices")).
    show(false)





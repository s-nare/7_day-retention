import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
val formatter = DateTimeFormat.forPattern("ddMMyyyy")

//// defining super users

val from  = DateTime.parse("2018-05-25")
val to  = DateTime.parse("2018-06-25")




// 

 val mau = getEvent("com.picsart.studio", "app_open", from, to).
 select($"user_id").distinct()




val users = getUsers.
filter($"resolvedLocation.country" === "United States").
select(
	$"userId", to_date($"birthDate").as("birthDate"),
	$"gender".as("gen_1"),
	$"connections.gender".as("gen_2"),
	$"connections.minAge", 
	$"connections.maxAge"
).
withColumn("gen_first",
	when($"gen_1"=== 0, null).
	otherwise($"gen_1")
).
withColumn("gen_second",
	when($"gen_2"=== 0, null).
	otherwise($"gen_2")
).
withColumn("minAge_1",
	when($"minAge"=== 0, null).
	otherwise($"minAge")
).
withColumn("maxAge_1",
	when($"maxAge"=== 0, null).
	otherwise($"maxAge")
).
drop($"gen_1").
drop($"gen_2").
drop($"minAge").
drop($"maxAge").
filter($"birthDate" >= "1900-01-01" || $"birthDate".isNull).
withColumn("birthDate_1", 
	when($"birthDate".isNotNull, lit("00-00-00")).
	otherwise($"birthDate")
).
drop($"birthDate")


val valod = users.as("a").join(mau.as("b"), $"a.userId" === $"b.user_id", "inner")

valod.groupBy($"birthDate_1",$"minAge_1",$"maxAge_1").count()


valod.groupBy($"gen_first", $"gen_second").count()






